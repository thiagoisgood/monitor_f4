"""Fixed, read-only Linux collector. Python 3.6 compatible; sent over SSH stdin."""
import json
import gzip
import os
import platform
import re
import socket
import subprocess
import sys
import time
import urllib.request


def compose_from_containers(rows):
    """Compose projects are Docker container labels; avoid a second heavy CLI."""
    groups = {}
    for row in rows:
        project = row.get('ComposeProject', '')
        if not project:
            continue
        group = groups.setdefault(project, {'states': {}, 'paths': set()})
        state = row.get('State') or 'unknown'
        group['states'][state] = group['states'].get(state, 0) + 1
        for path in row.get('ComposeConfigFiles', '').split(','):
            if path.strip():
                group['paths'].add(path.strip())
    return [{'name': name,
             'status': ', '.join('%s(%d)' % (state, count)
                                 for state, count in sorted(group['states'].items())),
             'path': ','.join(sorted(group['paths']))}
            for name, group in sorted(groups.items())]


MODE, HOST = sys.argv[1:3]
assert MODE in ('metrics', 'services', 'inventory')
assert HOST in ('t1', 't2', 't3', 'ali_cjlass')

def read(path):
    with open(path) as f:
        return f.read()

def command(args, seconds=1.0, stderr_to_stdout=False):
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT if stderr_to_stdout else subprocess.DEVNULL,
                           universal_newlines=True, timeout=seconds)
        return p.returncode, p.stdout[:350000]
    except subprocess.TimeoutExpired:
        return 124, ''
    except (OSError, ValueError):
        return 127, ''


def emit(data, compressed=False):
    # ASCII JSON also works on Python 3.6 under LC_ALL=C.
    payload = json.dumps(data, ensure_ascii=True).encode('ascii')
    if compressed:
        sys.stdout.buffer.write(gzip.compress(payload, compresslevel=6))
    else:
        print(payload.decode('ascii'))

mem = {k: int(v.split()[0]) * 1024 for k, v in
       (x.split(':', 1) for x in read('/proc/meminfo').splitlines())}
fs = os.statvfs('/')

if MODE == 'metrics':
    rx, tx = 0, 0
    for line in read('/proc/net/dev').splitlines()[2:]:
        name, data = line.split(':', 1)
        name = name.strip()
        if name == 'lo' or name.startswith(('veth', 'docker', 'br-', 'virbr')):
            continue
        cells = data.split()
        rx += int(cells[0]); tx += int(cells[8])
    data = {'cpu_ticks': [int(x) for x in read('/proc/stat').splitlines()[0].split()[1:9]],
            'memory_total': mem['MemTotal'], 'memory_available': mem['MemAvailable'],
            'swap_used': mem.get('SwapTotal', 0) - mem.get('SwapFree', 0),
            'disk_total': fs.f_blocks * fs.f_frsize, 'disk_free': fs.f_bavail * fs.f_frsize,
            'disk_percent': 100.0 * (fs.f_blocks - fs.f_bfree) / max(1, fs.f_blocks - fs.f_bfree + fs.f_bavail),
            'load': list(os.getloadavg()), 'uptime': float(read('/proc/uptime').split()[0]),
            'cores': os.cpu_count(), 'rx_total': rx, 'tx_total': tx,
            'boot_id': read('/proc/sys/kernel/random/boot_id').strip()}
    if HOST == 'ali_cjlass':
        # A missing mount must never be mistaken for the root filesystem.
        disk = {'mount': '/test', 'state': 'unavailable'}
        try:
            if os.path.ismount('/test'):
                extra = os.statvfs('/test')
                disk.update(state='live', total=extra.f_blocks * extra.f_frsize,
                            free=extra.f_bavail * extra.f_frsize,
                            percent=100.0 * (extra.f_blocks - extra.f_bfree) / max(1, extra.f_blocks - extra.f_bfree + extra.f_bavail))
        except OSError:
            pass
        data['extra_disk'] = disk
elif MODE == 'services':
    docker = ['sudo', '-n', 'docker'] if HOST == 't2' else ['docker']
    docker_format = '{"Names":{{json .Names}},"Image":{{json .Image}},"State":{{json .State}},"Status":{{json .Status}},"Ports":{{json .Ports}},"ComposeProject":{{json (.Label "com.docker.compose.project")}},"ComposeConfigFiles":{{json (.Label "com.docker.compose.project.config_files")}}}'
    code, text = command(docker + ['ps', '-a', '--format', docker_format], 1.3)
    containers, raw_containers = [], []
    for line in text.splitlines() if code == 0 else []:
        try:
            c = json.loads(line); status = c.get('Status', '')
            raw_containers.append(c)
            state = c.get('State') or ('running' if status.startswith('Up') else
                    'exited' if status.startswith('Exited') else 'unknown')
            health = ('unhealthy' if '(unhealthy)' in status else 'healthy' if '(healthy)' in status else
                      'starting' if 'health: starting' in status else 'not_configured')
            expected = c.get('Names') == 'example-init' and status.startswith('Exited (0)')
            containers.append({'name': c.get('Names', ''), 'image': c.get('Image', ''),
                'state': state, 'status': status, 'health': health, 'ports': c.get('Ports', ''),
                'expected_exit': expected})
        except (ValueError, TypeError):
            code = 65
    compose = compose_from_containers(raw_containers) if code == 0 else []
    # Replace example service names with the deployment's own allowlist.
    units = {host: (['docker.service', 'ssh.service'], []) for host in ('t1', 't2', 't3', 'ali_cjlass')}
    services = []
    os.environ['XDG_RUNTIME_DIR'] = '/run/user/' + str(os.getuid())
    for scope, names in zip(('system', 'user'), units[HOST]):
        if not names:
            continue
        args = ['systemctl'] + (['--user'] if scope == 'user' else [])
        sc, value = command(args + ['show', '--no-pager', '--property=Id,LoadState,ActiveState,SubState'] + names, .5)
        if value:
            for block in value.strip().split('\n\n'):
                props = dict(x.split('=', 1) for x in block.splitlines() if '=' in x)
                services.append({'name': props.get('Id', 'unknown'), 'scope': scope,
                    'state': 'not_found' if props.get('LoadState') == 'not-found' else props.get('ActiveState', 'unknown'),
                    'sub_state': props.get('SubState', 'unknown')})
        else:
            services += [{'name': n, 'scope': scope, 'state': 'unknown', 'sub_state': 'query_failed'} for n in names]
    probes = []
    if HOST in ('t1', 't2'):
        start = time.monotonic(); status = 0
        try:
            with urllib.request.urlopen('http://127.0.0.1:4000/health/liveliness', timeout=.8) as response:
                status = response.status
        except Exception:
            pass
        probes.append({'name': 'litellm_gateway', 'status': status,
            'state': 'gateway_reachable' if status == 200 else 'probe_failed',
            'latency_ms': round((time.monotonic()-start)*1000, 3)})
    if HOST == 't3':
        for port, name in ((1080, 'socks_listener'), (8118, 'privoxy_listener')):
            try:
                connection = socket.create_connection(('127.0.0.1', port), .2); connection.close()
                state = 'listener_only'
            except OSError:
                state = 'probe_failed'
            probes.append({'name': name, 'state': state, 'status': 0, 'latency_ms': 0})
    data = {'containers': containers, 'compose': compose, 'services': services, 'probes': probes,
            'docker_error': '' if code == 0 else ('permission_or_unavailable' if code != 124 else 'timeout'),
            'compose_error': '' if code == 0 else 'docker_metadata_unavailable'}
else:
    release = dict(x.split('=', 1) for x in read('/etc/os-release').splitlines() if '=' in x)
    software, errors = [], []
    commands = [('Docker', ['docker', '--version']), ('Git', ['git', '--version']),
       ('Node.js', ['node', '--version']), ('Python', ['python3', '--version']),
       ('Nginx', ['nginx', '-v']), ('Caddy', ['caddy', 'version'])]
    for name, args in commands:
        code, text = command(args, .35, stderr_to_stdout=(name == 'Nginx'))
        if code == 0 and text.strip():
            software.append({'name': name, 'version': text.strip().splitlines()[0][:200], 'source': 'CLI / current PATH'})
        elif code not in (127,):
            errors.append(name + ':query_unavailable')
    packages = []
    if HOST == 'ali_cjlass':
        pc, value = command(['rpm', '-qa', '--qf', '%{NAME}\t%{VERSION}-%{RELEASE}\n'], 1.0)
        for line in value.splitlines() if pc == 0 else []:
            cells = line.split('\t', 1)
            if len(cells) == 2: packages.append({'name': cells[0], 'version': cells[1], 'source': 'RPM installed'})
    else:
        pc, value = command(['dpkg-query', '-W', '-f', '${db:Status-Abbrev}\t${binary:Package}\t${Version}\n'], 1.0)
        for line in value.splitlines() if pc == 0 else []:
            cells = line.split('\t', 2)
            if len(cells) == 3 and cells[0].startswith('ii'):
                packages.append({'name': cells[1], 'version': cells[2], 'source': 'dpkg installed'})
    if pc != 0: errors.append('packages:query_failed')
    data = {'os': release.get('PRETTY_NAME', '').strip('"'), 'kernel': platform.release(),
        'architecture': platform.machine(), 'cores': os.cpu_count(), 'memory_total': mem['MemTotal'],
        'disk_total': fs.f_blocks*fs.f_frsize, 'software': software,
        'packages': sorted(packages, key=lambda x:x['name'])[:5000], 'errors': errors,
        'note': '当前PATH与系统包数据库；未遍历其他用户、虚拟环境或容器内软件。未找到的CLI不代表未安装。'}
emit(data, '--gzip' in sys.argv[3:])
