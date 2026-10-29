package fleet

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func TestSSHUsesPinnedHostAlgorithmAndRejectsChangedKey(t *testing.T) {
	_, trusted, _ := ed25519.GenerateKey(rand.Reader)
	_, changed, _ := ed25519.GenerateKey(rand.Reader)
	_, clientKey, _ := ed25519.GenerateKey(rand.Reader)
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trustedSigner, _ := ssh.NewSignerFromKey(trusted)
	ecdsaSigner, _ := ssh.NewSignerFromKey(ecdsaKey)
	for _, test := range []struct {
		name string
		key  ed25519.PrivateKey
		ok   bool
	}{{"mixed_algorithms_with_pinned_key", trusted, true}, {"changed_ed25519_key", changed, false}} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			dir := t.TempDir()
			private, err := x509.MarshalPKCS8PrivateKey(clientKey)
			if err != nil {
				t.Fatal(err)
			}
			keyPath, hostsPath := filepath.Join(dir, "key"), filepath.Join(dir, "known_hosts")
			if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(hostsPath, []byte(knownhosts.Line([]string{listener.Addr().String()}, trustedSigner.PublicKey())+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			runner, err := NewRunner(Config{SSHKey: keyPath, KnownHosts: hostsPath})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close()
			serverKey, _ := ssh.NewSignerFromKey(test.key)
			server := &ssh.ServerConfig{NoClientAuth: true}
			server.AddHostKey(ecdsaSigner)
			server.AddHostKey(serverKey)
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				sshConn, _, _, err := ssh.NewServerConn(conn, server)
				if err == nil {
					sshConn.Close()
				}
			}()
			conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			sshConn, _, _, err := ssh.NewClientConn(conn, listener.Addr().String(), runner.clientConfig("test"))
			if sshConn != nil {
				sshConn.Close()
			}
			conn.Close()
			<-done
			if (err == nil) != test.ok {
				t.Fatalf("host-key verification result mismatch: %v", err)
			}
		})
	}
}
