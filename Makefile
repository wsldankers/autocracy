all: server.crt server.key client.crt client.key

clean:
	exec rm -f localhost.crt localhost.key server.crt server.key

localhost.crt: localhost.key
	exec openssl req -x509 -subj '/CN=localhost' -days 36524 -sha256 -out $@ -key $< -nodes

localhost.key:
	exec openssl genpkey -algorithm ED25519 -out $@

server.crt: localhost.crt
	exec ln -sf $< $@

server.key: localhost.key
	exec ln -sf $< $@

client.crt: pki/pikachu.crt
	exec ln -sf $< $@

client.key: pki/pikachu.key
	exec ln -sf $< $@

pki/pikachu.crt pki/pikachu.key:
	$(MAKE) -C pki $(patsubst pki/%,%,$@)

.PHONY: all clean
