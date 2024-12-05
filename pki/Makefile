#! /usr/bin/make -f

all: $(patsubst %.cfg,%.csr,$(wildcard *.cfg))

%.cfg:
	exec sed s/template/$*/g <template >$@

%.key:
	umask 077 && exec openssl genpkey -algorithm ED25519 -out $@

%.csr: %.cfg %.key
	exec openssl req -new -utf8 -config $< -key $*.key -verify -out $@

%.crt: %.csr ca/certificate
	exec flock . openssl ca -utf8 -config ca/config -batch -out $@ -in $<

%.REVOKE:
	exec flock . openssl ca -utf8 -config ca/config -batch -revoke $*.crt
	$(MAKE) ca/crl

ca/crl:
	exec flock . openssl ca -utf8 -config ca/config -batch -gencrl -out $@

ca/certificate:
	exec flock . ca/create/genesis

.SECONDARY:

.PHONY: all ca/crl %.REVOKE
