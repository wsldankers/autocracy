#! /usr/bin/make -f

all: $(patsubst %.cfg,%.csr,$(wildcard *.cfg))

%.cfg:
	exec sed s/template/$*/g <template >$@

%.csr: %.cfg %.key
	exec openssl req -new -utf8 -config $< -key $*.key -verify -out $@

%.pfx: %.crt %.key ca/certificate
	umask 077 && exec openssl pkcs12 -export -out $@ -inkey $*.key -in $*.crt -certfile ca/certificate -keypbe NONE -certpbe NONE -nomaciter -passout pass:

%.crt: %.csr ca/certificate
	exec flock . openssl ca -utf8 -config ca/config -batch -out $@ -in $<

%.key:
	umask 077 && exec openssl genpkey -algorithm ED25519 -out $@

ca/certificate:
	ca/create/genesis

.SECONDARY:

.PHONY: all
