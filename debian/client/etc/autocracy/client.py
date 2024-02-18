base_dir = '/etc/autocracy/client'
server = 'https://autocracy'

# Before the client can start you need to set it up with
# SSL keys and certificates. Files you need to create:
#
# /etc/autocracy/client/server.crt:
# Simply copy /etc/autocracy/server/server.crt from the server.
#
# /etc/autocracy/client/client.crt and /etc/autocracy/client/client.key:
# Run this on the server (replacing yourhostname with the client's hostname):
# sudo make -C /etc/autocracy/server/pki yourhostname.crt
# Then copy /etc/autocracy/server/pki/yourhostname.crt
# and /etc/autocracy/server/pki/yourhostname.key to client.crt
# and client.key, respectively.
