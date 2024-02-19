# Tags are Python set() objects that can define roles
# or other aspects of systems, like location or tenant.

# Tags can have any name that is a valid Python variable,
# though you should avoid overwriting any Python builtins.

# Examples:
# role_webserver = {'server-1', 'server-2'}
# role_mailserver = {'server-3'}
# role_server = role_webserver | role_mailserver
# role_desktop = {'gamepc'}

# location_home = {'server-1', 'gamepc'}
# location_colo = {'server-2', 'server-3'}

# home_servers = role_server & location_home
