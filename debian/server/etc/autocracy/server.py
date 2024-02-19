import os

user = 'autocracy'
admin_users = {user, 'root'}
base_dir = '/etc/autocracy/server'
repository_root = '/etc/autocracy/server/repository'
control_socket_path = f"{os.environ['RUNTIME_DIRECTORY']}/control"
