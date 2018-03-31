class Server(object):
    def __init__(self, name, server_type):
        self.name = name
        self.type = server_type

"""
server may be accesible via http
it may be mounted too
when using locally - explain where is mounted
real-time rewrite (no offline - only store relative path)
when user has no mount then download

"""