from confobj import Config


class HttpConf(Config):
    def __init__(self, order=None) -> None:
        self.ssl = False
        self.host = "localhost"
        self.port = 80
        super().__init__(order)
        self.configure()
        self.protocol = "https" if self.ssl else "http"

    def get_server_address(self) -> str:
        return "{}://{}:{}/".format(self.protocol, self.host, self.port)
