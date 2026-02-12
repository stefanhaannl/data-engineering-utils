from abc import ABC


class Connection(ABC):

    connection_type: str

    def __init__(self):
        pass

    def __repr__(self):
        return f"{self.connection_type}Connection"
