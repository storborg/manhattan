from .text import TextLog
from ..client import Client


class RemoteLog(Client, TextLog):

    """Sends log entries to a remote server."""

    def write(self, elements):
        """Send ``elements`` to remote."""
        self.log(elements)
