import struct
from socket import inet_aton, inet_ntoa, error as socket_error

from sqlalchemy import types


class IP(types.TypeDecorator):
    """
    IP address type. Uses an integer at the database level, and converts to
    and from a string with python's socket module inet_ntoa/inet_aton
    functions.
    """
    impl = types.Integer

    def process_bind_param(self, value, dialect):
        """
        Convert value from our type to database type, by using the python
        socket module's inet_aton.
        """
        if value == '::1':
            # Deal with IPv6 loopback crap.
            value = '127.0.0.1'
        if value == None:
            return None
        else:
            try:
                return struct.unpack('!l', inet_aton(value))[0]
            except socket_error:
                raise ValueError(value)

    def process_result_value(self, value, dialect):
        """
        Convert value from database type to our type, by using the python
        socket module's inet_ntoa.
        """
        if value == None:
            return None
        else:
            return inet_ntoa(struct.pack('!l', value))
