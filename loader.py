import os
import configparser
import socket
import ssl
import sys
import csv
import numpy as np
from collections import abc




class Loader:

    def __init__(self, profile='default', key=None, crt=None, chain=None, relay=None, timeout=2):

        self.profile = profile
        self.key = key
        self.crt = crt
        self.chain = chain
        self.relay = relay


        self.timeout = timeout


        if not all([key, crt, chain, relay]):
            self._read_profile()

        if not all([self.key, self.crt, self.chain, self.relay]):
            raise Exception('Credentials and relay must be specified or in ~/.devo_credentials')

        self.address = (self.relay, 443)
        self._connect_socket()


    def _read_profile(self):

        config = configparser.ConfigParser()
        credential_path = os.path.join(os.path.expanduser('~'), '.devo_credentials')
        config.read(credential_path)

        if self.profile in config:
            profile_config = config[self.profile]

            self.key = profile_config.get('key')
            self.crt = profile_config.get('crt')
            self.chain = profile_config.get('chain')
            self.relay = profile_config.get('relay')



    def _connect_socket(self):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)

        self.sock = ssl.wrap_socket(self.sock,
                               keyfile=self.key,
                               certfile=self.crt,
                               ca_certs=self.chain,
                               cert_reqs=ssl.CERT_REQUIRED)

        self.sock.connect(self.address)


    def send_file(self, file):
        self.sock.sendfile(file)


    @staticmethod
    def _make_message_header(tag, historical):
        hostname = socket.gethostname()

        if historical:
            tag = '(usd)' + tag

        prefix = '<14>{0} '

        return prefix + '{0} {1}: '.format(hostname, tag)


    def load_file(self, file_path, tag, historical=True, ts_index=None):

        with open(file_path, 'r') as f:
            data = csv.reader(f)

            num_cols = len(next(data)) - 1
            f.seek(0)

            self._load(data, tag, historical, ts_index)

        self._build_linq(tag, num_cols)


    def _load(self, data, tag, historical, ts_index, chunk_size=50):
        """

        :param data: iterable of either lists/tuples or dicts
        :param tag:
        :param historical:
        :param ts_index:
        :return:
        """

        message_header_base = self._make_message_header(tag, historical)

        counter = 0
        bulk_msg = ''

        for row in data:

            ts = row.pop(ts_index) + '.000'
            header = message_header_base.format(ts)

            bulk_msg += self._make_msg(header, row)
            counter += 1

            if counter == chunk_size:
                self.sock.sendall(bulk_msg.encode())
                counter = 0
                bulk_msg = ''

        if bulk_msg:
            self.sock.sendall(bulk_msg.encode())


    @staticmethod
    def _make_msg(header, row):
        """
        Takes row (without timestamp)

        Concats column values in row
        calculates string indices of
        where columns start and begin

        :param row: list with column values as strings
        :return: string in form ofL indices<>cols
        """

        lengths = [len(s) for s in row]
        lengths.insert(0, 0)

        indices = np.cumsum(lengths)
        indices = ','.join(str(i) for i in indices)

        row_concated = ''.join(row)

        msg = indices + '<>' + row_concated

        return header + msg + '\n'


    @staticmethod
    def _process_seq(data, first):
        yield [str(c) for c in first]
        for row in data:
            yield [str(c) for c in row]


    @staticmethod
    def _process_mapping(data, first, ts_name):
        names =  list(first.keys())
        names.remove(ts_name)
        names += [ts_name]

        yield [str(first[c]) for c in names]

        for row in data:
            yield [str(row[c]) for c in names]


    def load(self, data, tag, historical=True, ts_index=None, ts_name=None):

        data = iter(data)
        first = next(data)
        num_cols = len(first) - 1


        if isinstance(first, abc.Sequence):
            data = self._process_seq(data, first)
        elif isinstance(first, abc.Mapping):
            ts_index = num_cols
            data = self._process_mapping(data, first, ts_name)

        if historical:
            chunk_size = 50
        else:
            chunk_size = 1


        self._load(data, tag, historical, ts_index, chunk_size)

        self._build_linq(tag, num_cols)


    def _build_linq(self, tag, num_cols, columns=None):

        if columns is None:
            columns = ['col_{0}'.format(i) for i in range(num_cols)]

        col_extract = '''
        select substring(payload,
        int(split(indicies, ",", {i})),
        int(split(indicies, ",", {i}+1)) - int(split(indicies, ",", {i}))
        ) as {col_name}
        '''

        linq = '''
        from {tag}

        select split(message, "<>", 0) as indicies
        select subs(message, re("[0-9,]*<>"), template("")) as payload

        '''.format(tag=tag)

        for i, col_name in zip(range(num_cols), columns):
            linq += col_extract.format(i=i, col_name=col_name)

        print(linq)

