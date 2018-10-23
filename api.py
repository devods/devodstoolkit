import os
import sys
import configparser
import datetime
from datetime import timezone
import json
import hashlib
import hmac
import requests
import csv
from collections import namedtuple,defaultdict
import numpy as np
import pandas as pd


class API(object):

    def __init__(self, profile='default', api_key=None, api_secret=None, end_point=None):
        self.profile = profile
        self.api_key = api_key
        self.api_secret = api_secret
        self.end_point = end_point

        if not all([api_key, api_secret, end_point]):
            self._read_profile()

        if not all([self.api_key, self.api_secret, self.end_point]):
            raise Exception('API keys and end point must be specified or in ~/.devo_credentials')



    def _read_profile(self):
        """
        Read Devo API keys from a credentials file located
        at ~/.devo_credentials if credentials are not provided

        Use profile to specify which set of credentials to use
        """

        config = configparser.ConfigParser()
        credential_path = os.path.join(os.path.expanduser('~'), '.devo_credentials')
        config.read(credential_path)

        if self.profile in config:
            self.api_key = config.get(self.profile, 'api_key')
            self.api_secret = config.get(self.profile, 'api_secret')
            self.end_point = config.get(self.profile, 'end_point')


    def _query(self, linq_query, start, stop=None, mode='csv', stream=False, limit=None):
        """
        Run a link query and return the results

        start: The start time for the query.  Can be a unix timestamp in seconds,
        a python datetime object, or string in form 'YYYY-mm-dd'

        stop: End time of the query in the same format as start.
        Set stop to None for a continuous query
        """
        if linq_query.endswith('.linq'):
            with open(linq_query, 'r') as f:
                query_text = f.read()
        else:
            query_text = linq_query


        if stop is None:
            stream = True


       # if mode not in ('csv','tsv') and stream:
        #    raise Exception('only csv/tsv formats can be streamed')

        r = self._make_request(query_text, start, stop, mode, stream, limit)

        if stream:
            return r.iter_lines()
        else:
            return r.text


    @staticmethod
    def _null_decorator(f):
        def null_f(v):
            if v == '':
                return None
            else:
                return f(v)
        return null_f

    def _get_types(self,linq_query,start):
        '''
        Gets types of each column of submitted
        '''

        start = self._to_unix(start)
        stop = start + 1

        funcs = {
                'timestamp':lambda t: datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f'),
                'str': str,
                'int8': int,
                'int4': int,
                'float8': float,
                'float4': float,
                'bool': lambda b: b == 'true'
               }

        map = defaultdict(lambda: str, {t:self._null_decorator(f) for t,f in funcs.items()})

        data = self._query(linq_query, start=start, stop=stop, mode='json/compact', limit=1)
        col_data = json.loads(data)['object']['m']

        type_dict = { k:map[v['type']] for k,v in col_data.items() }

        return type_dict




    @staticmethod
    def _to_unix(date, milliseconds=False):
        """
        Convert date to a unix timestamp in seconds

        date: A unix timestamp in second, a python datetime object,
        or string in form 'YYYY-mm-dd'
        """

        if date is None:
            return None

        elif date == 'now':
            epoch = datetime.datetime.now().timestamp()

        elif type(date) == str:
            dt = datetime.datetime.strptime(date, '%Y-%m-%d')
            epoch = dt.replace(tzinfo=timezone.utc).timestamp()

        elif type(date) == datetime.datetime:
            epoch = date.replace(tzinfo=timezone.utc).timestamp()

        elif isinstance(date, (int,float)):
            epoch = date


        if milliseconds:
            epoch *= 1000

        return int(epoch)


    def load(self, source, location, historical=False, date_col=None):
        """
        Load data into Devo.

        source: file or stream used to send data into Devo.

        location: where to load the data in Devo

        historical: if False, load data with actual time data is sent to Devo.
        If true, specify date of each row sent in

        date_col: if historical is true, specify which col of input data
        should be used as date.  If None, use the first column
        """
        pass


    def _make_request(self, query_text, start, stop, mode, stream, limit):


        start = self._to_unix(start)
        stop = self._to_unix(stop)

        ts = self._to_unix('now', milliseconds=True)
        ts = str(ts)


        body = json.dumps({'query': query_text,
                           'from': start,
                           'to': stop,
                           'mode': {'type': mode},
                           'limit': limit
                           }
                          )



        msg = self.api_key + body + ts
        sig = hmac.new(self.api_secret.encode(),
                       msg.encode(),
                       hashlib.sha256).hexdigest()

        headers = {
            'Content-Type': 'application/json',
            'x-logtrust-apikey': self.api_key,
            'x-logtrust-sign': sig,
            'x-logtrust-timestamp': ts
        }


        r = requests.post(
            self.end_point,
            data=body,
            headers=headers,
            stream=stream
        )

        return r


    @staticmethod
    def _decode_results(r):
        for l in r:
            yield l.decode('utf-8')



    def _stream(self, linq_query, start, stop=None):
        """
        yields columns names then rows in lists with converted
        types
        """

        type_dict = self._get_types(linq_query, start)

        result = self._query(linq_query, start, stop, mode = 'csv', stream = True)
        result = self._decode_results(result)

        reader = csv.reader(result)
        cols = next(reader)

        assert len(cols) == len(type_dict), "Duplicate column names encountered, custom columns must be named"

        type_list = [type_dict[c] for c in cols]

        yield cols

        for row in reader:
            yield [t(v) for t, v in zip(type_list, row)]


    def _stream_json(self, linq_query, start, stop):

        results  = self._query(linq_query, start, stop, mode='json/simple/compact', stream=True)

        header = next(results)
        cols = json.loads(header)['m'].keys()
        yield cols

        for r in results:
            yield json.loads(r)['d']



    def query(self, linq_query, start, stop=None, output='dict', stream_type='csv'):


        valid_outputs = ('dict', 'list', 'namedtuple', 'dataframe')
        assert output in valid_outputs, "method must be in {0}".format(valid_outputs)

        assert not (output=='dataframe' and stop is None), "DataFrame can't be build from continuous query"


        if stream_type == 'csv':
            results = self._stream(linq_query,start,stop)

        elif stream_type == 'json':
            results = self._stream_json(linq_query,start,stop)


        cols = next(results)


        return getattr(self, '_to_{0}'.format(output))(results,cols)


    @staticmethod
    def _to_list(results,cols):
        yield from results

    @staticmethod
    def _to_dict(results, cols):
        for row in results:
            yield {c:v for c,v in zip(cols,row)}

    @staticmethod
    def _to_namedtuple(results, cols):
        Row = namedtuple('Row', cols)
        for row in results:
            yield Row(*row)

    @staticmethod
    def _to_dataframe(results,cols):
        return pd.DataFrame(results, columns=cols).fillna(np.nan)







if __name__ == "__main__":
    a = API()

    start = '2018-10-10'
    start_plus = 1539129601
    stop = '2018-10-12'



q = '''
from my.app.mlvappdev.groceries
  select *
'''

