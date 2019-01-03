import os
import sys
import re
import configparser
import datetime
from datetime import timezone
import json
import hashlib
import hmac
import requests
import csv
from collections import namedtuple, defaultdict
import numpy as np
import pandas as pd
from scipy.stats import norm


csv.field_size_limit(sys.maxsize)


class API(object):

    def __init__(self, profile='default', api_key=None, api_secret=None, end_point=None, oauth_token=None):
        self.profile = profile
        self.api_key = api_key
        self.api_secret = api_secret
        self.end_point = end_point
        self.oauth_token = oauth_token



        if not ( self.end_point and (self.oauth_token or (self.api_key and self.api_secret))):
            self._read_profile()

        if not (self.end_point and (self.oauth_token or (self.api_key and self.api_secret))):
            raise Exception('End point and either API keys or OAuth Token must be specified or in ~/.devo_credentials')


        self._make_type_map()

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
            profile_config = config[self.profile]

            self.api_key = profile_config.get('api_key')
            self.api_secret = profile_config.get('api_secret')
            self.end_point = profile_config.get('end_point')
            self.oauth_token = profile_config.get('oauth_token')

        if self.end_point == 'USA':
            self.end_point = 'https://api-us.logtrust.com/search/query'
        if self.end_point == 'EU':
            self.end_point = 'https://api-eu.logtrust.com/search/query'

    def query(self, linq_query, start, stop=None, output='dict'):

        valid_outputs = ('dict', 'list', 'namedtuple', 'dataframe')
        assert output in valid_outputs, "output must be in {0}".format(valid_outputs)

        assert not (output=='dataframe' and stop is None), "DataFrame can't be build from continuous query"

        results = self._stream(linq_query,start,stop)
        cols = next(results)


        return getattr(self, '_to_{0}'.format(output))(results,cols)

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


        r = self._make_request(query_text, start, stop, mode, stream, limit)

        if stream:
            return r.iter_lines()
        else:
            return r.text

    def _make_request(self, query_text, start, stop, mode, stream, limit):


        start = self._to_unix(start)
        stop = self._to_unix(stop)

        ts = self._to_unix('now', milliseconds=True)
        ts = str(ts)


        body = json.dumps({
            'query': query_text,
            'from': start,
            'to': stop,
            'mode': {'type': mode},
            'limit': limit
        })



        if self.api_key and self.api_secret:

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

        elif self.oauth_token:

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.oauth_token}

        else:
            raise Exception('No credentials found')




        r = requests.post(
            self.end_point,
            data=body,
            headers=headers,
            stream=stream
        )

        return r

    @staticmethod
    def _null_decorator(f):
        def null_f(v):
            if v == '':
                return None
            else:
                return f(v)
        return null_f

    def _make_type_map(self):

        funcs = {
                'timestamp': lambda t: datetime.datetime.strptime(t.strip(), '%Y-%m-%d %H:%M:%S.%f'),
                'str': str,
                'int8': int,
                'int4': int,
                'float8': float,
                'float4': float,
                'bool': lambda b: b == 'true'
               }

        self._map = defaultdict(lambda: str, {t:self._null_decorator(f) for t,f in funcs.items()})

    def _get_types(self,linq_query,start):
        """
        Gets types of each column of submitted
        """

        # so we don't have  stop ts in future as required by API V2
        stop = self._to_unix(start)
        start = stop - 1


        response = self._query(linq_query, start=start, stop=stop, mode='json/compact', limit=1)
        data = json.loads(response)

        assert data['status'] == 0, 'Query Error'

        col_data = data['object']['m']

        type_dict = { k:self._map[v['type']] for k,v in col_data.items() }

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
            epoch = pd.to_datetime(date).timestamp()
        elif type(date) == datetime.datetime:
            epoch = date.replace(tzinfo=timezone.utc).timestamp()
        elif isinstance(date, (int,float)):
            epoch = date
        else:
            raise Exception('Invalid Date')

        if milliseconds:
            epoch *= 1000

        return int(epoch)

    @staticmethod
    def _decode_results(r):
        r = iter(r)

        # catch error not reported for json
        first = next(r)
        query_error = False
        try:
            query_error = not (json.loads(first)['status'] == 0)
        except:
            pass

        assert not query_error, 'Query Error'


        yield  first.decode('utf-8').strip()  # APIV2 adding space to first line of aggregates
        for l in r:
            yield l.decode('utf-8')

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

    def randomSample(self,linq_query,start,stop,sample_size):

        size_query = linq_query + ' group select count() as count'

        r = self.query(size_query,start,stop,output='list')
        table_size = next(r)[0]

        p = self._find_optimal_p(n=table_size,k=sample_size,threshold=0.99)

        sample_query = linq_query + ' where simplify(float8(rand())) < {0} '.format(p)

        while True:
            df = self.query(sample_query,start,stop,output='dataframe')

            if df.shape[0] >= sample_size:
                return df.sample(sample_size)
            else:
                pass

    @staticmethod
    def _loc_scale(n,p):
        """
        Takes parameters of a binomial
        distribution and finds the mean
        and std for a normal approximation

        :param n: number of trials
        :param p: probability of success
        :return: mean, std
        """
        loc = n*p
        scale = n*p*(1-p)

        return loc,scale

    def _find_optimal_p(self,n,k,threshold):
        """
        Use a normal approximation to the
        binomial distribution.  Starts with
        p such that mean of B(n,p) = k
        and iterates.

        :param n: number of trials
        :param k: desired number of successes
        :param threshold: desired probability to achieve k successes

        :return: probability of single trial that will yield
                 k success with n trials with probability of threshold

        """
        p = k / n
        while True:
            loc, scale = self._loc_scale(n, p)
            # sf = 1 - cdf, but can be more accurate according to scipy docs
            if norm.sf(x=k - 1, loc=loc, scale=scale) > threshold:
                break
            else:
                p *= 1.001

        return p

    def randomSampleColumn(self):
        """
        specify a linq query
        and specify a column to sample by
        and specify number of distinct values

        ie sample by phone number

        find all distinct phone numbers that
        meet the filter/where clause and time range
        in the linq / start + stop times provided
        (con't be con't query)

        random pick distinct phone numbers based on
        specified distinc values

        run specified linq query but filter to only
        rows that have selected phone numbers


        :return:
        """

        pass


class QueryError(Exception):
    pass


def check_status(data):
    status = data['status']

    if status == 0:
        return
    elif status == 400:
        message = data.get('object', ['400 error'])[0]
    # missing from clause or other access error (ie oauth token problem)
    elif status == 403:
        message = process_403(data)
    # date issue
    # general query issue
    elif status == 500:
        message = process_500(data)
    else:
        message = status

    raise QueryError(message)


def process_403(data):
    error = data.get('error', '')
    msg = data.get('msg', '')

    if error == "Access not allowed for table 'Nothing'":
        return error + ". Possible error in from clause"
    elif not (error or msg):
        return '403 error'
    else:
        return '. '.join(m for m in [msg, error] if m)


def process_500(data):
    try:
        full_error = data.get('object')[1]
    except:
        error = data.get('error', '')
        msg = data.get('msg', '')
        if not (error or msg):
            return '500 error'
        else:
            return '. '.join(m for m in [msg, error] if m)

    exception_pattern = r'Error from server: malote\.(.*?): '
    linq_pattern = r'as an Linq query\. Error:(.*)'
    error_pattern = r'malote\.{exception}: (.*?) \[MConnectionImpl\[address'

    match = re.search(exception_pattern, full_error)
    exception_type = match.group(1)

    if exception_type == 'code.CodeParseException':
        message = re.search(linq_pattern, full_error).group(1)
    elif exception_type in ('base.StaticException', 'typing.TypingException'):
        error_pattern = error_pattern.format(exception=exception_type)
        message = re.search(error_pattern, full_error).group(1)
    else:
        message = exception_type

    return message.replace('<?>@', 'Row,Column').strip()


if __name__ == "__main__":
   a = API()


