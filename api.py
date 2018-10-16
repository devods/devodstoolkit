import os
import configparser
import datetime
import json
import hashlib
import hmac
import requests



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


    def query(self, link_query, start, stop=None, mode='csv', stream=False):
        """
        Run a link query and return the results

        start: The start time for the query.  Can be a unix timestamp in seconds,
        a python datetime object, or string in form 'YYYY-mm-dd'

        stop: End time of the query in the same format as start.
        Set stop to None for a continuous query
        """
        if link_query.endswith('.link'):
            with open(link_query, 'r') as f:
                query_text = f.read()
        else:
            query_text = link_query


        if stop is None:
            stream = True


        if mode not in ('csv','tsv') and stream:
            raise Exception('only csv/tsv formats can be streamed')

        r = self._make_request(query_text, start, stop, mode, stream)

        if stream:
            return r.iter_lines()
        else:
            return r.text



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
            epoch = dt.timestamp()

        elif type(date) == datetime.datetime:
            epoch = date.timestamp()

        elif isinstance(date, (int,float)):
            epoch = date


        if milliseconds:
            epoch *= 1000

        return str(int(epoch))


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


    def _make_request(self, query_text, start, stop, mode, stream):


        start = self._to_unix(start)
        stop = self._to_unix(stop)

        ts = self._to_unix('now', milliseconds=True)


        body = json.dumps({'query': query_text,
                           'from': start,
                           'to': stop,
                           'mode': {'type': mode}}
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










