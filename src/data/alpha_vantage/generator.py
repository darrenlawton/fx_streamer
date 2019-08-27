# https://codereview.stackexchange.com/questions/188753/pull-stock-data-from-alpha-vantage-api
# https://github.com/RomelTorres/alpha_vantage/blob/develop/alpha_vantage/foreignexchange.py

# main: https://github.com/Pmlsa/AlphaVantage/tree/master/AlphaVantage
# understand this: https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
# https://urllib3.readthedocs.io/en/latest/user-guide.html#using-timeouts

import json, urllib3
from multiprocessing.pool import ThreadPool
from src.data import data_config as dc

class fxClient:

    def __init__(self):
        self.session = urllib3.HTTPConnectionPool(dc.AV_HOST)
        self.api_key = dc.validate_key(dc.AV_API_KEY)
        self.av_host = dc.AV_HOST
        self.av_function = dc.AV_FUNCTION_CALL 
        
    def get_fx_rate(self, from_currency: str, to_currency: str = 'USD'):
        payload = {
            'function': self.av_function,
            'from_currency': from_currency.upper(),
            'to_currency': to_currency.upper()
        }
        return self.av_request(parameters)

    def av_request(self, parameters: dict, timeout: int = 5):
        parameters['apikey'] = self.api_key
        try:
            response = self.session.request(
                'GET',  
                '/query', 
                fields=parameters,
                timeout=timeout
            )
        except urllib3.exceptions.NewConnectionError:
            print('Alpha vantage api connection failed.')
            pass
        finally:
            response.close()

        if response.status_code == 200:
            return json.loads(response.data)
        else: return None

    def get_fx_batch(self, from_currencies, to_currency: str = 'USD'):
        """
        threaded request for retrieving multiple
        :param from_currencies: a list of currencies
        :param to_currency: a string being the base currency
        :returns: list fx rates in json format
        """
        
        return None

    def threaded_requests(self):
        # pool = ThreadPool(len(symbols))
        # results = pool.map(function, symbols)
        # pool.close()
        # pool.join()
        #
        # return results
        return None
