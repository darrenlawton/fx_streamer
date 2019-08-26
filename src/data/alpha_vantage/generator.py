# https://codereview.stackexchange.com/questions/188753/pull-stock-data-from-alpha-vantage-api
# https://github.com/RomelTorres/alpha_vantage/blob/develop/alpha_vantage/foreignexchange.py

# main: https://github.com/Pmlsa/AlphaVantage/tree/master/AlphaVantage
# understand this: https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912

import json, urllib3
from multiprocessing.pool import ThreadPool
from src.data import data_config as dc


# # Public Methods
# def fxrate(self, from_currency: str, to_currency: str = 'USD', **kwargs) -> DataFrame or None:
#     """Simple wrapper to _av_api_call method for currency requests."""
#

# def _av_api_call(self, parameters: dict, timeout: int = 60, **kwargs) -> DataFrame or json or None:
#     """Main method to handle AlphaVantage API call request and response."""
#
#     proxies = kwargs['proxies'] if 'proxies' in kwargs else self.proxy
#
#     # Everything is ok so far, add the AV API Key
#     parameters['apikey'] = self.api_key
#
#     if not self.premium and self._api_call_count > 0:
#         time.sleep(15.01)
#
#     # Ready to Go. Format and get request response
#     try:
#         response = requests.get(  # Use till self._requests_session can be mocked in unittests
#             AlphaVantage.END_POINT,
#             params=parameters,
#             timeout=timeout,
#             proxies=proxies
#         )
#     # except requests.RequestException as ex:
#     except requests.exceptions.RequestException as ex:
#         print(f"[X] response.get() exception: {ex}\n    parameters: {parameters}")
#         pass
#     finally:
#         response.close()
#
#     if response.status_code != 200:
#         print(f"[X] Request Failed: {response.status_code}.\nText:\n{response.text}\n{parameters['function']}")
#
#     # If 'json' datatype, return as 'json'. Otherwise return text response for 'csv'
#     if self.datatype == 'json':
#         response = response.json()
#     else:
#         response = response.text

#     if self._api_call_count < 1:
#         self._api_call_count += 1

class fxClient:

    def __init__(self):
        self.session = urllib3.HTTPConnectionPool(dc.AV_HOST)
        self.api_key = dc.validate_key(dc.AV_API_KEY)

    def get_fx_rate(self, from_currency: str, to_currency: str = 'USD'):
        payload = {
            'function': dc.AV_FUNCTION_CALL,
            'from_currency': from_currency.upper(),
            'to_currency': to_currency.upper()
        }

        #     download = self._av_api_call(parameters, **kwargs)
        #     return download if download is not None else None

        return None

    def av_api_request(self):


        return None

    def get_fx_batch(self, from_currencies, to_currency):
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