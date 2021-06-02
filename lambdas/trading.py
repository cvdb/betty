import os
import betfairlightweight

def get_trading():
    # create trading instance
    # in this case we expect cert to be in same location as this file.
    dir_path = os.path.dirname(os.path.realpath(__file__))
    my_username = "???"
    my_password = "???"
    my_app_key = "???"
    trading = betfairlightweight.APIClient(username=my_username,
                                           password=my_password,
                                           app_key=my_app_key,
                                           certs=dir_path)
    trading.login()
    return trading

