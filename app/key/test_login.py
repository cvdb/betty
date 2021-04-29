# Import libraries
import betfairlightweight
from betfairlightweight import filters
import os
import datetime
import json

# Change this certs path to wherever you're storing your certificates
certs_path = r'/home/cvdb/projects/betty/app/key/'

# Change these login details to your own
my_username = "clinton.vdb@gmail.com"
my_password = "11qA22ws#"
my_app_key = "oTkHIrE7nbMAWSq1"

trading = betfairlightweight.APIClient(username=my_username,
                                       password=my_password,
                                       app_key=my_app_key,
                                       certs=certs_path)

trading.login()

# Grab all event type ids. This will return a list which we will iterate over to print out the id and the name of the sport
event_types = trading.betting.list_event_types()

sport_ids
