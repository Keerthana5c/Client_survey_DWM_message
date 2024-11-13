import pandas as pd 
import numpy as np  
from datetime import timedelta, dt 
from database_ops import get_data_cc

# from prefect import task, Flow
from dotenv import load_dotenv

env_path = '.env'
load_dotenv(dotenv_path=env_path)



