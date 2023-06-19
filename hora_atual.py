from datetime import datetime
from pytz import timezone
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession

spark = SparkSession.builder \
           .appName('data_atual') \
           .getOrCreate()


currentdate = current_date()
now = datetime.now(timezone('Africa/Lagos'))
today = datetime.today()
utcnow = datetime.utcnow()

print("current_date(): ", currentdate, currentdate.astype)
print("datetime.now(pytz.timezone('Africa/Lagos')): ", now, type(now))
print('datetime.today(): ', today, type(today))
print('datetime.utcnow(): ', utcnow, type(utcnow))

# diferença tem relação com controle para setar utc zone ou não
# https://stackoverflow.com/questions/32517248/what-is-the-difference-between-python-functions-datetime-now-and-datetime-t
# https://medium.com/@uzzaman.ahmed/pyspark-date-time-functions-a-comprehensive-guide-b250e92df264
