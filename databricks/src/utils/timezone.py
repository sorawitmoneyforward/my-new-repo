import pytz
import datetime as dt


JST = pytz.timezone('Asia/Tokyo')


def get_now_jst():
    return dt.datetime.now(JST)
