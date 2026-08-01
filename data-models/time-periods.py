# things to build:
# - week
# - day
# - month
# - quarter
# - year

from utils import i, logs


class Week:
    def __init__(self):

        # data
        self.price_cache = {
            "monday-open": None,
            "prev-friday-close": None,
            "friday-close": None,
            "aggregations": {
                    "avg-price": None,
                    "max-price": None,
                    "min-price": None,
                },
            "week-profile": {
                    "best-day": None,
                    "worst-day": None,
                    "highest-day": None,
                    "lowest-day": None
                }
        }

        # handle errors and populate_attempts
        self.populate_attempts = 0
        self.max_populate_attempts = 5
        
        # config for the object
        self.candles = None

    def populate(self, candles='1h'):
        print("Not implemented yet")
        
        self.candles = candles

        self.populate_attempts += 1
        if self.populate_attempts > self.max_populate_attempts:
            raise:
                "Error populating Week"
