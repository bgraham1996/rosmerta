

from utils import ix, dbs, caches, configs

import datetime as dt
import pandas as pd
import json, logger, os

BASE_LOGS_CONFIG = {
        "config": "base",
        "config-version": "0.0.0.1"
}


BASE_LOG_LEVELS = {
    "INFO": {
        "level": 5
    },
    "DEV": {
        "level": 7
    },
    "TEST": {
        "level": 6
    },
    "DEV-SMOKE": {
        "level": 7
    }
}

EXTENDED_LOG_LEVELS = {}

class Logs:
    def __init__(self):
        print("Not implemented yet")
        
    def init(self, config = BASE_LOGS_CONFIG,
             levels = [BASE_LOGS_CONFIG, EXTENDED_LOG_LEVELS])
        self.config = config

        return log.complete()


    def complete(self):
        returning = {
            "outcome": None
        }
        if returning["outcome"] is None:
            print("-=- -=- -=- -=- -=- Hard Error -=- -=- -=- -=- -=- -=- ")

class Object:
    def __init__(self):
        return log.complete()

