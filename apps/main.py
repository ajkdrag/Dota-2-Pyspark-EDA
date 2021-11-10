import argparse
import importlib
import json
import findspark
findspark.init()

from pyspark.sql import SparkSession
from shared.config_parser import ConfigParser


def _parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    parser.add_argument("--config", default="configs/config.json")
    opt = parser.parse_args()
    return opt
    

def _parse_config(config_file):
    cfg_parser = ConfigParser(config_file)
    return cfg_parser.parse_config()


def _init_spark(config):
    return SparkSession.builder.appName(config.get("app_name")).getOrCreate()


def main():
    opt = _parse_opt()
    config = _parse_config(opt.config)    
    spark = _init_spark(config)
    
    job_module = importlib.import_module(f"jobs.{opt.job}.entrypoint")
    getattr(job_module, "run")(spark, config)
    

if __name__ == "__main__":
    main()
