from pymongo import MongoClient, errors
from datetime import datetime, timezone
import json

class DataFilter:
    def __init__(self, config_file="config.json"):
        try:
            with open(config_file, "r") as file:
                config = json.load(file)
            
            self.mongo_client = MongoClient(config["mongodb"]["host"], config["mongodb"]["port"])
            self.db = self.mongo_client[config["mongodb"]["database"]]
            self.collection = self.db[config["mongodb"]["collection"]]
        except FileNotFoundError:
            raise FileNotFoundError(f"Error: Config file '{config_file}' not found.")
        except json.JSONDecodeError:
            raise ValueError("Error: Invalid JSON format in the config file.")
        except errors.ConnectionError:
            raise ConnectionError("Error: Could not connect to MongoDB server.")

    def _convert_to_timestamp(self, date_str, is_end_time=False):
        try:
            if len(date_str) == 10:
                date_str += " 23:59:59" if is_end_time else " 00:00:00"
            elif len(date_str) == 11:
                date_str += "23:59:59" if is_end_time else "00:00:00"

            date_naive = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            date_utc = date_naive.replace(tzinfo=timezone.utc)
            return int(date_utc.timestamp())
        except ValueError:
            raise ValueError(f"Warning: Date format should be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. Received '{date_str}'.")

    def filter_data(self, sensori, giorno_1, giorno_2, ora, campi):
        results = {} 

        try:
            if not sensori or not isinstance(sensori, list):
                raise ValueError("Error: 'sensori' should be a non-empty list of sensor IDs.")
            if not all(isinstance(s, str) for s in sensori):
                raise ValueError("Error: All elements in 'sensori' should be strings.")
            if not isinstance(ora, int) or ora <= 0:
                raise ValueError("Error: 'ora' should be a positive integer representing minutes.")
            if not campi or not isinstance(campi, list):
                raise ValueError("Error: 'campi' should be a non-empty list of field names.")

            timestamp_unix_1 = self._convert_to_timestamp(giorno_1)
            timestamp_unix_2 = self._convert_to_timestamp(giorno_2, is_end_time=True)

            for sensore in sensori:
                pipeline = [
                    {'$match': {
                        'sender.serialNr': sensore,
                        'measurements.ts': {'$gte': timestamp_unix_1, '$lte': timestamp_unix_2}
                    }},
                    {'$unwind': '$measurements'},
                    {'$addFields': {
                        "rounded_ts": {
                            "$toLong": {
                                "$multiply": [
                                    {"$floor": {"$divide": ["$measurements.ts", 60 * ora]}},
                                    60 * ora
                                ]
                            }
                        }
                    }},
                    {'$group': {
                        '_id': '$rounded_ts',
                        **{f'avg_{campo}': {'$avg': f'$measurements.{campo}'} for campo in campi},
                        **{f'max_{campo}': {'$max': f'$measurements.{campo}'} for campo in campi},
                        **{f'min_{campo}': {'$min': f'$measurements.{campo}'} for campo in campi},
                    }},
                    {'$sort': {'_id': 1}}
                ]

                try:
                    query = list(self.collection.aggregate(pipeline))
                    sensor_data = []
                    sensor_warnings = {}

                    if not query:
                        sensor_warnings["not found"] = f"No data avaible for sensor '{sensore}'."
                    else:
                        for campo in campi:
                            if all(doc.get(f'avg_{campo}') is None for doc in query):
                                sensor_warnings[campo] = f"No data found for '{campo}'."
                        
                        sensor_data = [
                            {
                                "bucket": datetime.fromtimestamp(doc['_id'], timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                **{
                                    campo: {
                                        "avg": doc.get(f'avg_{campo}'),
                                        "max": doc.get(f'max_{campo}'),
                                        "min": doc.get(f'min_{campo}')
                                    } for campo in campi if doc.get(f'avg_{campo}') is not None
                                }
                            }
                            for doc in query
                        ]
                    
                    results[sensore] = {"data": sensor_data, "warning": sensor_warnings}

                except errors.PyMongoError as e:
                    results[sensore] = {"data": [], "warning": {"general": f"Error querying MongoDB: {e}"}}

        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception(f"An unexpected error occurred: {e}")

        return results
