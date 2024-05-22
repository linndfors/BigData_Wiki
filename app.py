from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
import time
import pandas as pd
import json
from datetime import datetime, timedelta


app = FastAPI()

def wait_for_cassandra():
    while True:
        try:
            cluster = Cluster(['cassandra'], port=9042) 
            session = cluster.connect() 
            print("connected")
            return session
        except Exception as e:
            print(f"Error connecting to Cassandra: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

session = wait_for_cassandra()



def get_pages_for_each_domain(session):
    while True:
        now = datetime.now()
        end_time = now - timedelta(hours=1)
        start_time = now - timedelta(hours=7)

        try:
            query = session.execute("""
                SELECT created_at, domain, COUNT(page_id) AS page_count
                FROM created_pages
                WHERE created_at >= %s AND created_at < %s AND user_is_bot = false
                GROUP BY created_at, domain;
            """, (start_time, end_time))
            query = session.execute("""
                SELECT * from created_pages;
            """)

            result = session.execute(query)
        
            aggregated_data = {}

            for row in result:
                hour = row.created_at.strftime('%Y-%m-%d %H:00:00')
                domain = row.domain
                page_count = row.page_count

                if hour not in aggregated_data:
                    aggregated_data[hour] = {}

                if domain not in aggregated_data[hour]:
                    aggregated_data[hour][domain] = 0

                aggregated_data[hour][domain] += page_count

            # Format the output
            output = []
            for hour, domains in sorted(aggregated_data.items()):
                entry = {
                    "time_start": hour,
                    "time_end": (datetime.strptime(hour, '%Y-%m-%d %H:00:00') + timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00'),
                    "statistics": [{domain: count} for domain, count in domains.items()]
                }
                output.append(entry)

        
        except Exception as e:
            print(f"Error retrieving reviews from Cassandra: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            

@app.get("/get_pages_for_domain")
def read_reviews_by_product():
    session.set_keyspace('fancy_keyspace')
    reviews = get_pages_for_each_domain(session)
    if not reviews:
        raise HTTPException(status_code=404, detail="Reviews not found")
    return JSONResponse(content=reviews)

