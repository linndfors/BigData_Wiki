from collections import defaultdict
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
import time
from datetime import datetime, timedelta

app = FastAPI()

def wait_for_cassandra():
    while True:
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect()
            session.set_keyspace('fancy_keyspace')
            session.row_factory = tuple_factory
            print("connected")
            return session
        except Exception as e:
            print(f"Error connecting to Cassandra: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

session = wait_for_cassandra()

def get_pages_for_each_domain(session):
    now = datetime.now()

    now = now + timedelta(hours=7)

    end_time = now - timedelta(hours=1)
    start_time = now - timedelta(hours=7)

    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")

    print()
    print("Current time (now):", now)
    print("Start time (7 hours ago):", start_time)
    print("End time (1 hour ago):", end_time)

    try:
        query = f"""
            SELECT created_at, domain, page_id
            FROM created_pages
            WHERE created_at >= '{start_time}' AND created_at < '{end_time}' AND user_is_bot = false
            ALLOW FILTERING;
        """
        # prepared = session.prepare(query)
        result = session.execute(query)

        aggregated_data = {}
        for row in result:
            aggregated_data[row.page_id] = row.domain

        return aggregated_data

        # for row in result:
        #     created_at, domain, page_id = row
        #     hour = created_at.strftime('%Y-%m-%d %H:00:00')

        #     if hour not in aggregated_data:
        #         aggregated_data[hour] = {}

        #     if domain not in aggregated_data[hour]:
        #         aggregated_data[hour][domain] = 0

        #     aggregated_data[hour][domain] += 1  # Count each page_id

        # Format the output
        # output = []
        # for hour, domains in sorted(aggregated_data.items()):
        #     entry = {
        #         "time_start": hour,
        #         "time_end": (datetime.strptime(hour, '%Y-%m-%d %H:00:00') + timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00'),
        #         "statistics": [{"domain": domain, "count": count} for domain, count in domains.items()]
        #     }
        #     output.append(entry)

        # return output

    except Exception as e:
        print(f"Error retrieving reviews from Cassandra: {e}")
        print("Retrying in 5 seconds...")
        time.sleep(5)
        return []

@app.get("/test")
def testing():
    return JSONResponse(content=[1, 2, 3, 4, 5])

@app.get("/get_pages_for_domain")
def read_reviews_by_product():
    reviews = get_pages_for_each_domain(session)
    if not reviews:
        raise HTTPException(status_code=404, detail="Reviews not found")
    return JSONResponse(content=reviews)


@app.get("/domains")
async def get_domains():
    # session.execute("USE fancy_keyspace;")
    query = f"SELECT DISTINCT domain FROM pages;"
    result_query = session.execute(query)
    result_query = list(result_query)
    
    if len(result_query) != 0:
        domains = [item[0] for item in result_query]

        output = [{"Domains": domains}]
        return output
    else:
        raise HTTPException(status_code=404, detail="Not Found")
    

@app.get("/users/{user_id}/pages")
async def get_pages_by_user(user_id: str):
    query = f"SELECT page_id, page_title FROM pages WHERE user_id = '{user_id}';"
    result_query = session.execute(query)
    result_query = list(result_query)
    
    if len(result_query) != 0:
        output_data = [{"page_id": int(item[0]), "page_name": item[1]} for item in result_query]
        return output_data
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified user_id")
    

@app.get("/domains/{domain}/number-pages")
async def get_number_of_pages_by_domain(domain: str):
    query = f"SELECT COUNT(*) FROM pages WHERE domain = '{domain}';"
    result_query = session.execute(query)
    result_query = list(result_query)
    
    if len(result_query) != 0:
        result_query = [{"Number of pages created for specified domain": result_query[0][0]}]
        return result_query
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified domain")
    

@app.get("/pages")
async def get_page_user_info(start_date: datetime, end_date:datetime):
    query = f"SELECT user_id, page_id, page_title FROM pages WHERE created_at >= '{start_date}' AND created_at <= '{end_date}' ALLOW FILTERING"
    result_query = session.execute(query)
    result_query= list(result_query)
    
    user_pages = defaultdict(list)

    for user_id, page_id, page_title in result_query:
        user_pages[user_id].append({
            "page_id": page_id,
            "page_title": page_title
        })

    users_json = {
        "users": [
            {
                "user_id": user_id,
                "pages": pages,
                "number_of_pages": len(pages)
            }
            for user_id, pages in user_pages.items()
        ]
    }
    
    if len(result_query) != 0:
        return users_json
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified page_id")

    

