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


### Category B

def get_pages_for_each_domain(session):
    now = datetime.now()
    # add 2 hours, to work in EEST timezone
    now = now + timedelta(hours=2)

    end_time = now - timedelta(hours=1)
    start_time = now - timedelta(hours=7)

    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")

    print("Current time (now):", now)
    print("Start time (7 hours ago):", start_time)
    print("End time (1 hour ago):", end_time)

    query = f"""
        SELECT created_at, domain, page_id
        FROM created_pages
        WHERE created_at >= '{start_time}' AND created_at < '{end_time}' AND user_is_bot = false
        ALLOW FILTERING;
    """
    result = session.execute(query)
    result_query= list(result)
    aggregated_data = defaultdict(dict)

    for created_at, domain, page_id in result_query:
        hour = created_at.strftime('%Y-%m-%d %H:00:00')
        if hour not in aggregated_data:
            aggregated_data[hour] = {}
        if domain not in aggregated_data[hour]:
            aggregated_data[hour][domain] = 0
        aggregated_data[hour][domain] += 1

    output = []
    for hour, domains in sorted(aggregated_data.items()):
        entry = {
            "time_start": hour,
            "time_end": (datetime.strptime(hour, '%Y-%m-%d %H:00:00') + timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00'),
            "statistics": [{domain: count} for domain, count in domains.items()]
        }
        output.append(entry)

    return output


def get_pages_for_domain_by_bots(session):
    now = datetime.now()

    now = now + timedelta(hours=2)

    end_time = now - timedelta(hours=1)
    start_time = now - timedelta(hours=7)

    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")

    print("Current time (now):", now)
    print("Start time (7 hours ago):", start_time)
    print("End time (1 hour ago):", end_time)

    query = f"""
        SELECT created_at, domain, page_id
        FROM created_pages
        WHERE created_at >= '{start_time}' AND created_at < '{end_time}' AND user_is_bot = true
        ALLOW FILTERING;
    """
    result = session.execute(query)
    result_query= list(result)
    aggregated_data = defaultdict(dict)

    for created_at, domain, page_id in result_query:
        hour = created_at.strftime('%Y-%m-%d %H:%M:00')
        if hour not in aggregated_data:
            aggregated_data[hour] = {}
        if domain not in aggregated_data[hour]:
            aggregated_data[hour][domain] = 0
        aggregated_data[hour][domain] += 1

    output = []
    for hour, domains in sorted(aggregated_data.items()):
        entry = {
            "time_start": hour,
            "time_end": (datetime.strptime(hour, '%Y-%m-%d %H:%M:00') + timedelta(minutes=6)).strftime('%Y-%m-%d %H:%M:00'),
            "statistics": [{"domain": domain, "created_by_bots": count} for domain, count in domains.items()]
        }
        output.append(entry)

    return output

@app.get("/top_users")
def get_top_users():
    now = datetime.now()

    now = now + timedelta(hours=2)

    end_time = now - timedelta(hours=1)
    start_time = now - timedelta(hours=7)

    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f+0000")

    print("Current time (now):", now)
    print("Start time (7 hours ago):", start_time)
    print("End time (1 hour ago):", end_time)

    query = f"""
        SELECT user_id, user_text, page_title
        FROM for_user
        WHERE created_at >= '{start_time_str}' AND created_at < '{end_time_str}'
        ALLOW FILTERING;
    """
    result = session.execute(query)
    result_query = list(result)

    user_data = defaultdict(list)

    for user_id, user_text, page_title in result_query:
        user_data[(user_id, user_text)].append(page_title)

    top_users = sorted(user_data.items(), key=lambda item: len(item[1]), reverse=True)[:20]

    output = []
    for (user_id, user_text), pages in top_users:
        output.append({
            "user_id": user_id,
            "user_name": user_text,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "pages": pages,
            "number_of_pages": len(pages)
        })

    return output


@app.get("/get_pages_for_domain")
def read_domains_count():
    pages = get_pages_for_each_domain(session)
    if not pages:
        raise HTTPException(status_code=404, detail="Results not found")
    return JSONResponse(content=pages)


@app.get("/get_pages_for_domain_by_bots")
def read_pages_for_domain_by_bots():
    pages = get_pages_for_domain_by_bots(session)
    if not pages:
        raise HTTPException(status_code=404, detail="Results not found")
    return JSONResponse(content=pages)


### Category A

@app.get("/domains")
async def get_domains():
    query = f"SELECT DISTINCT domain FROM pages;"
    result_query = session.execute(query)
    if not result_query:
        raise HTTPException(status_code=404, detail="Results not found")
    result_query = list(result_query)
    
    if len(result_query) != 0:
        domains = [item[0] for item in result_query]

        output = {"Domains": domains}
        return JSONResponse(content=output)
    else:
        raise HTTPException(status_code=404, detail="Not Found")
    

@app.get("/users/{user_id}/pages")
async def get_pages_by_user(user_id: str):
    query = f"SELECT page_id, page_title FROM pages WHERE user_id = '{user_id}';"
    result_query = session.execute(query)
    if not result_query:
        raise HTTPException(status_code=404, detail="Results not found")
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
    if not result_query:
        raise HTTPException(status_code=404, detail="Results not found")
    result_query = list(result_query)
    
    if len(result_query) != 0:
        result_query = {"Number of pages created for specified domain": result_query[0][0]}
        return JSONResponse(content=result_query)
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified domain")


@app.get("/pages/{page_id}")
async def get_page_by_id(page_id: str):
    query = f"SELECT page_title FROM pages WHERE page_id = '{page_id}';"
    result_query = session.execute(query)
    if not result_query:
        raise HTTPException(status_code=404, detail="Results not found")
    result_query = list(result_query)

    if len(result_query) != 0:
        result_query = {"page_id": page_id, "page_title": result_query[0][0]}
        return JSONResponse(content=result_query)
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified page_id")

@app.get("/pages")
async def get_page_user_info(start_date: datetime, end_date:datetime):
    query = f"SELECT user_id, page_id, page_title FROM pages WHERE created_at >= '{start_date}' AND created_at <= '{end_date}' ALLOW FILTERING"
    result_query = session.execute(query)
    if not result_query:
        raise HTTPException(status_code=404, detail="Results not found")
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
        return JSONResponse(content=users_json)
    else:
        raise HTTPException(status_code=404, detail="Pages Not Found for a specified page_id")

    

