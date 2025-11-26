import os
from flask import Flask, request, jsonify, send_from_directory
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

load_dotenv()

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")                              # Postgresql password need to be set in the .env file
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB_NAME = os.getenv("PG_DB_NAME", "store_ecommerce")             # SQL done with PostgreSQL

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "storeEcommerce")        # NoSQL done with MongoDB

# 5 Tables/collections per database with 3 similar and 2 unique tables/collections
SQL_TABLES = {"customer", "product", "order_header", "order_item", "employee"}
MONGO_COLLECTIONS = {"customer", "product", "order_header", "review", "shipment"}

app = Flask(__name__, static_folder="frontend", static_url_path="")

# Connects to PostgreSQL if password is given and correct
def connectPg():
    if not PG_PASSWORD:
        raise RuntimeError("PG_PASSWORD missing in .env")
    return psycopg2.connect(
        dbname=PG_DB_NAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

# Connects to Mongo database
def connectMongo():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    return client[MONGO_DB_NAME]

# Fetches all rows with SELECT and returns them as dicts
def pgFetchAll(sql, params=None):
    with connectPg() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()

# Runs a write query (insert/update/delete) and tells how many rows it changed
def pgExecute(sql, params=None):
    with connectPg() as conn, conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.rowcount

# Keeps only the allowed fields that actually have a value and returns it
def projectFields(data, allowed_keys):
    return {
        field: data[field]
        for field in allowed_keys
        if field in data and data[field] is not None and data[field] != ""
    }

# Builds an SQL UPDATE query and its parameters based on data
def buildSqlUpdate(table, key_col, key_val, data: dict):
    """Return (sql, params) for UPDATE table SET ... WHERE key_col=%s"""
    sets, vals = [], []
    for field, value in (data or {}).items():
        if field == key_col:
            continue
        sets.append(f'{field}=%s')
        vals.append(value)
    if not sets:
        return None, None
    vals.append(key_val)
    sql = f'UPDATE "{table}" SET {", ".join(sets)} WHERE {key_col}=%s'
    return sql, vals

# Decides whether the inserted value goes to sql (total is even) or nosql/mongo (total is odd)
def chooseRoute(key: str) -> str:
    total = sum(ord(char) for char in key.strip())
    return "sql" if total % 2 == 0 else "mongo"

# Combines products from SQL and Mongo into one list, where SQL “wins” if the same SKU exists in both
def unionProductsList():
    sql_products = pgFetchAll('SELECT sku, name, price FROM "product"')
    db = connectMongo()
    mongo_products = list(db.product.find({}, {"_id": 0, "sku": 1, "name": 1, "price": 1}))

    merged = {}
    for product in sql_products:        # Add all SQL products first
        sku = product.get("sku")
        if not sku:
            continue
        merged[sku] = {**product, "source": "sql"}

    for product in mongo_products:      # Add mongo products if SKU not already in merged
        sku = product.get("sku")
        if not sku or sku in merged:
            continue
        merged[sku] = {
            "sku": sku,
            "name": product.get("name"),
            "price": float(product.get("price", 0)),
            "source": "mongo",
        }
    return sorted(merged.values(), key=lambda p: p.get("sku") or "")

# Creates a dict that maps each customer's email to their name with SQL winning on conflicts
def customerNameLookup():
    db = connectMongo()
    sql = pgFetchAll('SELECT email, name FROM "customer"')
    mg = list(db.customer.find({}, {"_id": 0, "email": 1, "name": 1}))
    out = {}
    for r in mg:
        if r.get("email"):
            out[r["email"]] = r.get("name")
    for r in sql:
        if r.get("email"):
            out[r["email"]] = r.get("name")  # SQL wins
    return out

# List rows from a PostgreSQL table
@app.get("/api/pg/<table>")
def listPg(table):
    if table not in SQL_TABLES:
        return jsonify({"ok": False, "error": "unknown table"}), 400
    rows = pgFetchAll(f'SELECT * FROM "{table}" ORDER BY 1 LIMIT 200')
    return jsonify(rows)

# List documents from a Mongo collection
@app.get("/api/mongo/<collection>")
def listMongo(collection):
    if collection not in MONGO_COLLECTIONS:
        return jsonify({"ok": False, "error": "unknown collection"}), 400
    db = connectMongo()
    docs = list(db[collection].find({}, {"_id": 0}).limit(400))
    return jsonify(docs)

# Union of products (SQL wins on conflicts)
@app.get("/api/product")
def unionProducts():
    return jsonify(unionProductsList())

# Union of customers (SQL wins on conflicts)
@app.get("/api/union/customer")
def unionCustomer():
    sql_rows = pgFetchAll('SELECT email, name, username, phone, address, country, customer_id FROM "customer"')
    db = connectMongo()
    mg_rows = list(db.customer.find(
        {}, {"_id": 0, "email": 1, "name": 1, "username": 1, "phone": 1, "address": 1, "country": 1, "customer_id": 1}))

    merged = {r["email"]: {**r, "source": "mongo"} for r in mg_rows if r.get("email")}
    for r in sql_rows:
        merged[r["email"]] = {**r, "source": "sql"}  # SQL wins

    def cid_key(row): # Helper for sorting customer ids
        try:
            return (0, int(row.get("customer_id", 10**12)), row.get("email", ""))
        except (TypeError, ValueError):
            return (1, 10**12, row.get("email", ""))

    out = list(merged.values())
    out.sort(key=cid_key)
    return jsonify(out)

# Union of order headers (SQL wins on conflicts)
@app.get("/api/union/order_header")
def unionOrderHeaders():
    sql_orders = pgFetchAll(
        'SELECT order_id, customer_email, created_at, status, total FROM "order_header"'
    )
    db = connectMongo()
    mongo_orders = list(
        db["order_header"].find(
            {},
            {"_id": 0,"orderId": 1,"customerEmail": 1,"createdAt": 1,"status": 1,"total": 1,},))

    merged = {}
    for order in mongo_orders:
        order_id = order.get("orderId")
        if order_id is not None:
            merged[order_id] = {
                "order_id": order_id,
                "customer_email": order.get("customerEmail"),
                "created_at": order.get("createdAt"),
                "status": order.get("status"),
                "total": order.get("total"),
                "source": "mongo",}
            
    for order in sql_orders:
        order_id = order.get("order_id")
        if order_id is not None:
            merged[order_id] = {**order, "source": "sql"}  # SQL wins

    out = list(merged.values())
    out.sort(key=lambda row: row.get("order_id") or 0)
    return jsonify(out)

# Join order headers (both DBs) with customers (name using the lookup)
@app.get("/api/orderJoined")
def orderJoined():
    pgOrders = pgFetchAll('SELECT order_id, customer_email, created_at, status, total FROM "order_header" ORDER BY order_id')
    db = connectMongo()
    mgOrders = list(db["order_header"].find({}, {"_id": 0, "orderId": 1, "customerEmail": 1, "createdAt": 1, "status": 1, "total": 1}))
    cname = customerNameLookup()

    def enrichSql(o):
        email = o.get("customer_email")
        return {**o, "customer_name": cname.get(email) or "(unknown)", "source": "sql"}

    def enrichMongo(o):
        email = o.get("customerEmail")
        row = {
            "order_id": o.get("orderId"),
            "customer_email": email,
            "created_at": o.get("createdAt"),
            "status": o.get("status"),
            "total": o.get("total"),
        }
        return {**row, "customer_name": cname.get(email) or "(unknown)", "source": "mongo"}

    out = [enrichSql(o) for o in pgOrders] + [enrichMongo(o) for o in mgOrders]
    out.sort(key=lambda x: x.get("order_id") or 0)
    return jsonify(out)

# Join unified products with Mongo reviews
@app.get("/api/join/product-review")
def joinProductReview():
    products = {p["sku"]: p for p in unionProductsList()}
    db = connectMongo()
    reviews = list(db.review.find({}, {"_id": 0, "reviewId": 1, "sku": 1, "email": 1, "rating": 1, "comment": 1}))
    out = []
    for rv in reviews:
        sku = rv.get("sku")
        p = products.get(sku, {})
        out.append({
            "sku": sku,
            "productName": p.get("name"),
            "price": p.get("price"),
            "reviewId": rv.get("reviewId"),
            "email": rv.get("email"),
            "rating": rv.get("rating"),
            "comment": rv.get("comment"),
            "source": "mongo"
        })
    out.sort(key=lambda x: (x.get("sku") or "", x.get("reviewId") or ""))
    return jsonify(out)

# Join order_item from SQL with product from Mongo via union
@app.get("/api/join/product-items")
def joinOrderItemProduct():
    items = pgFetchAll(
        'SELECT order_item_id, order_id, sku, qty, unit_price, discount FROM "order_item" ORDER BY order_item_id'
    )
    products = {p["sku"]: p for p in unionProductsList()}  # SQL-preferred union
    out = []
    for it in items:
        p = products.get(it.get("sku"), {})
        out.append({
            **it,
            "product_name": p.get("name"),
            "product_price": p.get("price"),
            "product_source": p.get("source"),
        })
    return jsonify(out)

# Join review from Mongo with customer from SQL
@app.get("/api/join/customer-reviews")
def joinReviewCustomer():
    db = connectMongo()
    reviews = list(
        db.review.find({},{"_id": 0, "reviewId": 1, "sku": 1, "email": 1, "rating": 1, "comment": 1},))
    sql_customers = pgFetchAll('SELECT email, name, customer_id, country FROM "customer"')
    mongo_customers = list(db.customer.find({}, {"_id": 0, "email": 1, "name": 1, "customer_id": 1, "country": 1}))

    customers = {}
    for customer in mongo_customers:
        email = customer.get("email")
        if email:
            customers[email] = {
                "customer_name": customer.get("name"),
                "customer_id": customer.get("customer_id"),
                "country": customer.get("country"),}
            
    for customer in sql_customers:
        email = customer.get("email")
        if email:
            customers[email] = {
                "customer_name": customer.get("name"),
                "customer_id": customer.get("customer_id"),
                "country": customer.get("country"),}
    merged = [
        {**review, **customers.get(review.get("email"), {})}
        for review in reviews
    ]
    merged.sort(key=lambda row: row.get("reviewId") or "")
    return jsonify(merged)

# Join order_header from SQL with shipment from Mongo
@app.get("/api/join/orders-shipment")
def joinOrderShipment():
    db = connectMongo()
    # Union of order headers (SQL wins on conflicts)
    sql_orders = pgFetchAll('SELECT order_id, customer_email, created_at, status, total FROM "order_header"')
    mongo_orders = list(db["order_header"].find({},{"_id": 0,"orderId": 1,"customerEmail": 1,"createdAt": 1,"status": 1,"total": 1,},))

    orders = {}
    for order in mongo_orders:
        order_id = order.get("orderId")
        if order_id is None:
            continue
        orders[order_id] = {"order_id": order_id,
            "customer_email": order.get("customerEmail"),
            "created_at": order.get("createdAt"),
            "order_status": order.get("status"),
            "total": order.get("total"),}
        
    for order in sql_orders:
        order_id = order.get("order_id")
        if order_id is None:
            continue
        orders[order_id] = {
            **order,
            "order_status": order.get("status"),
            "order_source": "sql",}

    shipments = list(db.shipment.find({},{"_id": 0,"shipmentId": 1,"orderId": 1,"status": 1,"carrier": 1,},)) # Shipments (Mongo)
    joined = []
    for shipment in shipments:
        order_id = shipment.get("orderId")
        order = orders.get(order_id, {})
        joined.append({
                "order_id": order_id,
                "customer_email": order.get("customer_email"),
                "created_at": order.get("created_at"),
                "order_status": order.get("order_status"),
                "total": order.get("total"),
                "shipmentId": shipment.get("shipmentId"),
                "shipment_status": shipment.get("status"),
                "carrier": shipment.get("carrier"),})
        
    joined.sort(key=lambda row: (row.get("order_id") or 0, row.get("shipmentId") or ""))
    return jsonify(joined)

# Create or update a product without revealing the DB
@app.post("/api/product")
def createProduct():
    data = request.get_json(force=True)
    sku   = (data.get("sku") or "").strip()
    name  = (data.get("name") or "").strip()
    price = data.get("price")
    if not sku or not name or price is None:
        return jsonify({"ok": False, "error": "sku, name, price required"}), 400

    if pgFetchAll('SELECT 1 FROM "product" WHERE sku=%s', [sku]):
        pgExecute('UPDATE "product" SET name=%s, price=%s WHERE sku=%s', [name, price, sku])
        return jsonify({"ok": True, "routedTo": "sql", "mode": "update"})

    db = connectMongo()
    if db.product.count_documents({"sku": sku}, limit=1):
        db.product.update_one({"sku": sku}, {"$set": {"name": name, "price": float(price)}}, upsert=True)
        return jsonify({"ok": True, "routedTo": "mongo", "mode": "update"})

    if chooseRoute(sku) == "sql":
        pgExecute('INSERT INTO "product"(sku, name, price) VALUES (%s,%s,%s)', [sku, name, price])
        return jsonify({"ok": True, "routedTo": "sql", "mode": "insert"})
    else:
        db.product.insert_one({"sku": sku, "name": name, "price": float(price)})
        return jsonify({"ok": True, "routedTo": "mongo", "mode": "insert"})

# Update a product by SKU (tries SQL, then Mongo)
@app.put("/api/product/<sku>")
def updateProduct(sku):
    sku = sku.strip()
    data = request.get_json(force=True)

    if pgFetchAll('SELECT sku FROM "product" WHERE sku=%s', [sku]):
        sql, params = buildSqlUpdate("product", "sku", sku, projectFields(data, {"name","price"}))
        if not sql:
            return jsonify({"ok": False, "error": "nothing to update"}), 400
        pgExecute(sql, params)
        return jsonify({"ok": True, "source": "sql"})

    db = connectMongo()
    upd = projectFields(data, {"name","price"})
    if "price" in upd:
        upd["price"] = float(upd["price"])
    if not upd:
        return jsonify({"ok": False, "error": "nothing to update"}), 400
    res = db.product.update_one({"sku": sku}, {"$set": upd})
    if res.matched_count == 0:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "source": "mongo"})

# Delete a product by SKU (whichever DB has it)
@app.delete("/api/product/<sku>")
def deleteProduct(sku):
    sku = sku.strip()
    try:
        if pgExecute('DELETE FROM "product" WHERE sku=%s', [sku]):
            return jsonify({"ok": True, "source": "sql"})
    except Exception:
        pass
    db = connectMongo()
    if db.product.delete_one({"sku": sku}).deleted_count:
        return jsonify({"ok": True, "source": "mongo"})
    return jsonify({"ok": False, "error": "not found"}), 404

# Create or update a customer without revealing the DB
@app.post("/api/customer")
def createCustomer():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip()
    name  = (data.get("name") or "").strip()
    if not email or not name:
        return jsonify({"ok": False, "error": "email and name are required"}), 400

    allowed = {"username","phone","address","country","customer_id"}
    payload = {"email": email, "name": name, **projectFields(data, allowed)}

    if pgFetchAll('SELECT 1 FROM "customer" WHERE email=%s', [email]):
        sql, params = buildSqlUpdate("customer", "email", email, payload)
        if sql:
            pgExecute(sql, params)
        return jsonify({"ok": True, "routedTo": "sql", "mode": "update"})

    db = connectMongo()
    if db.customer.count_documents({"email": email}, limit=1):
        upd = {k:v for k,v in payload.items() if k != "email"}
        db.customer.update_one({"email": email}, {"$set": upd}, upsert=True)
        return jsonify({"ok": True, "routedTo": "mongo", "mode": "update"})

    if chooseRoute(email) == "sql":
        cols = list(payload.keys())
        vals = [payload[c] for c in cols]
        pgExecute(f'INSERT INTO "customer"({", ".join(cols)}) VALUES ({", ".join(["%s"]*len(cols))})', vals)
        return jsonify({"ok": True, "routedTo": "sql", "mode": "insert"})
    else:
        db.customer.insert_one(payload)
        return jsonify({"ok": True, "routedTo": "mongo", "mode": "insert"})

# Update a customer by email (tries SQL, then Mongo)
@app.put("/api/customer/<email>")
def updateCustomer(email):
    email = email.strip()
    data = request.get_json(force=True)
    allowed = {"name","username","phone","address","country","customer_id"}

    if pgFetchAll('SELECT 1 FROM "customer" WHERE email=%s', [email]):
        sql, params = buildSqlUpdate("customer", "email", email, projectFields(data, allowed))
        if not sql:
            return jsonify({"ok": False, "error": "nothing to update"}), 400
        pgExecute(sql, params)
        return jsonify({"ok": True, "source": "sql"})

    db = connectMongo()
    upd = projectFields(data, allowed)
    if not upd:
        return jsonify({"ok": False, "error": "nothing to update"}), 400
    res = db.customer.update_one({"email": email}, {"$set": upd})
    if res.matched_count == 0:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "source": "mongo"})

# Delete a customer by email (whichever DB has it)
@app.delete("/api/customer/<email>")
def deleteCustomer(email):
    email = email.strip()
    try:
        if pgExecute('DELETE FROM "customer" WHERE email=%s', [email]):
            return jsonify({"ok": True, "source": "sql"})
    except Exception:
        pass
    db = connectMongo()
    if db.customer.delete_one({"email": email}).deleted_count:
        return jsonify({"ok": True, "source": "mongo"})
    return jsonify({"ok": False, "error": "not found"}), 404

# Serves the main frontend page
@app.get("/")
def indexPage():
    return send_from_directory(app.static_folder, "index.html")

# Dev server entrypoint
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)