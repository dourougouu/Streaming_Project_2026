"""
BONUS: REST API για ανάγνωση στατιστικών από MongoDB
Εκτέλεση: python api.py
Endpoints:
  GET /stats                  → Τελευταία στατιστικά ανά link (από traffic.stats)
  GET /stats/<link>           → Ιστορικό για συγκεκριμένη ακμή
  GET /windowed               → Windowed aggregations (traffic.windowed_stats)
  GET /raw?limit=N            → Raw δεδομένα (traffic.raw_data), default limit=100
  GET /summary                → Συγκεντρωτικά (μέση ταχύτητα ανά link σε όλο το διάστημα)
"""

from flask import Flask, jsonify, request, abort
from pymongo import MongoClient, DESCENDING
from bson import ObjectId
import json
from flask_cors import CORS
 
app = Flask(__name__)
CORS(app)  #Για να συνδέσω το dashboard απευθείας από το browser στο Flask
#Για να δούμε το dashboard πηγαινουμε στο αρχείο traffic_dashboard_visualization.html
 
# ============================================================
# Σύνδεση MongoDB
# ============================================================
MONGO_URI = "mongodb://localhost:27017"   # Αλλάξτε σε mongo:27017 αν τρέχετε μέσα στο Docker network
client = MongoClient(MONGO_URI)
db = client["traffic"]
 
 
# ============================================================
# Helper: Μετατροπή MongoDB documents σε JSON-serializable
# ============================================================
def serialize_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc
 
 
# ============================================================
# GET /stats
# Τελευταία εγγραφή ανά link (η πιο πρόσφατη χρονικά)
# ============================================================
@app.route("/stats", methods=["GET"])
def get_latest_stats():
    pipeline = [
        {"$sort": {"time": DESCENDING}},
        {
            "$group": {
                "_id": "$link",
                "latest_time": {"$first": "$time"},
                "vcount":      {"$first": "$vcount"},
                "vspeed":      {"$first": "$vspeed"},
                "max_speed":   {"$first": "$max_speed"},
                "min_speed":   {"$first": "$min_speed"},
                "avg_spacing": {"$first": "$average_spacing"},
            }
        },
        {"$sort": {"_id": 1}}
    ]
    results = list(db["stats"].aggregate(pipeline))
    for r in results:
        r["link"] = r.pop("_id")
    return jsonify(results)
 
 
# ============================================================
# GET /stats/<link>
# Ιστορικό για συγκεκριμένη ακμή, ταξινομημένο κατά χρόνο
# ============================================================
@app.route("/stats/<string:link>", methods=["GET"])
def get_stats_for_link(link):
    limit = request.args.get("limit", default=200, type=int)
    docs = list(
        db["stats"]
        .find({"link": link}, {"_id": 0})
        .sort("time", 1)
        .limit(limit)
    )
    if not docs:
        abort(404, description=f"Link '{link}' not found in stats collection.")
    return jsonify(docs)
 
 
# ============================================================
# GET /windowed
# Windowed aggregations (30s παράθυρα) — BONUS
# Query params: link (optional), limit (default 100)
# ============================================================
@app.route("/windowed", methods=["GET"])
def get_windowed_stats():
    link_filter = request.args.get("link")
    limit = request.args.get("limit", default=100, type=int)
 
    query = {}
    if link_filter:
        query["link"] = link_filter
 
    docs = list(
        db["windowed_stats"]
        .find(query, {"_id": 0})
        .sort("window_start", DESCENDING)
        .limit(limit)
    )
    return jsonify(docs)
 
 
# ============================================================
# GET /raw
# Raw vehicle records — με pagination μέσω limit & skip
# ============================================================
@app.route("/raw", methods=["GET"])
def get_raw_data():
    limit = request.args.get("limit", default=100, type=int)
    skip  = request.args.get("skip",  default=0,   type=int)
    link  = request.args.get("link")
 
    query = {}
    if link:
        query["link"] = link
 
    docs = list(
        db["raw_data"]
        .find(query, {"_id": 0})
        .sort("time", 1)
        .skip(skip)
        .limit(limit)
    )
    return jsonify(docs)
 
 
# ============================================================
# GET /summary
# Συγκεντρωτικά: μέση ταχύτητα, max/min, συνολικά οχήματα
# ανά link σε ΟΛΟ το διάστημα της προσομοίωσης
# ============================================================
@app.route("/summary", methods=["GET"])
def get_summary():
    pipeline = [
        {
            "$group": {
                "_id": "$link",
                "overall_avg_speed":  {"$avg": "$vspeed"},
                "overall_max_speed":  {"$max": "$max_speed"},
                "overall_min_speed":  {"$min": "$min_speed"},
                "total_vehicle_obs":  {"$sum": "$vcount"},
                "num_timesteps":      {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "link":               "$_id",
                "_id":                0,
                "overall_avg_speed":  {"$round": ["$overall_avg_speed", 2]},
                "overall_max_speed":  {"$round": ["$overall_max_speed", 2]},
                "overall_min_speed":  {"$round": ["$overall_min_speed", 2]},
                "total_vehicle_obs":  1,
                "num_timesteps":      1,
            }
        }
    ]
    results = list(db["stats"].aggregate(pipeline))
    return jsonify(results)
 
 
# ============================================================
# GET /health
# Έλεγχος σύνδεσης MongoDB
# ============================================================
@app.route("/health", methods=["GET"])
def health():
    try:
        client.admin.command("ping")
        stats_count   = db["stats"].estimated_document_count()
        raw_count     = db["raw_data"].estimated_document_count()
        windowed_count = db["windowed_stats"].estimated_document_count()
        return jsonify({
            "status": "ok",
            "mongodb": "connected",
            "collections": {
                "stats":           stats_count,
                "raw_data":        raw_count,
                "windowed_stats":  windowed_count,
            }
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
 
 
# ============================================================
# Error handlers
# ============================================================
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": str(e)}), 404
 
@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": str(e)}), 500
 
 
if __name__ == "__main__":
    print("Starting REST API on http://0.0.0.0:5000")
    print("Endpoints:")
    print("  GET /health")
    print("  GET /stats")
    print("  GET /stats/<link>")
    print("  GET /windowed?link=<link>&limit=100")
    print("  GET /raw?limit=100&skip=0&link=<link>")
    print("  GET /summary")
    app.run(host="0.0.0.0", port=5000, debug=False)