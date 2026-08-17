from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Μπορειτε να χρησιμοποιειτε παρενθέσεις ( ... ) γύρω από μπλοκ κώδικα για να επιτρέψετε αλλαγή γραμμής (multi-line statement) χωρίς να χρειάζεται να χρησιμοποιείτε τον χαρακτήρα backslash \


# 1. Ορισμός του Schema
## vehicles_to_pandas returns a pd.DataFrame:
#             A DataFrame containing the travel logs of vehicles, with the columns:
#
#             - 'name': the name of the vehicle (platoon).
#             - 'dn': the platoon size.
#             - 'orig': the origin node of the vehicle's trip.
#             - 'dest': the destination node of the vehicle's trip.
#             - 't': the timestep.
#             - 'link': the link the vehicle is on (or relevant status).
#             - 'x': the position of the vehicle on the link.
#             - 's': the spacing of the vehicle.
#             - 'v': the speed of the vehicle.
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("time", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())


# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
# Μπορείτε να αντικαταστήσετε το 'uxsim' με όνομα του topic της επιλογής σας
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing του JSON και Μετασχηματισμός
# Χρησιμοποιείστε "cast" για να μετατρεψετε τις raw binary τιμες σε ευαναγνωστο JSON string
# Εφαρμοστε το schema στο string, δημιουργώντας ενα μοναδικό struct με την ονομασία data
# Καθε key το JSON (name, orig, dest, etc.) μετατρεπεται σε ξεχωριστη στήλη στο DataFrame.
# parsed = df.select ...
data = ( 
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    )
    #ή .select(col("data.name")) κλπ

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (time)
# time = time από την εξομοίωση, v = ταχύτητα οχήματος
# Στον παρακάτω κώδικα χρησιμοποιούμε το πεδίο time της UXsim ως "time". Αν η εξομοίωση στέλνει δεδομένα πολύ γρήγορα, το Spark θα τα ομαδοποιεί σωστά ανά simulation step.
# stats = parsed.  ....
stats = (data.groupBy("time", "link")
            .agg(
                count("name").alias("vcount"),
                avg("v").alias("vspeed"),
                avg("s").alias("average_spacing")
            )
        )
        


# 6α. Raw data στη MongoDB
# parsed.writeStream ...
def write_to_mongoDB(batch_df, batch_id):
    (batch_df.write  
    .format("mongodb")
    .mode("append")
    .option("database", "traffic")
    .option("collection", "raw_data")
    .save())

query_raw_data = (
    data.writeStream
    .foreachBatch(write_to_mongoDB)  #κάθε micro-batch, θα εκτελείται η παραπάνω συνάρτηση, και θα γράφεται σαν κανονικό DataFrame στη MongoDB. Το foreachBatch μας δίνει τον πλήρη έλεγχο για το πώς γράφουμε τα δεδομένα, και μας επιτρέπει να χρησιμοποιήσουμε τις κανονικές λειτουργίες DataFrame για να γράψουμε στη MongoDB.)
    .outputMode("append")
    .start()
)
    

# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
# Το outputMode("append") στο streaming απαιτεί τη χρήση Watermarks (χρονικά όρια) για να ξέρει το Spark πότε "έκλεισε" ένα group και μπορεί να το γράψει.
# Διαφορετικά, χρησιμοποιούμε το .outputMode("update"), το οποίο γράφει στη MongoDB μόνο τα links που άλλαξαν οι τιμές τους.
def write_stats_to_mongoDB(batch_df, batch_id):
    (batch_df.write  
    .format("mongodb")
    .mode("append")
    .option("database", "traffic")
    .option("collection", "stats")
    .save())

query_stats = (
    stats.writeStream
    .foreachBatch(write_stats_to_mongoDB)  #κάθε micro-batch, θα εκτελείται η παρακάτω συνάρτηση, και θα γράφεται σαν κανονικό DataFrame στη MongoDB. Το foreachBatch μας δίνει τον πλήρη έλεγχο για το πώς γράφουμε τα δεδομένα, και μας επιτρέπει να χρησιμοποιήσουμε τις κανονικές λειτουργίες DataFrame για να γράψουμε στη MongoDB.
    .outputMode("update") #γραφει μονο οσα αλλαζουν
    .start()
)

    

# 7. Console για debugging
query_console = (
    stats.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination() # query_mongo.awaitTermination()
