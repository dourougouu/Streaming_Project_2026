# Streaming_Project_2026
<h1><b>Επεξεργασία Δεδομένων Ροής(Data Streaming) με Kafka, Spark και MongoDB</b></h1>
<h2><b> Περιγραφή Project </b></h2>
Αυτό το project υλοποιήθηκε στο πλαίσιο του μαθήματος <b>Συστήματα Διαχείρισης Μεγάλων Δεδομένων (CEID_NE4348)</b> του Τμήματος Μηχανικών Η/Υ και Πληροφορικής (Πανεπιστήμιο Πατρών).

Στόχος είναι η δημιουργία μιας pipeline επεξεργασίας δεδομένων σε πραγματικό χρόνο:
1. <b>Παραγωγή δεδομένων:</b> Εξομοίωση κίνησης οχημάτων με τον UXSIM.
2. <b>Message Broker:</b> Αποστολή δεδομένων στο Redpanda (Kafka-compatible).
3. <b>Stream Processing:</b> Κατανάλωση και επεξεργασία των δεδομένων με Spark Structured Streaming.
4. <b>Storage:</b> Αποθήκευση raw και επεξεργασμένων στατιστικών (vcount, vspeed) στη MongoDB.

<h2><b> Στοιχεία Ομάδας </b></h2>
    <b>Μέλος 1:</b>   Δρούγκα Μαρία - 1100545
    <b>Μέλος 2:</b>   Θεοδοσόπουλος Χριστόφορος - 1100557
    <b>Εξάμηνο:</b>   Εαρινό 2026[cite: 1]
    <b>Υπεύθυνοι Καθηγητές:</b>   Β. Μεγαλοοικονόμου, Ε. Ζαχαράκη

<h2><b> Αρχιτεκτονική Συστήματος </b></h2>
Η ροή των δεδομένων ακολουθεί το παρακάτω σχήμα:
`UXSIM (Producer) -> Redpanda (Broker) -> Spark Structured Streaming -> MongoDB (Storage)`[cite: 1]

<h2><b> Τεχνολογίες & Εργαλεία </b></h2>
* **Γλώσσα:** Python 3.x[cite: 1]
* **Broker:** Redpanda Local (localhost:9092)[cite: 1]
* **Processing:** Apache Spark & PySpark[cite: 1]
* **Database:** MongoDB Community Edition[cite: 1]
* **Βιβλιοθήκες:** `pandas`, `kafka-python`, `pyspark`, `pymongo`[cite: 1]

<h2><b> Οδηγίες Εγκατάστασης & Εκτέλεσης </b></h2>

<b> 1. Προετοιμασία Περιβάλλοντος </b>
Εγκαταστήστε τις απαραίτητες βιβλιοθήκες:
pip install pandas kafka-python pyspark pymongo
