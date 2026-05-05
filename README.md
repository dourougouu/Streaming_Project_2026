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
    <b>Μέλος 1:</b>   Δρούγκα Μαρία - 1100545 <br>
    <b>Μέλος 2:</b>   Θεοδοσόπουλος Χριστόφορος - 1100557 <br>
    <b>Εξάμηνο:</b>   Εαρινό 2026 <br>
    <b>Υπεύθυνοι Καθηγητές:</b>   Β. Μεγαλοοικονόμου, Ε. Ζαχαράκη  <br>

<h2><b> Αρχιτεκτονική Συστήματος </b></h2>
Η ροή των δεδομένων ακολουθεί το παρακάτω σχήμα:
`UXSIM (Producer) -> Redpanda (Broker) -> Spark Structured Streaming -> MongoDB (Storage)

<h2><b> Τεχνολογίες & Εργαλεία </b></h2>
    <b>Γλώσσα:</b> Python 3.x <br>
    <b>Broker:</b> Redpanda Local (localhost:9092) <br>
    <b>Processing:</b> Apache Spark & PySpark <br>
    <b>Database:</b> MongoDB Community Edition <br>
    <b>Βιβλιοθήκες:</b> `pandas`, `kafka-python`, `pyspark`, `pymongo` <br>

<h2><b> Οδηγίες Εγκατάστασης & Εκτέλεσης </b></h2>

<b> 1. Προετοιμασία Περιβάλλοντος </b>
Εγκαταστήστε τις απαραίτητες βιβλιοθήκες:
pip install pandas kafka-python pyspark pymongo
