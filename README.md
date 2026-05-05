# Streaming_Project_2026
<h2><b>Επεξεργασία Δεδομένων Ροής(Data Streaming) με Kafka, Spark και MongoDB</b></h2>
<h2> Περιγραφή Project </h2>
Αυτό το project υλοποιήθηκε στο πλαίσιο του μαθήματος <b>Συστήματα Διαχείρισης Μεγάλων Δεδομένων (CEID_NE4348)</b> του Τμήματος Μηχανικών Η/Υ και Πληροφορικής (Πανεπιστήμιο Πατρών).

Στόχος είναι η δημιουργία μιας pipeline επεξεργασίας δεδομένων σε πραγματικό χρόνο:
1. <b>Παραγωγή δεδομένων:</b> Εξομοίωση κίνησης οχημάτων με τον UXSIM.
2. <b>Message Broker:</b> Αποστολή δεδομένων στο Redpanda (Kafka-compatible).
3. <b>Stream Processing:</b> Κατανάλωση και επεξεργασία των δεδομένων με Spark Structured Streaming.
4. <b>Storage:</b> Αποθήκευση raw και επεξεργασμένων στατιστικών (vcount, vspeed) στη MongoDB.

<h2> Στοιχεία Ομάδας </h2>
* **Μέλος 1:** Δρούγκα Μαρία - 1100545
* **Μέλος 2:** Θεοδοσόπουλος Χριστόφορος - 1100557
* **Εξάμηνο:** Εαρινό 2026[cite: 1]
* **Υπεύθυνοι Καθηγητές:** Β. Μεγαλοοικονόμου, Ε. Ζαχαράκη[cite: 1]

## Αρχιτεκτονική Συστήματος
Η ροή των δεδομένων ακολουθεί το παρακάτω σχήμα:
`UXSIM (Producer) -> Redpanda (Broker) -> Spark Structured Streaming -> MongoDB (Storage)`[cite: 1]

## Τεχνολογίες & Εργαλεία
* **Γλώσσα:** Python 3.x[cite: 1]
* **Broker:** Redpanda Local (localhost:9092)[cite: 1]
* **Processing:** Apache Spark & PySpark[cite: 1]
* **Database:** MongoDB Community Edition[cite: 1]
* **Βιβλιοθήκες:** `pandas`, `kafka-python`, `pyspark`, `pymongo`[cite: 1]

## Οδηγίες Εγκατάστασης & Εκτέλεσης

### 1. Προετοιμασία Περιβάλλοντος
Εγκαταστήστε τις απαραίτητες βιβλιοθήκες:
```bash
pip install pandas kafka-python pyspark pymongo
