# Streaming_Project_2026
<h2><b>Επεξεργασία Δεδομένων Ροής(Data Streaming) με Kafka, Spark και MongoDB</b></h2>
## Περιγραφή Project
Αυτό το project υλοποιήθηκε στο πλαίσιο του μαθήματος **Συστήματα Διαχείρισης Μεγάλων Δεδομένων (CEID_NE4348)** του Τμήματος Μηχανικών Η/Υ και Πληροφορικής (Πανεπιστήμιο Πατρών)[cite: 1].

Στόχος είναι η δημιουργία μιας pipeline επεξεργασίας δεδομένων σε πραγματικό χρόνο:
1. **Παραγωγή δεδομένων:** Εξομοίωση κίνησης οχημάτων με τον **UXSIM**[cite: 1].
2. **Message Broker:** Αποστολή δεδομένων στο **Redpanda** (Kafka-compatible)[cite: 1].
3. **Stream Processing:** Κατανάλωση και επεξεργασία των δεδομένων με **Spark Structured Streaming**[cite: 1].
4. **Storage:** Αποθήκευση raw και επεξεργασμένων στατιστικών (vcount, vspeed) στη **MongoDB**[cite: 1].

## Στοιχεία Ομάδας
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
