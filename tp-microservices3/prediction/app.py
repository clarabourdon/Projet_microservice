import os
import json
import time
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import RandomForestClassifier

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-prediction"

# Chemin du modèle pour créer modele_commande.pkl
chemin_modele = os.path.join(os.path.dirname(__file__), "modele_commande.pkl")

# Si le fichier n'existe pas encore on l'entraîne une première fois automatiquement
if not os.path.exists(chemin_modele):
    print("Fichier 'modele_commande.pkl' introuvable => entraînement initial")
    donnees_entrainement = pd.DataFrame([
        {"age": 20, "anciennete": 1, "nombre_commandes": 1, "montant_total": 50, "montant_moyen": 50.0, "jours_depuis_derniere_commande": 60, "achete": 0},
        {"age": 32, "anciennete": 3, "nombre_commandes": 8, "montant_total": 1250, "montant_moyen": 156.25, "jours_depuis_derniere_commande": 12, "achete": 1},
        {"age": 45, "anciennete": 5, "nombre_commandes": 15, "montant_total": 3000, "montant_moyen": 200.0, "jours_depuis_derniere_commande": 5, "achete": 1},
        {"age": 22, "anciennete": 1, "nombre_commandes": 2, "montant_total": 80, "montant_moyen": 40.0, "jours_depuis_derniere_commande": 45, "achete": 0},
        {"age": 28, "anciennete": 2, "nombre_commandes": 6, "montant_total": 800, "montant_moyen": 133.3, "jours_depuis_derniere_commande": 10, "achete": 1},
        {"age": 55, "anciennete": 1, "nombre_commandes": 1, "montant_total": 30, "montant_moyen": 30.0, "jours_depuis_derniere_commande": 90, "achete": 0}
    ])
    colonnes = ["age", "anciennete", "nombre_commandes", "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]
    clf = RandomForestClassifier(random_state=42)
    clf.fit(donnees_entrainement[colonnes], donnees_entrainement["achete"])
    joblib.dump(clf, chemin_modele)
    print("Modèle généré et sauvegardé dans 'modele_commande.pkl'")

# Chargement du modèle pré entraîné
modele = joblib.load(chemin_modele)
print("Modèle de Machine Learning chargé avec succès !")

# Initialisation du Producteur pour renvoyer les résultats
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"[PREDICTION] Producer connecté à {KAFKA_BROKER}")
        break
    except Exception as e:
        print(f"[PREDICTION] Attente Kafka Producer ({i+1}/10)...")
        time.sleep(2)

# Consommateur pour recevoir les demandes
consumer = None
for i in range(10):
    try:
        consumer = KafkaConsumer(
            TOPIC_DEMANDES,
            bootstrap_servers=KAFKA_BROKER,
            group_id="prediction-group",
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        print(f"[PREDICTION] Consumer à l'écoute de '{TOPIC_DEMANDES}'")
        break
    except Exception as e:
        print(f"[PREDICTION] Attente Kafka Consumer ({i+1}/10)...")
        time.sleep(2)

# Boucle de traitement des événements
print("[PREDICTION] Prêt à traiter les demandes...")
for message in consumer:
    data = message.value
    print(f"\n[DEMANDE REÇUE] Client ID : {data.get('client_id')}")

    # CTransformation des caractéristiques en Dataframe avec pandas
    X = pd.DataFrame([{
        "age": data["age"],
        "anciennete": data["anciennete"],
        "nombre_commandes": data["nombre_commandes"],
        "montant_total": data["montant_total"],
        "montant_moyen": data["montant_moyen"],
        "jours_depuis_derniere_commande": data["jours_depuis_derniere_commande"]
    }])

    # Prédiction et probabilité
    pred = int(modele.predict(X)[0])
    proba = float(modele.predict_proba(X)[0][1])

    print(f"Client {data['client_id']} | Prédiction: {pred} | Probabilité: {proba:.2%}")

    # Préparer et émettre l'événement résultat pour commandes
    resultat = {
        "client_id": data["client_id"],
        "prediction": pred,
        "probabilite_commande": round(proba, 4)
    }

    if producer:
        producer.send(TOPIC_RESULTATS, value=resultat)
        producer.flush()
        print(f" Résultat publié sur '{TOPIC_RESULTATS}' : {resultat}")