import os
import json
import time
import threading
from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
# Topic 1 : Commandes dépose les données du client
TOPIC_DEMANDES = "demandes-prediction"
# Topic 2 : Commandes récupère les probabilités calculées
TOPIC_RESULTATS = "resultats-prediction"

producer = None

# Fonction de connexion avec le broker Kafka
def get_kafka_producer():
    global producer
    if producer is not None:
        return producer
    # On a des tentatives successives car démarrage Kafka peut parfois être prêt après le lancement Docker    
    for tentative in range(10):
        try:
            # KafkaProducer prend en charge l'envoi de messages vers Kafka
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # fonction lambda convertit dictionnaire Python en chaîne JSON avant envoi réseau
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"[COMMANDES] Producer connecté sur {KAFKA_BROKER}")
            return producer
        except Exception:
            time.sleep(2)
    return None

# Thread d'écoute qui eçoit la réponse du modèle ML envoyée par prediction
def demarrer_ecoute_resultats():
    consumer = None
    # Plusieurs tentatives (voir plus haut)
    for _ in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_RESULTATS,
                bootstrap_servers=KAFKA_BROKER,
                group_id="commandes-group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"[COMMANDES] Écoute des réponses sur '{TOPIC_RESULTATS}'")
            break
        except Exception:
            time.sleep(2)

    if not consumer:
        return

    for message in consumer:
        res = message.value
        print("\n========================================")
        print(f"[RÉSULTAT ML REÇU PAR COMMANDES]")
        print(f"• Client ID   : {res.get('client_id')}")
        print(f"• Prédiction  : {res.get('prediction')} (1 = commande, 0 = non)")
        print(f"• Probabilité : {res.get('probabilite_commande') * 100:.2f} %")
        print("========================================\n")

@app.route('/predict', methods=['POST'])
# Reçoit les 6 métriques client en HTTP et les publie dans Kafka
def demander_prediction():
    data = request.get_json()
    # Les 6 métriques
    champs = ["client_id", "age", "anciennete", "nombre_commandes", "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]
    # Gestion erreur
    if not data or not all(k in data for k in champs):
        return jsonify({"erreur": f"Champs obligatoires : {', '.join(champs)}"}), 400

    # Envoi du message JSON dans le topic Kafka
    prod = get_kafka_producer()
    if prod:
        prod.send(TOPIC_DEMANDES, value=data)
        prod.flush()
        print(f"[COMMANDES] Demande publiée dans Kafka pour le client {data['client_id']}")
        # On répond direct au client HTTP avec message de création
        return jsonify({"message": "Demande transmise à Kafka", "donnees": data}), 202

    return jsonify({"erreur": "Kafka indisponible"}), 503

if __name__ == '__main__':
    # Initialisation au démarrage
    get_kafka_producer()
    t = threading.Thread(target=demarrer_ecoute_resultats, daemon=True)
    t.start()
    # Le microservice commandes écoute sur le port 8081
    app.run(host='0.0.0.0', port=8081)