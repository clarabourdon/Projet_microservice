
import os
import json
import time
from datetime import datetime
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

# Adresse du broker Kafka, elle est configurable avec la variable d'environnement
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
# Nom du topic dans lequel on va publier tous les événements de commande
TOPIC_COMMANDES = "commandes-creees"

producer = None

# Fonction de connexion avec le broker Kafka
def get_kafka_producteur():
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
            print(f"Connecté à Kafka ({KAFKA_BROKER}) !")
            return producer
        except Exception as e:
            print(f"En attente de Kafka ({tentative + 1}/10)... Erreur : {e}")
            time.sleep(2)
    return None

# Données du TP en mémoire
commandes = [
    {"commande_id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"commande_id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"commande_id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

@app.route('/commandes', methods=['GET'])
def get_commandes():
    # Retourne la liste des commandes enregistrées 
    return jsonify(commandes), 200

# Endpoint pour créer une commande et publier l'événement dans Kafka => c'est ici qu'on introduit la communication asynchrone
@app.route('/commandes', methods=['POST'])
def create_commande():
    data = request.get_json()
    # Gestion erreur
    if not data or not all(k in data for k in ("client_id", "produit", "montant")):
        return jsonify({"erreur": "Champs requis : client_id, produit, montant"}), 400

    nouvelle_id = len(commandes) + 1
    # On ajoute la date du mois actuel pour pour le calcul CA par mois de la partie 1.7
    mois_actuel = datetime.now().strftime("%Y-%m")

    # Structure du message JSON de l'événement
    nouvel_evenement = {
        "commande_id": nouvelle_id,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        "mois": mois_actuel
    }

    # Sauvegarde locale
    commandes.append(nouvel_evenement)

    # Envoi du message JSON dans le topic Kafka
    prod = get_kafka_producteur()
    if prod:
        prod.send(TOPIC_COMMANDES, value=nouvel_evenement)
        prod.flush()
        print(f"[KAFKA] Événement publié sur {TOPIC_COMMANDES} : {nouvel_evenement}")
        # On répond direct au client HTTP avec message de création
        return jsonify({
            "message": "Commande créée et publiée dans Kafka",
            "commande": nouvel_evenement
        }), 201
    else:
        return jsonify({"erreur": "Impossible de joindre Kafka"}), 503

if __name__ == '__main__':
    # Initialisation au démarrage
    get_kafka_producteur()
    # Le microservice commandes écoute sur le port 8081
    app.run(host='0.0.0.0', port=8081)