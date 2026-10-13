import os
import json
import time
import threading
from collections import defaultdict
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

# Configuration Kafka
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_COMMANDES = "commandes-creees"

# Le répartoire des clients
clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

commandes_par_client = defaultdict(int)

# Fonction pour écouter Kafka en continu
def demarrer_consommateur_kafka():
    consumer = None
    # Boucle avec plusieuyrs tentatives en attendant lancement de Kafka
    for tentative in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_COMMANDES,
                bootstrap_servers=KAFKA_BROKER,
                group_id="clients-group", # identifiant unique de ce groupe de consommateurs
                auto_offset_reset="earliest", # verif si le service démarre pour la 1re fois ou après un crash
                enable_auto_commit=True, # Valide automatiquement auprès de Kafka les messages déjà lus
                value_deserializer=lambda m: json.loads(m.decode("utf-8")) # reconvertit les octets reçus en dictionnaire Python
            )
            print(f"[CLIENTS] Connecté au topic '{TOPIC_COMMANDES}'")
            break
        except Exception as e:
            time.sleep(2)

    if not consumer:
        return

    # Boucle d'écoute
    for message in consumer:
        cmd = message.value # message.value contient le dictionnaire JSON produit par le service commandes
        clid = cmd.get("client_id")
        produit = cmd.get("produit")
        montant = cmd.get("montant")

        # Recherche du client associé pour l'affichage
        client_obj = next((c for c in clients if c["id"] == clid), None)
        nom = client_obj["nom"] if client_obj else f"Inconnu ({clid})"

        # Mise à jour de l'état local (compteur par client)
        commandes_par_client[clid] += 1

        # Affichage (cf 1.5 TP)
        print("\n--------------------------------------------------")
        print(f"[NOUVELLE COMMANDE REÇUE]")
        print(f"• Client  : {nom} (id: {clid})")
        print(f"• Produit : {produit}")
        print(f"• Montant : {montant} €")
        print(f"=> Total commandes pour {nom} : {commandes_par_client[clid]}")
        print("--------------------------------------------------\n")

# API REST de consultation
@app.route('/clients', methods=['GET'])
def get_clients():
    # Permet de consulter la liste des clients et leur statut
    return jsonify(clients), 200

if __name__ == '__main__':
    t = threading.Thread(target=demarrer_consommateur_kafka, daemon=True)
    t.start()
    # Le service clients écoute sur le port 8080
    app.run(host='0.0.0.0', port=8080)