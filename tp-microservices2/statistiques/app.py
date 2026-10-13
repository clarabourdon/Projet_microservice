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

# Total des commandes par produit :
commandes_par_produit = defaultdict(int)
# Total du chiffre d'affaires par mois
ca_par_mois = defaultdict(float)

# Définition du deuxième consommateur
# Consomme également les événements du topic 'commandes-creees' mais avec son propre group_id ('statistiques-group')
# reçoit une COPIE intégrale de tous les événements reçus par le service clients
def demarrer_consommateur_stats():
    consumer = None
    # Gestion des pleusieurs tentatives
    for tentative in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_COMMANDES,
                bootstrap_servers=KAFKA_BROKER,
                group_id="statistiques-group", # group_id distinct dinc offset indépendant de clients-group
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"[STATS] Connecté au topic '{TOPIC_COMMANDES}'")
            break
        except Exception as e:
            time.sleep(2)

    if not consumer:
        return

    # Boucle d'écoute des messages
    for message in consumer:
        cmd = message.value
        produit = cmd.get("produit")
        montant = cmd.get("montant", 0)
        mois = cmd.get("mois", "2026-10")

        # Mise à jour des statistiques en temps réel
        if produit:
            commandes_par_produit[produit] += 1
        ca_par_mois[mois] += montant
        print(f"[STATS] Reçu : {produit} ({montant} €) pour {mois}")

# API REST du microservice statistiques
@app.route('/stats/produits/<produit>', methods=['GET'])
# R etourne le nombre total de commandes pour un produit donné
def get_stats_produit(produit):
    return jsonify({"produit": produit, "nombre_commandes": commandes_par_produit.get(produit, 0)}), 200

@app.route('/stats/ca/<mois>', methods=['GET'])
# Retourne le chiffre d'affaires total pour un mois donné
def get_stats_ca(mois):
    return jsonify({"mois": mois, "chiffre_affaires": ca_par_mois.get(mois, 0.0)}), 200

if __name__ == '__main__':
    # Lancement du consommateur dans son thread d'arrière-plan
    t = threading.Thread(target=demarrer_consommateur_stats, daemon=True)
    t.start()
    # Le service statistiques écoute sur le port 8082
    app.run(host='0.0.0.0', port=8082)