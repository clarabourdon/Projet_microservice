import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# URL de base du service clients
CLIENTS_SERVICE_URL = os.environ.get("CLIENTS_SERVICE_URL", "http://localhost:8080")

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

@app.route('/commandes', methods=['GET'])
def get_commandes():
    return jsonify(commandes), 200

@app.route('/commandes/<int:commande_id>', methods=['GET'])
def get_commande(commande_id):
    # Quest 1 (1.5): récupération de la commande
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if not commande:
        return jsonify({"erreur": "Commande non trouvée"}), 404

    # Quest 2 : récupératipn du client_id associé
    client_id = commande["client_id"]

    # Quest 3 : appel du microservice clients vec requête HTTP + gestion des erreurs
    try:
        response = requests.get(f"{CLIENTS_SERVICE_URL}/clients/{client_id}", timeout=3)
        if response.status_code == 200:
            client_data = response.json()
        # Code 404 pour un client inexistant
        elif response.status_code == 404:
            return jsonify({"erreur": f"Client {client_id} non trouvé"}), 404
        # Cas ou le microservuce clients n'est pas dispo
        else:
            return jsonify({"erreur": "Erreur inattendue du service clients"}), 502
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Microservice clients indisponible"}), 503

    # On retourne la commande enrichie avec les données client
    resultat = {
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": client_data
    }
    return jsonify(resultat), 200

if __name__ == '__main__':
    # On écoute sur le port 8081 en local pour éviter le conflit avec le port 8080 de clients
    app.run(host='0.0.0.0', port=8081)