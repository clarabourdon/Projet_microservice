from flask import Flask, jsonify

app = Flask(__name__)

# Données du TP pour les clients
clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# Endpoint : GET /clients => on retourne tous les clients
@app.route('/clients', methods=['GET'])
def get_clients():
    return jsonify(clients), 200

# Endpoint : GET /clients/<id> => on retourne un client par son id
@app.route('/clients/<int:client_id>', methods=['GET'])
def get_client(client_id):
    client = next((c for c in clients if c["id"] == client_id), None)
    if client:
        return jsonify(client), 200
    # Cas ou le client n'a pas été trouvé, erreur 404
    return jsonify({"erreur": "Client non trouvé"}), 404

if __name__ == '__main__':
    # Écoute sur toutes les interfaces (0.0.0.0) sur le port 8080
    app.run(host='0.0.0.0', port=8080)