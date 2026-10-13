import time
import random
import requests

url = "http://localhost:8081/predict"
print("Lancement du flux continu vers le service commandes (Ctrl+C pour stopper)...")

cid = 100
while True:
    payload = {
        "client_id": cid,
        "age": random.randint(18, 65),
        "anciennete": random.randint(1, 10),
        "nombre_commandes": random.randint(1, 20),
        "montant_total": round(random.uniform(50, 3000), 2),
        "montant_moyen": round(random.uniform(20, 200), 2),
        "jours_depuis_derniere_commande": random.randint(1, 100)
    }
    try:
        rep = requests.post(url, json=payload, timeout=2)
        print(f"Demande transmise pour le client {cid} (Statut: {rep.status_code})")
        cid += 1
    except Exception as err:
        print(f"Erreur d'envoi : {err}")
    time.sleep(1)