import sys
import json
from kafka import KafkaConsumer

nom_instance = sys.argv[1] if len(sys.argv) > 1 else "Instance-1"
groupe_choisi = sys.argv[2] if len(sys.argv) > 2 else "groupe-A"

print(f"[{nom_instance}] Écoute du topic avec group_id='{groupe_choisi}'...")

consumer = KafkaConsumer(
    "commandes-creees",
    bootstrap_servers="localhost:9092",
    group_id=groupe_choisi,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for message in consumer:
    data = message.value
    print(f"[{nom_instance}] reçu : Produit='{data.get('produit')}', Montant={data.get('montant')}€")