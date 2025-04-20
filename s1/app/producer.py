from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  # ou "localhost:9092" se estiver rodando fora do container
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_event(topic, event):
    try:
        producer.send(topic, value=event)
        producer.flush()  # Garantir que os dados foram enviados
        print(f"[Producer] ✅ Evento enviado para o Kafka: {event}")
    except Exception as e:
        print(f"[Producer] ❌ Erro ao enviar evento: {e}")
