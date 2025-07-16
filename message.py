import json

class Message:
    """
    Representa uma mensagem no sistema distribuído.
    Inclui informações para o multicast confiável e o Relógio de Lamport.
    """
    def __init__(self, sender_id: str, message_content: str, lamport_timestamp: int, message_id: str):
        self.sender_id = sender_id          
        self.message_content = message_content 
        self.lamport_timestamp = lamport_timestamp 
        self.message_id = message_id        

    def to_json(self) -> str:
        """Serializa o objeto Message para uma string JSON."""
        return json.dumps({
            "sender_id": self.sender_id,
            "message_content": self.message_content,
            "lamport_timestamp": self.lamport_timestamp,
            "message_id": self.message_id
        })

    @staticmethod
    def from_json(json_string: str):
        """Desserializa uma string JSON para um objeto Message."""
        data = json.loads(json_string)
        return Message(
            data["sender_id"],
            data["message_content"],
            data["lamport_timestamp"],
            data["message_id"]
        )

    def __str__(self) -> str:
        """Representação em string da mensagem para exibição."""
        return (f"[De: {self.sender_id}] [Relógio: {self.lamport_timestamp}] "
                f"Mensagem ID: {self.message_id} - '{self.message_content}'")