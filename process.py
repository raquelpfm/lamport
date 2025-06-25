import socket
import threading
import time
import sys
import uuid # Para gerar IDs de mensagens únicos
from collections import deque # Para a fila de mensagens a enviar
from typing import Dict, Set, Tuple
from message import Message # Importa a classe Message

# --- Configurações Iniciais ---
BUFFER_SIZE = 4096
RETRY_INTERVAL = 3 # Segundos para esperar antes de re-tentar enviar uma mensagem
MAX_RETRIES = 5    # Número máximo de re-tentativas para uma mensagem

class DistributedProcess:
    def __init__(self, process_id: str, port: int, peer_ports: Dict[str, int]):
        self.process_id = process_id
        self.port = port
        self.peer_ports = peer_ports # {ID_peer: porta_peer}
        self.peers_connected: Set[str] = set() # IDs dos peers atualmente conectados

        self.lamport_clock = 0
        self.clock_lock = threading.Lock() # Para garantir acesso seguro ao relógio

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Reutiliza a porta
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(5)
        print(f"[Processo {self.process_id}] Servidor escutando na porta {self.port}")

        # Mensagens pendentes de ACK: {message_id: {message_obj, remetentes_esperando_ack: {peer_id}, tentativas}}
        self.pending_acks: Dict[str, Tuple[Message, Set[str], int]] = {}
        self.pending_acks_lock = threading.Lock()

        # Armazenar IDs de mensagens recebidas para evitar duplicatas (para multicast confiável)
        self.received_message_ids: Set[str] = set()
        self.received_message_ids_lock = threading.Lock()

        # Fila de mensagens para enviar em multicast (para separar do thread principal)
        self.outgoing_message_queue = deque()
        self.outgoing_queue_lock = threading.Lock()

    def _increment_lamport_clock(self):
        """Incrementa o Relógio de Lamport do processo."""
        with self.clock_lock:
            self.lamport_clock += 1
            return self.lamport_clock

    def _update_lamport_clock(self, received_timestamp: int):
        """Atualiza o Relógio de Lamport ao receber uma mensagem."""
        with self.clock_lock:
            self.lamport_clock = max(self.lamport_clock, received_timestamp) + 1
            return self.lamport_clock

    def _handle_client_connection(self, conn: socket.socket, addr: Tuple[str, int]):
        """Lida com as mensagens recebidas de um cliente (outro processo)."""
        peer_id = None
        try:
            # A primeira mensagem do cliente é seu ID de processo
            initial_data = conn.recv(BUFFER_SIZE).decode('utf-8')
            if not initial_data.startswith("HELLO_IAM:"):
                print(f"[{self.process_id}] Erro: Mensagem inicial inválida de {addr}")
                conn.close()
                return

            peer_id = initial_data.split(":")[1].strip()
            self.peers_connected.add(peer_id)
            print(f"[{self.process_id}] Conectado com sucesso ao Processo {peer_id} em {addr}")

            while True:
                data = conn.recv(BUFFER_SIZE)
                if not data:
                    break # Conexão fechada pelo peer

                json_string = data.decode('utf-8')
                try:
                    # Verifica se é um ACK
                    if json_string.startswith("ACK:"):
                        original_message_id = json_string.split(":")[1].strip()
                        self._handle_ack_received(original_message_id, peer_id)
                        continue # Não é uma mensagem de conteúdo, não processa como tal

                    # Se não é ACK, tenta desserializar como Message
                    incoming_message = Message.from_json(json_string)

                    # --- Lógica do Relógio de Lamport ---
                    old_clock = self.lamport_clock
                    new_clock = self._update_lamport_clock(incoming_message.lamport_timestamp)

                    print(f"[{self.process_id}] [Relógio: {old_clock} -> {new_clock}] "
                          f"Recebido de {incoming_message.sender_id}: '{incoming_message.message_content}' "
                          f"(ID: {incoming_message.message_id})")

                    # --- Lógica de Multicast Confiável (Evitar duplicatas) ---
                    with self.received_message_ids_lock:
                        if incoming_message.message_id in self.received_message_ids:
                            print(f"[{self.process_id}] [Relógio: {self.lamport_clock}] "
                                  f"Mensagem {incoming_message.message_id} já recebida. Ignorando.")
                        else:
                            self.received_message_ids.add(incoming_message.message_id)
                            # Enviar ACK de volta ao remetente
                            conn.sendall(f"ACK:{incoming_message.message_id}".encode('utf-8'))
                            print(f"[{self.process_id}] Enviado ACK para {incoming_message.sender_id} "
                                  f"para mensagem {incoming_message.message_id}")

                except json.JSONDecodeError:
                    print(f"[{self.process_id}] Erro ao decodificar JSON de {peer_id}: {json_string}")
                except Exception as e:
                    print(f"[{self.process_id}] Erro ao processar mensagem de {peer_id}: {e}")

        except ConnectionResetError:
            print(f"[{self.process_id}] Conexão com {peer_id if peer_id else addr} foi resetada.")
        except Exception as e:
            print(f"[{self.process_id}] Erro na conexão com {peer_id if peer_id else addr}: {e}")
        finally:
            if peer_id and peer_id in self.peers_connected:
                self.peers_connected.remove(peer_id)
            conn.close()
            print(f"[{self.process_id}] Conexão com {peer_id if peer_id else addr} encerrada.")

    def _accept_connections(self):
        """Aceita conexões de outros processos."""
        while True:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_client_connection, args=(conn, addr), daemon=True).start()
            except Exception as e:
                print(f"[{self.process_id}] Erro ao aceitar conexão: {e}")
                break

    def _connect_to_peers(self):
        """Tenta conectar-se a outros processos se ainda não estiver conectado."""
        while True:
            for peer_id, peer_port in self.peer_ports.items():
                if peer_id == self.process_id: # Não conecta consigo mesmo
                    continue
                if peer_id not in self.peers_connected:
                    try:
                        # Tenta se conectar como cliente
                        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket.connect(('localhost', peer_port))
                        client_socket.sendall(f"HELLO_IAM:{self.process_id}".encode('utf-8'))

                        # Como somos o cliente, precisamos de um thread para receber ACKs e mensagens
                        # do peer para o qual nos conectamos.
                        # Este thread ouvirá as respostas do peer.
                        threading.Thread(target=self._handle_client_connection, args=(client_socket, ('localhost', peer_port)), daemon=True).start()

                        # Adiciona o peer à lista de conectados para evitar novas tentativas
                        self.peers_connected.add(peer_id)
                        print(f"[{self.process_id}] Conectado com sucesso ao Processo {peer_id} na porta {peer_port}")

                    except ConnectionRefusedError:
                        # print(f"[{self.process_id}] Conexão recusada por {peer_id} na porta {peer_port}. Tentando novamente...")
                        pass # Silencioso, pois é normal no início
                    except Exception as e:
                        print(f"[{self.process_id}] Erro ao conectar a {peer_id} ({peer_port}): {e}")
            time.sleep(RETRY_INTERVAL) # Espera antes de tentar novamente

    def _send_message_to_peer(self, peer_id: str, message: Message):
        """Envia uma mensagem para um peer específico."""
        peer_port = self.peer_ports.get(peer_id)
        if not peer_port:
            print(f"[{self.process_id}] Erro: Porta para o peer {peer_id} não encontrada.")
            return False

        try:
            # Cria um novo socket para enviar a mensagem, para não interferir nos sockets de escuta/recebimento
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', peer_port))
                s.sendall(f"HELLO_IAM:{self.process_id}".encode('utf-8')) # Envia ID ao peer
                s.sendall(message.to_json().encode('utf-8'))
            return True
        except ConnectionRefusedError:
            print(f"[{self.process_id}] Conexão recusada por {peer_id}. Mensagem não enviada.")
            return False
        except Exception as e:
            print(f"[{self.process_id}] Erro ao enviar mensagem para {peer_id}: {e}")
            return False

    def _handle_ack_received(self, original_message_id: str, sender_peer_id: str):
        """Processa um ACK recebido de um peer."""
        with self.pending_acks_lock:
            if original_message_id in self.pending_acks:
                message_obj, awaiting_peers, retries = self.pending_acks[original_message_id]
                if sender_peer_id in awaiting_peers:
                    awaiting_peers.remove(sender_peer_id)
                    print(f"[{self.process_id}] ACK recebido de {sender_peer_id} para mensagem {original_message_id}. "
                          f"Faltam {len(awaiting_peers)} ACKs.")
                    if not awaiting_peers:
                        # Todos os ACKs para esta mensagem foram recebidos
                        del self.pending_acks[original_message_id]
                        print(f"[{self.process_id}] Todos os ACKs para mensagem {original_message_id} recebidos.")
                else:
                    print(f"[{self.process_id}] ACK duplicado ou inesperado de {sender_peer_id} para {original_message_id}.")
            else:
                print(f"[{self.process_id}] ACK para mensagem desconhecida {original_message_id} de {sender_peer_id}.")

    def _send_multicast_message(self, message_content: str):
        """
        Envia uma mensagem para todos os peers, implementando a confiabilidade.
        """
        message_id = str(uuid.uuid4()) # Gera um ID único para a mensagem
        lamport_ts = self._increment_lamport_clock() # Incrementa o relógio no envio

        message = Message(self.process_id, message_content, lamport_ts, message_id)
        
        # Copia os peers para esta mensagem
        peers_to_ack = set(self.peer_ports.keys()) - {self.process_id}

        # Adiciona a mensagem à lista de pendentes de ACK
        with self.pending_acks_lock:
            self.pending_acks[message_id] = (message, peers_to_ack.copy(), 0) # message_obj, awaiting_peers, retries

        print(f"[{self.process_id}] [Relógio: {lamport_ts}] "
              f"Enviando multicast: '{message_content}' (ID: {message_id})")

        # Coloca a mensagem na fila de saída para o thread de retransmissão lidar
        with self.outgoing_queue_lock:
            self.outgoing_message_queue.append((message, peers_to_ack.copy()))

    def _retransmission_daemon(self):
        """Daemon que gerencia retransmissões de mensagens pendentes de ACK."""
        while True:
            time.sleep(RETRY_INTERVAL) # Verifica periodicamente

            with self.pending_acks_lock:
                messages_to_resend = []
                for msg_id, (message_obj, awaiting_peers, retries) in self.pending_acks.items():
                    if retries >= MAX_RETRIES:
                        print(f"[{self.process_id}] ERRO: Máximo de re-tentativas atingido para "
                              f"mensagem {msg_id}. Peers não alcançados: {awaiting_peers}")
                        # Considerar remover esta mensagem ou tratar como falha
                        continue

                    if awaiting_peers: # Se ainda há peers esperando ACK
                        messages_to_resend.append((message_obj, awaiting_peers.copy()))
                        self.pending_acks[msg_id] = (message_obj, awaiting_peers, retries + 1)
                        print(f"[{self.process_id}] Re-tentativa {retries + 1} para mensagem {msg_id}. "
                              f"Peers esperando ACK: {awaiting_peers}")

            for message_obj, peers_to_resend in messages_to_resend:
                for peer_id in peers_to_resend:
                    # Envia a mensagem novamente para os peers que não confirmaram
                    threading.Thread(target=self._send_message_to_peer, args=(peer_id, message_obj), daemon=True).start()


    def start(self):
        """Inicia os threads do processo."""
        # Thread para aceitar conexões de outros processos
        threading.Thread(target=self._accept_connections, daemon=True).start()
        # Thread para tentar conectar-se aos outros processos
        threading.Thread(target=self._connect_to_peers, daemon=True).start()
        # Thread para gerenciar retransmissões
        threading.Thread(target=self._retransmission_daemon, daemon=True).start()

        # Loop principal para entrada do usuário
        print(f"[{self.process_id}] Digite 'multicast <sua mensagem>' para enviar.")
        print(f"[{self.process_id}] Digite 'exit' para sair.")
        while True:
            try:
                command = input(f"[{self.process_id}] > ").strip()
                if command.lower().startswith("multicast "):
                    message_content = command[len("multicast "):].strip()
                    if message_content:
                        # Coloca a mensagem na fila para ser processada pelo retransmission daemon
                        self._send_multicast_message(message_content)
                    else:
                        print(f"[{self.process_id}] Por favor, forneça o conteúdo da mensagem.")
                elif command.lower() == "exit":
                    print(f"[{self.process_id}] Encerrando...")
                    break
                else:
                    print(f"[{self.process_id}] Comando inválido.")
            except KeyboardInterrupt:
                print(f"\n[{self.process_id}] Encerrando por interrupção do usuário...")
                break
            except Exception as e:
                print(f"[{self.process_id}] Erro no loop principal: {e}")

        self.server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python process.py <ID_PROCESSO> <PORTA_PROCESSO> [<ID_PEER>:<PORTA_PEER> ...]")
        print("Exemplo: python process.py P1 8001 P2:8002 P3:8003")
        sys.exit(1)

    process_id = sys.argv[1]
    port = int(sys.argv[2])

    peer_ports: Dict[str, int] = {}
    for i in range(3, len(sys.argv)):
        try:
            peer_id, peer_port_str = sys.argv[i].split(':')
            peer_ports[peer_id] = int(peer_port_str)
        except ValueError:
            print(f"Formato inválido para peer: {sys.argv[i]}. Use ID:PORTA.")
            sys.exit(1)

    # Adiciona o próprio processo na lista de peers (útil para o multicast "para todos", exceto ele mesmo)
    peer_ports[process_id] = port

    process = DistributedProcess(process_id, port, peer_ports)
    process.start()