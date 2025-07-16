import socket
import threading
import time
import sys
import uuid 
from collections import deque 
from typing import Dict, Set, Tuple
from message import Message 

BUFFER_SIZE = 4096
RETRY_INTERVAL = 3  
MAX_RETRIES = 5    

class DistributedProcess:
    def __init__(self, process_id: str, port: int, peer_ports: Dict[str, int]):
        self.process_id = process_id
        self.port = port
        self.peer_ports = peer_ports 
        self.peers_connected: Set[str] = set() 
        self.peer_connections: Dict[str, socket.socket] = {} 

        self.lamport_clock = 0
        self.clock_lock = threading.Lock()  

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(5)
        print(f"[Processo {self.process_id}] Servidor escutando na porta {self.port}")

        self.pending_acks: Dict[str, Tuple[Message, Set[str], int]] = {}
        self.pending_acks_lock = threading.Lock()

        self.received_message_ids: Set[str] = set()
        self.received_message_ids_lock = threading.Lock()

        self.outgoing_message_queue = deque()
        self.outgoing_queue_lock = threading.Lock()

        self.connections_lock = threading.Lock()

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

    def _handle_client_connection(self, conn: socket.socket, addr: Tuple[str, int], peer_id: str = None):
        """Lida com as mensagens recebidas de um cliente."""
        try:
            if not peer_id:
                initial_data = conn.recv(BUFFER_SIZE).decode('utf-8').strip()
                if not initial_data.startswith("HELLO_IAM:"):
                    print(f"[{self.process_id}] Erro: Mensagem inicial inválida de {addr}")
                    conn.close()
                    return

                peer_id = initial_data.split(":")[1].strip()
                conn.sendall("OK\n".encode('utf-8'))

            with self.connections_lock:
                self.peers_connected.add(peer_id)
                self.peer_connections[peer_id] = conn
                print(f"[{self.process_id}] Conectado com sucesso ao Processo {peer_id} em {addr}")

            while True:
                try:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break

                    message = data.decode('utf-8').strip()
                    if message.startswith("ACK:"):
                        ack_id = message.split(":")[1].strip()
                        self._handle_ack_received(ack_id, peer_id)
                    else:
                        try:
                            incoming_message = Message.from_json(message)
                            
                            old_clock = self.lamport_clock
                            new_clock = self._update_lamport_clock(incoming_message.lamport_timestamp)
                            
                            print(f"[{self.process_id}] [Relógio: {old_clock} -> {new_clock}] "
                                  f"Recebido de {incoming_message.sender_id}: '{incoming_message.message_content}' "
                                  f"(ID: {incoming_message.message_id})")

                            with self.received_message_ids_lock:
                                if incoming_message.message_id not in self.received_message_ids:
                                    self.received_message_ids.add(incoming_message.message_id)
                                    ack = f"ACK:{incoming_message.message_id}\n"
                                    conn.sendall(ack.encode('utf-8'))
                                    print(f"[{self.process_id}] Enviado ACK para {incoming_message.sender_id} "
                                          f"para mensagem {incoming_message.message_id}")

                        except json.JSONDecodeError as e:
                            print(f"[{self.process_id}] Erro ao decodificar mensagem de {peer_id}: {e}")

                except ConnectionError:
                    break
                except Exception as e:
                    print(f"[{self.process_id}] Erro ao processar mensagem de {peer_id}: {e}")
                    break

        except Exception as e:
            print(f"[{self.process_id}] Erro na conexão com {peer_id}: {e}")
        finally:
            with self.connections_lock:
                if peer_id in self.peers_connected:
                    self.peers_connected.remove(peer_id)
                if peer_id in self.peer_connections:
                    del self.peer_connections[peer_id]
            conn.close()
            print(f"[{self.process_id}] Conexão com {peer_id} encerrada.")

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
            try:
                for peer_id, peer_port in self.peer_ports.items():
                    if peer_id == self.process_id:  
                        continue
                    
                    if peer_id < self.process_id:
                        continue
                        
                    if peer_id not in self.peers_connected:
                        try:
                            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            client_socket.connect(('localhost', peer_port))
                            
                            handshake = f"HELLO_IAM:{self.process_id}\n"
                            client_socket.sendall(handshake.encode('utf-8'))
                            
                            response = client_socket.recv(BUFFER_SIZE).decode('utf-8').strip()
                            if response != "OK":
                                print(f"[{self.process_id}] Erro: Handshake falhou com {peer_id}")
                                client_socket.close()
                                continue

                            threading.Thread(
                                target=self._handle_client_connection,
                                args=(client_socket, ('localhost', peer_port), peer_id),
                                daemon=True
                            ).start()

                            self.peers_connected.add(peer_id)
                            print(f"[{self.process_id}] Conectado com sucesso ao Processo {peer_id} na porta {peer_port}")

                        except ConnectionRefusedError:
                            pass  
                        except Exception as e:
                            print(f"[{self.process_id}] Erro ao conectar a {peer_id} ({peer_port}): {e}")
                
                time.sleep(RETRY_INTERVAL)  
            except Exception as e:
                print(f"[{self.process_id}] Erro no loop de conexão: {e}")
                time.sleep(RETRY_INTERVAL)

    def _send_message_to_peer(self, peer_id: str, message: Message):
        """Envia uma mensagem para um peer específico usando conexão existente ou criando nova."""
        try:
            with self.connections_lock:
                if peer_id not in self.peer_connections:
                    peer_port = self.peer_ports.get(peer_id)
                    if not peer_port:
                        print(f"[{self.process_id}] Erro: Porta para o peer {peer_id} não encontrada.")
                        return False

                    try:
                        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket.connect(('localhost', peer_port))
                        
                        handshake = f"HELLO_IAM:{self.process_id}\n"
                        client_socket.sendall(handshake.encode('utf-8'))
                        
                        response = client_socket.recv(BUFFER_SIZE).decode('utf-8').strip()
                        if response != "OK":
                            print(f"[{self.process_id}] Erro: Handshake falhou com {peer_id}")
                            client_socket.close()
                            return False

                        self.peer_connections[peer_id] = client_socket
                        self.peers_connected.add(peer_id)
                        
                        threading.Thread(
                            target=self._handle_client_connection,
                            args=(client_socket, ('localhost', peer_port), peer_id),
                            daemon=True
                        ).start()

                    except Exception as e:
                        print(f"[{self.process_id}] Erro ao estabelecer conexão com {peer_id}: {e}")
                        return False

                try:
                    self.peer_connections[peer_id].sendall(message.to_json().encode('utf-8') + b'\n')
                    return True
                except Exception as e:
                    print(f"[{self.process_id}] Erro ao enviar mensagem para {peer_id}: {e}")
                    self.peer_connections[peer_id].close()
                    del self.peer_connections[peer_id]
                    self.peers_connected.remove(peer_id)
                    return False

        except Exception as e:
            print(f"[{self.process_id}] Erro geral ao enviar mensagem para {peer_id}: {e}")
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
        if not self.peers_connected:
            print(f"[{self.process_id}] Não há peers conectados. Aguarde a conexão antes de enviar mensagens.")
            return

        message_id = str(uuid.uuid4())  
        lamport_ts = self._increment_lamport_clock()  

        message = Message(self.process_id, message_content, lamport_ts, message_id)
        
        peers_to_ack = self.peers_connected.copy()

        with self.pending_acks_lock:
            self.pending_acks[message_id] = (message, peers_to_ack.copy(), 0)  

        print(f"[{self.process_id}] [Relógio: {lamport_ts}] "
              f"Enviando multicast: '{message_content}' (ID: {message_id})")

        with self.outgoing_queue_lock:
            self.outgoing_message_queue.append((message, peers_to_ack.copy()))

    def _retransmission_daemon(self):
        """Daemon que gerencia retransmissões de mensagens pendentes de ACK."""
        time.sleep(3)
        
        while True:
            try:
                time.sleep(RETRY_INTERVAL)

                with self.outgoing_queue_lock:
                    while self.outgoing_message_queue:
                        message, peers_to_send = self.outgoing_message_queue.popleft()
                        active_peers = peers_to_send.intersection(self.peers_connected)
                        if active_peers:
                            for peer_id in active_peers:
                                success = self._send_message_to_peer(peer_id, message)
                                if not success and peer_id in self.peers_connected:
                                    with self.connections_lock:
                                        if peer_id in self.peers_connected:
                                            self.peers_connected.remove(peer_id)
                                        if peer_id in self.peer_connections:
                                            try:
                                                self.peer_connections[peer_id].close()
                                            except:
                                                pass
                                            del self.peer_connections[peer_id]
                        else:
                            print(f"[{self.process_id}] Nenhum peer ativo para enviar a mensagem.")

                with self.pending_acks_lock:
                    messages_to_resend = []
                    for msg_id, (message_obj, awaiting_peers, retries) in list(self.pending_acks.items()):
                        awaiting_peers = awaiting_peers.intersection(self.peers_connected)
                        
                        if not awaiting_peers:
                            del self.pending_acks[msg_id]
                            print(f"[{self.process_id}] Mensagem {msg_id} completada ou sem peers ativos.")
                            continue

                        if retries >= MAX_RETRIES:
                            print(f"[{self.process_id}] ERRO: Máximo de re-tentativas atingido para "
                                  f"mensagem {msg_id}. Peers não alcançados: {awaiting_peers}")
                            del self.pending_acks[msg_id]
                            continue

                        self.pending_acks[msg_id] = (message_obj, awaiting_peers, retries + 1)
                        messages_to_resend.append((message_obj, awaiting_peers))
                        print(f"[{self.process_id}] Re-tentativa {retries + 1} para mensagem {msg_id}. "
                              f"Peers esperando ACK: {awaiting_peers}")

                for message_obj, peers_to_resend in messages_to_resend:
                    for peer_id in peers_to_resend:
                        success = self._send_message_to_peer(peer_id, message_obj)
                        if not success and peer_id in self.peers_connected:
                            with self.connections_lock:
                                if peer_id in self.peers_connected:
                                    self.peers_connected.remove(peer_id)
                                if peer_id in self.peer_connections:
                                    try:
                                        self.peer_connections[peer_id].close()
                                    except:
                                        pass
                                    del self.peer_connections[peer_id]

            except Exception as e:
                print(f"[{self.process_id}] Erro no daemon de retransmissão: {e}")
                time.sleep(RETRY_INTERVAL)  


    def start(self):
        """Inicia os threads do processo."""
        threading.Thread(target=self._accept_connections, daemon=True).start()
        threading.Thread(target=self._connect_to_peers, daemon=True).start()
        threading.Thread(target=self._retransmission_daemon, daemon=True).start()

        print(f"[{self.process_id}] Digite 'multicast <sua mensagem>' para enviar.")
        print(f"[{self.process_id}] Digite 'exit' para sair.")
        while True:
            try:
                command = input(f"[{self.process_id}] > ").strip()
                if command.lower().startswith("multicast "):
                    message_content = command[len("multicast "):].strip()
                    if message_content:
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

    peer_ports[process_id] = port

    process = DistributedProcess(process_id, port, peer_ports)
    process.start()