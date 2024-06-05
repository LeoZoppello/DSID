import socket
import threading
import sys
import random

class PeerNode:
    def __init__(self, address, port, neighbors_file=None, kv_file=None):
        self.address = address
        self.port = int(port)
        self.neighbors = {}
        self.kv_store = {}
        self.seq_no = 0
        self.ttl_default = 100
        self.statistics = {
            'messages_sent': 0,
            'messages_received': 0,
            'search_successful': 0,
            'search_failed': 0
        }
        self.seen_messages = set()
        self.load_neighbors(neighbors_file)
        self.load_kv_store(kv_file)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.address, self.port))
        self.sock.listen(5)
        
        threading.Thread(target=self.listen_for_connections).start()

    def load_neighbors(self, neighbors_file):
        if neighbors_file:
            with open(neighbors_file, 'r') as f:
                for line in f:
                    neighbor_address, neighbor_port = line.strip().split(':')
                    print(f'Tentando adicionar vizinho {neighbor_address}:{neighbor_port}')
                    if self.send_hello(neighbor_address, int(neighbor_port)):
                        self.neighbors[(neighbor_address, int(neighbor_port))] = True

    def load_kv_store(self, kv_file):
        if kv_file:
            with open(kv_file, 'r') as f:
                for line in f:
                    key, value = line.strip().split()
                    self.kv_store[key] = value

    def send_hello(self, address, port):
        message = f"{self.address}:{self.port} {self.seq_no} 1 HELLO\n"
        self.seq_no += 1
        response = self.send_message(message, address, port)
        if response == 'HELLO_OK':
            print(f'Envio feito com sucesso: {message.strip()}')
            return True
        else:
            print(f'Erro ao enviar mensagem: {message.strip()}')
            return False

    def send_message(self, message, address, port):
        try:
            print(f'Encaminhando mensagem "{message.strip()}" para {address}:{port}')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((address, port))
                s.sendall(message.encode('utf-8'))
                response = s.recv(1024).decode('utf-8').strip()
                if response.endswith('_OK'):
                    self.statistics['messages_sent'] += 1
                    return response
        except Exception as e:
            print(f'Erro ao enviar mensagem: {e}')
        return None

    def listen_for_connections(self):
        while True:
            conn, addr = self.sock.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

    def handle_connection(self, conn, addr):
        with conn:
            data = conn.recv(1024).decode('utf-8').strip()
            if data:
                self.statistics['messages_received'] += 1
                print(f"Recebida mensagem: {data}")
                parts = data.split()
                origin, seq_no, ttl, operation = parts[:4]
                ttl = int(ttl) - 1
                if operation == 'HELLO':
                    origin_address, origin_port = origin.split(':')
                    origin_port = int(origin_port)
                    if (origin_address, origin_port) not in self.neighbors:
                        self.neighbors[(origin_address, origin_port)] = True
                        print(f"Adicionando vizinho na tabela: {origin_address}:{origin_port}")
                    else:
                        print(f"Vizinho já está na tabela: {origin_address}:{origin_port}")
                    conn.sendall(b'HELLO_OK\n')
                elif operation == 'SEARCH':
                    mode = parts[4]
                    last_hop_port = int(parts[5])
                    key = parts[6]
                    hop_count = int(parts[7]) + 1
                    if (origin, seq_no) in self.seen_messages:
                        print("Mensagem já vista, descartando.")
                        return
                    self.seen_messages.add((origin, seq_no))
                    if key in self.kv_store:
                        value = self.kv_store[key]
                        response = f"{self.address}:{self.port} {self.seq_no} {ttl} VAL {mode} {key} {value} {hop_count}\n"
                        self.send_message(response, origin.split(':')[0], int(origin.split(':')[1]))
                        self.statistics['search_successful'] += 1
                    elif ttl > 0:
                        if mode == 'FL':
                            for neighbor in self.neighbors:
                                if neighbor[1] != last_hop_port:
                                    self.forward_search(key, origin, seq_no, ttl, neighbor, mode, hop_count)
                        elif mode == 'RW':
                            next_hop = self.get_random_neighbor(exclude_port=last_hop_port)
                            if next_hop:
                                self.forward_search(key, origin, seq_no, ttl, next_hop, mode, hop_count)
                elif operation == 'VAL':
                    mode = parts[4]
                    key, value = parts[5], parts[6]
                    hop_count = parts[7]
                    print(f"Chave {key} encontrada: {value} (modo: {mode}, saltos: {hop_count})")
                    self.statistics['search_successful'] += 1
                conn.sendall(f"{operation}_OK\n".encode('utf-8'))

    def forward_search(self, key, origin, seq_no, ttl, neighbor, mode, hop_count):
        message = f"{origin} {seq_no} {ttl} SEARCH {mode} {self.port} {key} {hop_count}\n"
        response = self.send_message(message, neighbor[0], neighbor[1])
        if response is None:
            self.statistics['search_failed'] += 1

    def get_random_neighbor(self, exclude_port=None):
        neighbors_list = list(self.neighbors.keys())
        if exclude_port:
            neighbors_list = [n for n in neighbors_list if n[1] != exclude_port]
        if not neighbors_list:
            return None
        return random.choice(neighbors_list)

    def menu(self):
        while True:
            print("\nEscolha o comando:")
            print("[0] Listar vizinhos")
            print("[1] HELLO")
            print("[2] SEARCH (flooding)")
            print("[3] SEARCH (random walk)")
            print("[4] SEARCH (busca em profundidade)")
            print("[5] Estatísticas")
            print("[6] Alterar valor padrão de TTL")
            print("[9] Sair")
            choice = input("Escolha a opção de busca: ")
            if choice == '0':
                self.list_neighbors()
            elif choice == '1':
                self.hello()
            elif choice == '2':
                key = input("Digite a chave a ser buscada: ")
                self.flooding_search(key)
            elif choice == '3':
                key = input("Digite a chave a ser buscada: ")
                self.random_walk_search(key)
            elif choice == '4':
                key = input("Digite a chave a ser buscada: ")
                self.dfs_search(key)
            elif choice == '5':
                self.show_statistics()
            elif choice == '6':
                new_ttl = input("Digite o novo valor de TTL: ")
                self.ttl_default = int(new_ttl)
            elif choice == '9':
                break

    def list_neighbors(self):
        neighbors_list = list(self.neighbors.keys())
        print(f"Há {len(neighbors_list)} vizinhos na tabela:")
        for index, neighbor in enumerate(neighbors_list):
            print(f"[{index}] {neighbor[0]} {neighbor[1]}")

    def hello(self):
        self.list_neighbors()
        choice = input("Escolha o vizinho: ")
        try:
            choice_index = int(choice)
            neighbor = list(self.neighbors.keys())[choice_index]
            self.send_hello(neighbor[0], neighbor[1])
        except (IndexError, ValueError):
            print("Escolha inválida. Tente novamente.")

    def flooding_search(self, key):
        if key in self.kv_store:
            print(f"Valor na tabela local! chave: {key} valor: {self.kv_store[key]}")
        else:
            message = f"{self.address}:{self.port} {self.seq_no} {self.ttl_default} SEARCH FL {self.port} {key} 1\n"
            self.seq_no += 1
            self.seen_messages.add((f"{self.address}:{self.port}", self.seq_no))
            for neighbor in self.neighbors:
                self.send_message(message, neighbor[0], neighbor[1])

    def random_walk_search(self, key):
        if key in self.kv_store:
            print(f"Valor na tabela local! chave: {key} valor: {self.kv_store[key]}")
        else:
            next_hop = self.get_random_neighbor()
            if next_hop:
                message = f"{self.address}:{self.port} {self.seq_no} {self.ttl_default} SEARCH RW {self.port} {key} 1\n"
                self.seq_no += 1
                self.send_message(message, next_hop[0], next_hop[1])
                self.seen_messages.add((f"{self.address}:{self.port}", self.seq_no))

    def dfs_search(self, key):
        if key in self.kv_store:
            print(f"Valor na tabela local! chave: {key} valor: {self.kv_store[key]}")
        else:
            stack = list(self.neighbors.keys())
            while stack:
                neighbor = stack.pop()
                message = f"{self.address}:{self.port} {self.seq_no} {self.ttl_default} SEARCH DFS {self.port} {key} 1\n"
                self.seq_no += 1
                response = self.send_message(message, neighbor[0], neighbor[1])
                if response is not None:
                    break

    def show_statistics(self):
        print(f"Mensagens enviadas: {self.statistics['messages_sent']}")
        print(f"Mensagens recebidas: {self.statistics['messages_received']}")
        print(f"Buscas bem-sucedidas: {self.statistics['search_successful']}")
        print(f"Buscas falhadas: {self.statistics['search_failed']}")

if __name__ == "__main__":
    address, port = sys.argv[1].split(':')
    neighbors_file = sys.argv[2] if len(sys.argv) > 2 else None
    kv_file = sys.argv[3] if len(sys.argv) > 3 else None

    node = PeerNode(address, port, neighbors_file, kv_file)
    node.menu()
