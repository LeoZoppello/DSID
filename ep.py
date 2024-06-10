import socket
import sys
import threading
import time

class Node:
    def __init__(self, address, port, neighbors_file=None, kv_file=None):
        self.address = address
        self.port = port
        self.neighbors = []
        self.kv_store = {}
        self.seqno = 1
        self.ttl_default = 100
        self.messages_seen = set()
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'messages_forwarded': 0,
        }
        self.load_neighbors(neighbors_file)
        self.load_kv_store(kv_file)
        self.start_server()

    def load_neighbors(self, neighbors_file):
        if neighbors_file:
            with open(neighbors_file, 'r') as file:
                for line in file:
                    neighbor = line.strip()
                    self.neighbors.append(neighbor)
                    print(f"Tentando adicionar vizinho {neighbor}")
                    self.send_hello(neighbor)

    def load_kv_store(self, kv_file):
        if kv_file:
            with open(kv_file, 'r') as file:
                for line in file:
                    key, value = line.strip().split()
                    self.kv_store[key] = value
                    print(f"Adicionando par ({key}, {value}) na tabela local")

    def send_hello(self, neighbor):
        try:
            neighbor_address, neighbor_port = neighbor.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((neighbor_address, int(neighbor_port)))
                message = f"{self.address}:{self.port} {self.seqno} 1 HELLO\n"
                s.sendall(message.encode())
                print(f"Encaminhando mensagem {message.strip()} para {neighbor}")
                response = s.recv(1024).decode().strip()
                if response == "HELLO_OK":
                    print(f"Envio feito com sucesso: {message.strip()}")
                else:
                    print("Erro ao conectar!")
        except Exception as e:
            print(f"Erro ao conectar: {e}")

    def start_server(self):
        server = threading.Thread(target=self.run_server)
        server.start()

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.address, self.port))
            s.listen()
            print(f"Servidor criado: {self.address}:{self.port}")
            while True:
                conn, addr = s.accept()
                client_handler = threading.Thread(target=self.handle_client, args=(conn,))
                client_handler.start()

    def handle_client(self, conn):
        with conn:
            data = conn.recv(1024).decode().strip()
            print(f"Mensagem recebida: {data}")
            self.stats['messages_received'] += 1
            parts = data.split()
            origin = parts[0]
            seqno = parts[1]
            ttl = int(parts[2])
            operation = parts[3]
            message_id = (origin, seqno)

            if message_id in self.messages_seen:
                print("Mensagem repetida!")
                return

            self.messages_seen.add(message_id)

            if operation == "HELLO":
                self.handle_hello(conn, origin)
            elif operation == "SEARCH":
                mode = parts[4]
                last_hop_port = parts[5]
                key = parts[6]
                hop_count = int(parts[7])
                self.handle_search(origin, seqno, ttl, mode, last_hop_port, key, hop_count)

    def handle_hello(self, conn, origin):
        if origin not in self.neighbors:
            self.neighbors.append(origin)
            print(f"Adicionando vizinho na tabela: {origin}")
        else:
            print(f"Vizinho já está na tabela: {origin}")
        conn.sendall(b"HELLO_OK\n")

    def handle_search(self, origin, seqno, ttl, mode, last_hop_port, key, hop_count):
        if key in self.kv_store:
            value = self.kv_store[key]
            response = f"{self.address}:{self.port} {self.seqno} 1 VAL {mode} {key} {value} {hop_count}\n"
            self.send_message(origin, response)
            return

        ttl -= 1
        if ttl == 0:
            print("TTL igual a zero, descartando mensagem")
            return

        hop_count += 1
        message = f"{origin} {seqno} {ttl} SEARCH {mode} {self.port} {key} {hop_count}\n"
        for neighbor in self.neighbors:
            if neighbor.endswith(f":{last_hop_port}"):
                continue
            self.send_message(neighbor, message)

    def handle_bye(self, origin):
        if origin in self.neighbors:
            self.neighbors.remove(origin)
            print(f"Removendo vizinho da tabela: {origin}")

    def send_message(self, destination, message):
        try:
            dest_address, dest_port = destination.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((dest_address, int(dest_port)))
                s.sendall(message.encode())
                print(f"Encaminhando mensagem {message.strip()} para {destination}")
                response = s.recv(1024).decode().strip()
                if response.endswith("_OK"):
                    print(f"Envio feito com sucesso: {message.strip()}")
                    self.stats['messages_sent'] += 1
                else:
                    print("Erro ao enviar mensagem!")
        except Exception as e:
            print(f"Erro ao conectar: {e}")

    def send_bye_to_all(self):
        for neighbor in self.neighbors:
            message = f"{self.address}:{self.port} {self.seqno} 1 BYE\n"
            self.send_message(neighbor, message)
            self.seqno += 1

    def menu(self):
        while True:
            print("Escolha o comando\n[0] Listar vizinhos\n[1] HELLO\n[2] SEARCH (flooding)\n[3] SEARCH (random walk)\n[4] SEARCH (busca em profundidade)\n[5] Estatísticas\n[6] Alterar valor padrão de TTL\n[9] Sair")
            choice = input().strip()
            if choice == '0':
                self.list_neighbors()
            elif choice == '1':
                self.send_hello_to_all()
            elif choice == '2':
                self.search("flooding")
            elif choice == '3':
                self.search("random walk")
            elif choice == '4':
                self.search("depth")
            elif choice == '5':
                self.print_statistics()
            elif choice == '6':
                self.change_ttl()
            elif choice == '9':
                print("Você escolheu 9")
                print("Saindo...")
                self.send_bye_to_all()
                break

    def list_neighbors(self):
        for neighbor in self.neighbors:
            print(neighbor)

    def send_hello_to_all(self):
        for neighbor in self.neighbors:
            self.send_hello(neighbor)

    def search(self, mode):
        key = input("Digite a chave a ser buscada: ").strip()
        if key in self.kv_store:
            print(f"Valor na tabela local! chave: {key} valor: {self.kv_store[key]}")
        else:
            self.initiate_search(key, mode)

    def initiate_search(self, key, mode):
        message = f"{self.address}:{self.port} {self.seqno} {self.ttl_default} SEARCH {mode} {self.port} {key} 1\n"
        self.messages_seen.add((f"{self.address}:{self.port}", str(self.seqno)))
        for neighbor in self.neighbors:
            self.send_message(neighbor, message)
        self.seqno += 1

    def print_statistics(self):
        print("Estatísticas:")
        print(f"Mensagens enviadas: {self.stats['messages_sent']}")
        print(f"Mensagens recebidas: {self.stats['messages_received']}")
        print(f"Mensagens encaminhadas: {self.stats['messages_forwarded']}")

    def change_ttl(self):
        self.ttl_default = int(input("Digite o novo valor de TTL: ").strip())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Modo de uso: python ep.py <endereço>:<porta> [vizinhos.txt] [lista_chave_valor.txt]")
        sys.exit(1)

    address, port = sys.argv[1].split(':')
    neighbors_file = sys.argv[2] if len(sys.argv) > 2 else None
    kv_file = sys.argv[3] if len(sys.argv) > 3 else None

    node = Node(address, int(port), neighbors_file, kv_file)
    node.menu()
