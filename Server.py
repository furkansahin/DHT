import json
import hashlib
import thread
import btpeer


def main():
    Server(7)


class Server:
    def __init__(self, circle_size):
        print("Called")
        self.nodes = dict()
        self.circle_size = circle_size
        self.node = btpeer.BTPeer(0, 2222)
        print(self.node.serverhost)
        self.node.addhandler('SWRQ', self.request_handler)
        self.node.addhandler('PSET', self.client_request)
        self.node.mainloop()

    def request_handler(self, conn, msg):
        ip = conn.ip
        json_response = json.loads(msg)
        print("Json: %s" % json_response)
        port = json_response['port']

        hash = self.calculate_hash(ip)
        message = json.dumps({"ip": ip, "id": hash, "idDictionary": self.nodes, "m": self.circle_size})

        for (key, value) in self.nodes.items():
            print(key, value)
            self.node.sendtopeer(value, 'NEWN', json.dumps({"id": hash, "ip": ip}))

        print(message)
        print("Adding")
        self.node.addpeer(ip, ip, port)
        self.nodes[hash] = ip
        print("Peers%s" % self.node.getpeerids())

        conn.senddata("SWRQ", message)

    def client_request(self, conn, msg):
        self.node.checklivepeers()
        keys = self.node.peers.keys()

        conn.senddata('PSET', json.dumps(keys))

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.circle_size
        return key

    def send_new_node(self, ip_to_sent, ip_to_be_sent, key):
        message = json.dumps({"ip": ip_to_be_sent, "id": key})
        # send(message, ip_to_sent)


if __name__ == "__main__":
    main()
