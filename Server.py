import json
import hashlib
import thread


class Server:
    def __init__(self, system_m):
        self.nodes = dict()
        self.system_m = system_m
        try:
            thread.start_new_thread(self.accept_requests)
        except:
            print("Error: unable to start thread")

    def accept_requests(self):
        while True:
            # check for requests
            request = "incoming request"
            json_request = json.loads(request)
            ip = "127.0.0.1"
            key = self.calculate_hash(ip)
            self.send_response(ip, key)

            for _ip in self.nodes.keys():
                self.send_new_node(_ip, ip, key)

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.system_m
        return key

    def send_response(self, ip, key):
        message = json.dumps({"ip": ip, "id": key, "idDictionary": self.nodes, "m": self.system_m})
        # send(message, ip, port)

    def send_new_node(self, ip_to_sent, ip_to_be_sent, key):
        message = json.dumps({"ip": ip_to_be_sent, "id": key})
        # send(message, ip_to_sent)
