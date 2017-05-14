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
            port = "2222"
            hash = self.calculate_hash(ip + port)
            self.send_response(ip, port, hash)

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.system_m
        return key

    def send_response(self, ip, port, hash):
        message = json.dumps({"ip": ip, "id": hash, "idDictionary": self.nodes, "m": self.system_m})
        # send(message, ip, port)