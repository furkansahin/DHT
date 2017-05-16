import json
import bisect
import hashlib
import btpeer

def main():
    Node()


class Node:
    def __init__(self):

        print "Called"
        jsonObj = json.loads(
            "{\"idDictionary\": {\"16\": \"127.0.0.3\", \"32\": \"127.0.0.2\", \"45\": \"127.0.0.4\",\"96\": \"127.0.0.5\",\"112\": \"127.0.0.6\"},\"id\": 80,\"m\": 7, \"ip\":\"127.0.0.1\"}")
        id_dictionary = jsonObj['idDictionary']

        self.data_dict = dict()
        self.id_dictionary = jsonObj['idDictionary']
        self.finger_table = dict()
        self.id_set = set()
        self.node_id = jsonObj['id']
        self.node_ip = jsonObj['ip']
        self.server_connection = None
        self.successor = None
        self.system_m = jsonObj['m']
        self.node = btpeer.BTPeer(0,2223)

        self.connect_to_server('207.154.219.184','2222')


    def connect_to_server(self,ip_address, port_num):

        self.node.addpeer('server',ip_address,port_num)

        response = self.n
        # Server connection code will be called in here

        # TODO we need a server_connection which has a connect method sending the
        # provided message to the server and takes the response
        return response

    def create_finger_table(self):
        self.id_set = self.id_dictionary.keys()
        self.id_set = [int(x) for x in self.id_set]
        self.id_set.sort()

        self.successor = self.id_set[bisect.bisect_left(self.id_set, self.node_id + 1)]

        for i in range(self.system_m):
            num = (self.node_id + 2 ** i) % 2 ** self.system_m
            id_found = bisect.bisect_left(self.id_set, num)
            if id_found == len(self.id_set):
                raise ValueError('No item found with key at or above: %r' % (id_found,))
            self.finger_table[num] = self.id_set[id_found]

    def send_response(self, request_ip, request_key, data):
        message = json.dumps({'request_key': request_key, 'data': data})
        self.server_connection.send_message(message, request_ip)

    def pass_request(self, to_node, request_key, request_ip, request_port, sender_id):
        return

    def incoming_query(self, request):
        json_request = json.loads(request)
        request_ip = json_request['ip']
        request_port = json_request['port']
        request_key = json_request['key']
        request_val = None
        if 'request_val' in json_request:
            request_val = json_request['val']
        sender_id = None
        if 'sender_id' in json_request:
            sender_id = json_request['sender_id']
        # if sender_id is None:
        #     key = hashlib.sha1(request_ip + request_port).hexdigest()
        #     key = int(key, 16) % 2**system_m
        request_key = int(request_key)

        if request_val is not None:
            ids = self.finger_table.values()
            ids.sort()
            index = bisect.bisect(ids, self.node_id) - 1
            if index >= 0 and self.node_id > request_key > ids(index):
                # do the stuff
                5
        else:
            if request_key in self.data_dict:
                self.send_response(request_ip, request_key, self.data_dict[request_key])
                return
            if sender_id is not None and self.node_id > request_key > sender_id:
                self.send_response(request_ip, request_key, None)
                return

            sorted_values = self.finger_table.values()
            sorted_values.sort()
            index = bisect.bisect(sorted_values, request_key) - 1
            if index < 0:
                self.pass_request(self.successor, request_key, request_ip, request_port, self.node_id)
                return

        to_node = sorted_values[index]
        self.pass_request(to_node, request_key, request_ip, request_port, self.node_id)

if __name__ == "__main__":
    main()
