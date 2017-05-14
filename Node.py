from pyp2p.net import *
import json
import bisect
import hashlib

data_dict = dict()
id_dictionary = dict()
finger_table = dict()
id_set = set()
node_id = None
node_ip = None
server_connection = None
successor = None
system_m = None


def connect_to_server(ip_address, port_num):
    # Server connection code will be called in here

    # TODO we need a server_connection which has a connect method sending the
    # provided message to the server and takes the response

    # server_connection = new Connection()
    message = json.dumps({'ipAddress': ip_address, 'portNum': port_num})
    response = server_connection.connect(message)
    return response


def create_finger_table():
    id_set = id_dictionary.keys()
    id_set = [int(x) for x in id_set]
    id_set.sort()

    successor = bisect.bisect_left(id_set, node_id + 1)

    for i in range(system_m):
        num = (id + 2**i) % 2**system_m
        id_found = bisect.bisect_left(id_set, num)
        if id_found == len(id_set):
            raise ValueError('No item found with key at or above: %r' % (id_found,))
        finger_table[num] = id_set[id_found]


def send_response(request_ip, request_key, data):
    message = json.dumps({'request_key': request_key, 'data': data})
    server_connection.send_message(message, request_ip)


def pass_request(to_node, request_key, request_ip, request_port, sender_id):
    #TODO we are going to pass the request to the other node
    5


def incoming_query(request):
    json_request = json.loads(request)
    request_ip = json_request['ip']
    request_port = json_request['port']
    request_key = json_request['key']
    sender_id = json_request['sender_id']
    # if sender_id is None:
    #     key = hashlib.sha1(request_ip + request_port).hexdigest()
    #     key = int(key, 16) % 2**system_m
    if request_key < node_id:
        if request_key in data_dict:
            send_response(request_ip, request_key, data_dict[request_key])
            return
        if sender_id is not None and request_key > sender_id:
            send_response(request_ip, request_key, None)
            return
        elif sender_id is None:
            to_node = finger_table(bisect.bisect_right(id_set, request_key))
            if to_node == node_id:
                to_node == finger_table(node_id + 2**(system_m-1) % 2**system_m )
            pass_request(to_node, request_key, request_ip, request_port, sender_id)


def main():


    ipAddr = sys.argv(0)
    portNum = sys.argv(1)

    jsonObj = json.loads(connect_to_server(ipAddress=ipAddr, portNum=portNum))
#    jsonObj = json.loads("{\"idDictionary\": {\"16\": \"127.0.0.1\", \"32\": \"127.0.0.2\", \"45\": \"127.0.0.3\",\"80\": \"127.0.0.4\",\"96\": \"127.0.0.5\",\"112\": \"127.0.0.6\"},\"id\": 80,\"m\": 7}")
    id_dictionary = jsonObj['idDictionary']
    node_id = jsonObj['id']
    system_m = jsonObj['m']
    node_ip = jsonObj['ip']
    finger_table = create_finger_table()


if __name__=="__main__":
    main()

