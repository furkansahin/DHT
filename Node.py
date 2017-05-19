import json
import bisect
import hashlib

import time

import btpeer


def main():
    n = Node()


class Node:
    def __init__(self):

        print("Called")

        self.data_dict = dict()
        self.finger_table = dict()
        self.id_set = set()
        self.node_id = None
        self.node_ip = None
        self.server_connection = None
        self.successor = None
        self.circle_size = None
        self.node = btpeer.BTPeer(0, 2223)
        self.start = None

        self.connect_to_server('207.154.219.184', '2222')
        self.node.addhandler('KQUE', self.incoming_query)
        self.node.addhandler('NEWN', self.new_node)
        self.node.addhandler('PUTX', self.put_request)
        self.node.addhandler('GETX', self.get_request)
        self.node.addhandler('CONT', self.contains_request)
        self.node.addhandler('RMVX', self.remove_request)

        self.create_finger_table()
        print(self.finger_table)
        self.node.mainloop()

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.circle_size
        return key

    def put_request(self, conn, msg):
        time.sleep(1)
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        request_val = json_msg['value']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: %s", request_key)

        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            self.data_dict[request_key] = request_val
            print("KEY IS IN ME!")
            conn.senddata('PUTX', json.dumps('success'))
            return
        else:
            response = self.node.sendtopeer(to_node, 'PUTX',
                                            json.dumps({'key': request_key, 'value': request_val, 'check': True}))
            conn.senddata('PUTX', json.dumps(json.loads(response[0][1])))
            return

    def is_in_me(self,request_key):
        if (request_key in self.data_dict) \
                or (self.node_id >= request_key > self.start) \
                or (self.node_id < self.start < request_key) \
                or (self.start > self.node_id >= request_key):
            return self.node_id

            #        self.finger_table['NONE'] = self.node_id
        ids = self.finger_table.keys()
        ids.sort()
        to_node = None
        if request_key < ids[0]:
            to_node = self.successor
            print("SENDING TO SUCCESSOR %d", self.successor)
        else:
            for ind in range(len(ids)):
                if ids[ind] > request_key:
                    to_node = self.finger_table[ids[ind - 1]]
                    break

        if to_node is None:
            # find the largest key's value
            to_node = self.finger_table[ids[len(ids) - 1]]

        return to_node

    def get_request(self, conn, msg):
        time.sleep(1)
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: %s", request_key)

        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            if self.data_dict.has_key(request_key):
                val = self.data_dict[request_key]
            else:
                val = None
            print("KEY IS IN ME!")
            conn.senddata('GETX', json.dumps(val))
            return
        else:
            response = self.node.sendtopeer(to_node, 'GETX',
                                            json.dumps({'key': request_key, 'check': True}))
            conn.senddata('GETX', json.dumps(json.loads(response[0][1])))
            return

    def contains_request(self, conn, msg):
        time.sleep(1)
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: %s", request_key)

        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            val = self.data_dict.has_key(request_key)
            print("KEY IS IN ME!")
            conn.senddata('CONT', json.dumps(val))
            return
        else:
            response = self.node.sendtopeer(to_node, 'CONT',
                                            json.dumps({'key': request_key, 'check': True}))
            conn.senddata('CONT', json.dumps(json.loads(response[0][1])))
            return

    def remove_request(self, conn, msg):
        time.sleep(1)
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: %s", request_key)

        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            val = self.data_dict.has_key(request_key)

            if val:
                del self.data_dict[request_key]
            print("KEY IS IN ME!")
            conn.senddata('RMVX', json.dumps(val))
            return
        else:
            response = self.node.sendtopeer(to_node, 'RMVX',
                                            json.dumps({'key': request_key, 'check': True}))
            conn.senddata('RMVX', json.dumps(json.loads(response[0][1])))
            return

    def new_node(self, conn, msg):
        json_response = json.loads(msg)
        new_id = json_response['id']
        new_ip = json_response['ip']
        new_port = json_response['port']
        self.node.addpeer(new_id, new_ip, new_port)
        self.create_finger_table()
        print(self.node.peers)
        print(self.finger_table)

    def connect_to_server(self, ip_address, port_num):
        self.node.addpeer('server', ip_address, port_num)

        message = json.dumps({"port": self.node.serverport})

        response = self.node.sendtopeer('server', 'swrq', message)
        print(response)
        json_response = json.loads(response[0][1])

        id_dictionary = json_response['idDictionary']

        print(id_dictionary)
        for (key, value) in id_dictionary.items():
            self.node.addpeer(key, value[0], value[1])

        self.node_id = json_response['id']
        self.node_ip = json_response['ip']
        self.circle_size = json_response['m']

    def create_finger_table(self):
        self.id_set = self.node.peers.keys()

        # TODO this integer conversion may lead a problem!
        self.id_set = [int(x) for x in self.id_set if x is not 'server']
        self.id_set.append(self.node_id)
        self.id_set.sort()

        print(self.id_set)

        index = bisect.bisect_left(self.id_set, self.node_id)

        index2 = index
        if index2 - 1 < 0 or self.id_set[index2 - 1] == self.node_id:
            index2 = len(self.id_set)

        self.start = self.id_set[index2 - 1]

        print("START: " + str(self.start))

        print(index)
        if index == len(self.id_set):
            self.successor = self.node_id
        else:
            self.successor = self.id_set[index]
        for i in range(self.circle_size):
            num = (self.node_id + 2 ** i) % 2 ** self.circle_size
            id_found = bisect.bisect_right(self.id_set, num)

            if len(self.id_set) != 0:
                id_found = id_found % len(self.id_set)

            if len(self.id_set) == 1:
                self.finger_table[num] = self.node_id
            elif id_found == len(self.id_set):
                raise ValueError('No item found with key at or above: %r' % (id_found,))
            else:
                self.finger_table[num] = self.id_set[id_found]

    def pass_request(self, to_node, request_key, request_ip, request_port, sender_id):
        return

    def incoming_query(self, conn, msg):
        json_request = json.loads(msg[0][1])
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
        #     key = int(key, 16) % 2**circle_size
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
       #         self.send_response(request_ip, request_key, self.data_dict[request_key])
                return
            if sender_id is not None and self.node_id > request_key > sender_id:
      #          self.send_response(request_ip, request_key, None)
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
