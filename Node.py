import json
import bisect
import hashlib
import time
import btpeer


def main():
    n = Node()


class Node:
    def __init__(self):

        #        print("Called")

        self.data_dict = dict()
        self.data_dict_backup = dict()
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
        self.node.addhandler('NEWN', self.new_node)
        self.node.addhandler('DROP', self.drop_node)
        self.node.addhandler('PUTX', self.put_request)
        self.node.addhandler('PUTY', self.put_request_backup)
        self.node.addhandler('GETX', self.get_request)
        self.node.addhandler('CONT', self.contains_request)
        self.node.addhandler('RMVX', self.remove_request)
        self.node.addhandler('RMVY', self.remove_request_backup)
        self.node.addhandler('REQV', self.request_values)
        self.node.addhandler('REQB', self.request_backup)
        self.node.addhandler('REDV', self.request_values_duplicate)
        self.node.addhandler('REDB', self.request_backup_duplicate)

        self.create_finger_table()
        #        print(self.finger_table)

        if self.successor != self.node_id:
            (host, port) = self.node.peers[self.successor]

            result = self.node.connectandsend(host, port, 'REQV',
                                              json.dumps({'start': self.start, 'end': self.node_id}))

            taken_dict = json.loads(result[0][1])['data_dict']

            #            print("TAKEN DICTIONARY: " + str(taken_dict))

            for (key, value) in taken_dict.items():
                self.data_dict[int(key)] = value

            (host, port) = self.node.peers[self.start]

            result = self.node.connectandsend(host, port, 'REQB',
                                              json.dumps({'start': self.node_id, 'end': self.successor}))

            taken_dict = json.loads(result[0][1])['data_dict']

            #            print("TAKEN BACKUP DICTIONARY: " + str(taken_dict))

            for (key, value) in taken_dict.items():
                self.data_dict_backup[int(key)] = value
        else:
            print("successor: " + str(self.successor))

        print("DATA_DICT:" + str(self.data_dict))
        print("DATA_DICT_BACKUP:" + str(self.data_dict_backup))

        self.dict_print()
        self.node.mainloop()

    def dict_print(self):
        # print("Dictionaries of: " + str(self.node_id))
        # print ("Dict: " + str(self.data_dict))
        # print ("BackupDict: " + str(self.data_dict_backup))
        5

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.circle_size
        return key

    def put_request_backup(self, conn, msg):
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        request_val = json_msg['value']
        check = json_msg['check']
        self.data_dict_backup[request_key] = request_val

        #        print("Print after backup put")
        self.dict_print()
        return

    def remove_request_backup(self, conn, msg):
        json_msg = json.loads(msg)
        request_key = json_msg['key']

        del self.data_dict_backup[request_key]

        #        print("Print after backup remove")
        self.dict_print()
        return

    def put_request(self, conn, msg):
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        request_val = json_msg['value']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: " + str(request_key))

        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            self.data_dict[request_key] = request_val
            print("KEY IS IN ME!")
            print("key: " + str(request_key))
            print("val: " + str(request_val))
            conn.senddata('PUTX', json.dumps('success'))

            if self.start != self.node_id:
                self.node.sendtopeer(self.start, 'PUTY',
                                     json.dumps({'key': request_key, 'value': request_val, 'check': False}))
            else:
                self.data_dict_backup[request_key] = request_val
            print("Print after put")
            self.dict_print()
            return
        else:
            response = self.node.sendtopeer(to_node, 'PUTX',
                                            json.dumps({'key': request_key, 'value': request_val, 'check': True}))
            conn.senddata('PUTX', json.dumps(json.loads(response[0][1])))
            return

    def is_in_me(self, request_key):
        if (request_key in self.data_dict) \
                or (self.node_id >= request_key > self.start) \
                or (self.node_id < self.start < request_key) \
                or (self.start > self.node_id >= request_key):
            return self.node_id

        ids = self.finger_table.keys()
        ids.sort()
        to_node = None
        if request_key < ids[0]:
            to_node = self.successor
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
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: " + str(request_key))
        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            if self.data_dict.has_key(request_key):
                val = self.data_dict[request_key]
            else:
                val = None
            print("KEY IS IN ME!")
            print("key: " + str(request_key))
            print("val: " + str(val))
            conn.senddata('GETX', json.dumps(val))
            return
        else:
            response = self.node.sendtopeer(to_node, 'GETX',
                                            json.dumps({'key': request_key, 'check': True}))
            conn.senddata('GETX', json.dumps(json.loads(response[0][1])))
            return

    def contains_request(self, conn, msg):
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: " + str(request_key))
        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            val = self.data_dict.has_key(request_key)
            print("KEY IS IN ME!")
            print("key: " + str(request_key))
            print("val: " + str(val))
            conn.senddata('CONT', json.dumps(val))
            return
        else:
            response = self.node.sendtopeer(to_node, 'CONT',
                                            json.dumps({'key': request_key, 'check': True}))
            conn.senddata('CONT', json.dumps(json.loads(response[0][1])))
            return

    def remove_request(self, conn, msg):
        json_msg = json.loads(msg)
        request_key = json_msg['key']
        check = json_msg['check']

        if check is False:
            request_key = self.calculate_hash(str(request_key))
            print("I CALCULATED THE HASH AS: " + str(request_key))
        to_node = self.is_in_me(request_key)

        if to_node == self.node_id:
            val = self.data_dict.has_key(request_key)

            if val:
                del self.data_dict[request_key]
            print("KEY IS IN ME!")
            print("key: " + str(request_key))
            print("val: " + str(val))

            print (self.start != self.node_id)

            if self.start != self.node_id:
                self.node.sendtopeer(self.start, 'RMVY',
                                     json.dumps({'key': request_key, 'check': False}))
            else:
                del self.data_dict_backup[request_key]

            print(val)
            conn.senddata('RMVX', json.dumps(val))
            #            print("Print after remove")
            self.dict_print()
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

        print("Print after newnode")
        self.dict_print()

    def drop_node(self, conn, msg):
        json_response = json.loads(msg)
        drop_id = json_response['id']

        old_successor = self.successor
        old_start = self.start

        self.node.removepeer(drop_id)
        self.create_finger_table()

        if self.successor == self.node_id:
            for (key, value) in self.data_dict_backup.items():
                self.data_dict[int(key)] = value
            self.data_dict_backup.clear()
            self.data_dict_backup = dict(self.data_dict)
            print("Print after drop")
            self.dict_print()
            return

        if drop_id == old_successor:

            result = self.node.sendtopeer(self.successor, 'REDV',
                                          json.dumps({'start': old_successor, 'end': self.successor}))

            taken_dict = json.loads(result[0][1])['data_dict']

            for (key, value) in taken_dict.items():
                self.data_dict_backup[int(key)] = value

            print("Print after drop")
            self.dict_print()

            return

        if drop_id == old_start:

            result = self.node.sendtopeer(self.start, 'REDB',
                                          json.dumps({'start': self.start, 'end': old_start}))

            taken_dict = json.loads(result[0][1])['data_dict']

            for (key, value) in taken_dict.items():
                self.data_dict[int(key)] = value

            print("Print after drop")
            self.dict_print()

            return

    def request_values(self, conn, msg):
        json_req = json.loads(msg)
        key_start = json_req['start']
        key_end = json_req['end']

        to_return = dict()

        for (key, val) in self.data_dict.items():
            if key_end >= key > key_start:
                to_return[key] = val
                del self.data_dict[key]
            elif key_start > key_end >= key:
                to_return[key] = val
                del self.data_dict[key]
            elif key > key_start > key_end:
                to_return[key] = val
                del self.data_dict[key]

        conn.senddata('REQV', json.dumps({'data_dict': to_return}))

        print("Print after requested val")
        self.dict_print()

        return

    def request_backup(self, conn, msg):
        json_req = json.loads(msg)
        key_start = json_req['start']
        key_end = json_req['end']

        to_return = dict()

        for (key, val) in self.data_dict_backup.items():
            if key_end >= key > key_start:
                to_return[key] = val
                del self.data_dict_backup[key]
            elif key_start > key_end >= key:
                to_return[key] = val
                del self.data_dict_backup[key]
            elif key > key_start > key_end:
                to_return[key] = val
                del self.data_dict_backup[key]

        conn.senddata('REQB', json.dumps({'data_dict': to_return}))

        print("Print after requested back")
        self.dict_print()

        return

    def request_values_duplicate(self, conn, msg):
        json_req = json.loads(msg)
        key_start = json_req['start']
        key_end = json_req['end']

        to_return = dict()

        for (key, val) in self.data_dict.items():
            if key_end >= key > key_start:
                to_return[key] = val
            elif key_start > key_end >= key:
                to_return[key] = val
            elif key > key_start > key_end:
                to_return[key] = val

        conn.senddata('REDV', json.dumps({'data_dict': to_return}))
        return

    def request_backup_duplicate(self, conn, msg):
        json_req = json.loads(msg)
        key_start = json_req['start']
        key_end = json_req['end']

        to_return = dict()

        for (key, val) in self.data_dict_backup.items():
            if key_end >= key > key_start:
                to_return[key] = val
            elif key_start > key_end >= key:
                to_return[key] = val
            elif key > key_start > key_end:
                to_return[key] = val

        conn.senddata('REDB', json.dumps({'data_dict': to_return}))
        return

    def connect_to_server(self, ip_address, port_num):
        self.node.addpeer('server', ip_address, port_num)

        message = json.dumps({"port": self.node.serverport})

        response = self.node.sendtopeer('server', 'swrq', message)
        json_response = json.loads(response[0][1])

        id_dictionary = json_response['idDictionary']

        for (key, value) in id_dictionary.items():
            self.node.addpeer(int(key), value[0], value[1])

        self.node_id = json_response['id']
        self.node_ip = json_response['ip']
        self.circle_size = json_response['m']

    def create_finger_table(self):
        self.id_set = self.node.peers.keys()

        self.id_set = [int(x) for x in self.id_set if x is not 'server']
        self.id_set.append(self.node_id)
        self.id_set.sort()

        index = bisect.bisect_left(self.id_set, self.node_id)

        index2 = index
        if index2 - 1 < 0 or self.id_set[index2 - 1] == self.node_id:
            index2 = len(self.id_set)

        self.start = self.id_set[index2 - 1]

        if index == len(self.id_set):
            self.successor = self.node_id
        else:
            self.successor = self.id_set[(index + 1) % len(self.id_set)]

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


if __name__ == "__main__":
    main()
