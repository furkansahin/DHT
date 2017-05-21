import random
import time
import Client


def main():

    put_dict = dict()
    total_time = 0
    total_local_time = 0
    total_local_get_time = 0
    total_local_contains_time = 0
    total_local_remove_time = 0

    while True:
        cmd = raw_input("COMMAND: ")
        parts = cmd.split(" ")

        if parts[0] == 'PUT':
            for i in range(int(parts[1])):
                key = random.randint(0, 2 ** int(parts[2]))
                val = random.randint(0, 2 ** int(parts[2]))

                ts_local = time.time()
                put_dict[key] = val
                ts = time.time()
                total_local_time = total_local_time + ts - ts_local

        elif parts[0] == 'GET':
            ts = time.time()
            for (key, val) in put_dict.items():
                if val != None:
                    print("SUCCESS")
                else:
                    print("FAILED!")
                    break
            total_local_get_time += time.time() - ts
        elif parts[0] == 'CONTAINS':
            keys = put_dict.keys()

            ts = time.time()
            for key in keys:
                if key in put_dict:
                    print("SUCCESS")
                else:
                    print("FAILED!")
                    break

            total_local_contains_time += time.time() - ts
        elif parts[0] == 'REMOVE':
            keys = put_dict.keys()

            ts = time.time()
            for key in keys:
                del put_dict[key]
            total_local_remove_time += time.time() - ts


        print("TOTAL Put time: " + str(total_local_time))
        print("TOTAL Get time: " + str(total_local_get_time))
        print("TOTAL Contains time: " + str(total_local_contains_time))
        print("TOTAL Remove Put time: " + str(total_local_remove_time))

        total_local_time = 0
        total_local_get_time = 0
        total_local_contains_time = 0
        total_local_remove_time = 0

if __name__ == "__main__":
    main()
