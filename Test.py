import random
import time
import Client


def main():
    c = Client()

    put_dict = dict()
    total_time = 0
    total_local_time = 0
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

                c.put(key, val)
                tf = time.time()

                total_local_time = total_local_time + ts - ts_local
                total_time = total_time + tf - ts
        elif parts[0] == 'GET':
            for (key, val) in put_dict.items():
                if val == int(c.get(key)):
                    print("SUCCESS")
                else:
                    print("FAILED!")
                    break
        elif parts[0] == 'CONTAINS':
            print('Value is ' + str(c.contains(int(parts[1]))))
        elif parts[0] == 'REMOVE':
            print('Value is ' + str(c.remove(int(parts[1]))))

        print("TOTAL Put time: " + str(total_time))
        print("TOTAL local Put time: " + str(total_time))


if __name__ == "__main__":
    main()
