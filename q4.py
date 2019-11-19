import multiprocessing
import queue
import json
import sys


# Useful for debugging concurrency issues.
def log(msg):
    print(sys.stderr, multiprocessing.current_process().name, msg)


# Each worker reads the json file, computes a sum and a count for the target
# field, then stores both in the output queue.
def task(in_q, out_q):
    age_sum = 0
    age_cnt = 0

    try:
        while (True):
            f = in_q.get(block=False)
            lines=[]
            with open(f) as json_file:
                for line in json_file:
                    data = json.loads(line)
                    if data["provider"]!="GitHub":
                        lines.append(data)
    except queue.Empty:
        pass  #print "Done processing"

    out_q.put(lines)


def main_task(cnt):
    nprocs = int(cnt)

    q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()

    # Enqueue filenames to be processed in parallel.
    for i in range(100):
        f = "data/json/mybinder%03d.json" % (i)
        q.put(f)

    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(target=task, args=(q, out_q))
        p.start()
        procs.append(p)

    # Main task takes partial results and computes the final average.

    lines=[]
    for p in procs:
        lines.extend(out_q.get())
    with open('q4.json', 'w', encoding='utf-8') as f:
        json.dump(lines, f, ensure_ascii=False, indent=4)



# python3 queue_test.py <n_cores>
if __name__ == "__main__":
    # print ('workers',sys.argv[1])
    # print(sys.argv)
    main_task(sys.argv[1])
