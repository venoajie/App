import time
from multiprocessing import Process


def sum_to_num(final_num: int) -> int:
    start = time.monotonic()

    result = 0
    for i in range(0, final_num+1, 1):
        result += i

    print(f"The method with {final_num} completed in {time.monotonic() - start:.2f} second(s).")
    return result

def main():
    # We initialize the two processes with two parameters, from largest to smallest
    process_a = Process(target=sum_to_num, args=(200_000_000,))
    process_b = Process(target=sum_to_num, args=(50_000_000,))

    # And then let them start executing
    process_a.start()
    process_b.start()

    # Note that the join method is blocking and gets results sequentially
    start_a = time.monotonic()
    process_a.join()
    print(f"Process_a completed in {time.monotonic() - start_a:.2f} seconds")

    # Because when we wait process_a for join. The process_b has joined already.
    # so the time counter is 0 seconds.
    start_b = time.monotonic()
    process_b.join()
    print(f"Process_b completed in {time.monotonic() - start_b:.2f} seconds")