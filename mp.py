import multiprocessing as mp
import numpy as np
import time


# Prepare data
np.random.RandomState(100)
arr = np.random.randint(0, 10, size=[200000, 1000])
data = arr.tolist()
data[:5]

results = []

def howmany_within_range(row, minimum, maximum):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return count

def howmany_within_range2(i, row, minimum, maximum):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return (i, count)

def collect_result(result):
    global results
    results.append(result)

def howmany_within_range_rowonly(row, minimum=4, maximum=8):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return count

def format_elapsed_time(start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)

def without_parallelization(data):
    time_start = time.time()
    results = []
    for row in data:
        results.append(howmany_within_range(row, minimum=4, maximum=8))
    time_end = time.time()
    print("Without parallelization: " + format_elapsed_time(time_start, time_end))

# using pool apply
def using_pool_apply(data):
    pool = mp.Pool(mp.cpu_count())
    time_start = time.time()
    results = [pool.apply(howmany_within_range, args=(row, 4, 8)) for row in data]
    pool.close()
    time_end = time.time()
    print("Pool apply: " + format_elapsed_time(time_start, time_end))
    return results

# using pool apply
def using_pool_map(data):
    pool = mp.Pool(mp.cpu_count())
    time_start = time.time()
    results = pool.map(howmany_within_range_rowonly, [row for row in data])
    pool.close()
    time_end = time.time()
    print("Pool map: " + format_elapsed_time(time_start, time_end))
    return results

def using_pool_starmap(data):
    pool = mp.Pool(mp.cpu_count())
    time_start = time.time()
    results = pool.starmap(howmany_within_range, [(row, 4, 8) for row in data])
    pool.close()
    time_end = time.time()
    print("Pool starmap: " + format_elapsed_time(time_start, time_end))
    return results

# async
def using_pool_apply_async(data):
    pool = mp.Pool(mp.cpu_count())
    time_start = time.time()
    for i, row in enumerate(data):
        pool.apply_async(howmany_within_range2, args=(i, row, 4, 8), callback=collect_result)
    pool.close()
    pool.join()
    time_end = time.time()
    print("Pool apply async: " + format_elapsed_time(time_start, time_end))
    return results

def using_pool_starmap_async(data):
    pool = mp.Pool(mp.cpu_count())
    time_start = time.time()
    results = []
    results = pool.starmap_async(howmany_within_range2, [(i, row, 4, 8) for i, row in enumerate(data)]).get()
    pool.close()
    time_end = time.time()
    print("Pool starmap async: " + format_elapsed_time(time_start, time_end))
    return results




without_parallelization(data)
# using_pool_apply(data)
using_pool_map(data)
using_pool_starmap(data)
# using_pool_apply_async(data)
using_pool_starmap_async(data)

# print(results)
#> [3, 1, 4, 4, 4, 2, 1, 1, 3, 3])