from time import time, sleep

from tqdm import tqdm

from misc_utils.processing_utils import process_with_threadpool_backpressure


def fun(x):
    sleep(0.1)
    return x


if __name__ == "__main__":

    start = time()
    list(tqdm(process_with_threadpool_backpressure(fun, range(100), max_workers=3)))
    # list(tqdm(map(fun,range(100))))
    print(f"took: {time()-start}")
