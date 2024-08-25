import os
import re
from multiprocessing import Pool, cpu_count
from urllib.parse import urlsplit
from urllib.parse import urlparse
# 计时器
def timer(func):
    def func_wrapper(*args, **kwargs):
        from time import time
        time_start = time()
        result = func(*args, **kwargs)
        time_end = time()
        time_spend = time_end - time_start
        print('%s cost time: %.3f s' % (func.__name__, time_spend))
        return result

    return func_wrapper




# 多余10层的目录 -> 没必要过滤
def filter_deep_urls(url):
    parts = urlsplit(url)
    path_parts = parts.path.rstrip('/').split('/')
    if len([part for part in path_parts if part]) <= 10: # 10层以下都留着
        return True
    return None


# 重复目录
def clean_repeated_directories(url):
    # 使用正则表达式匹配连续重复的目录名称
    pattern = r'/(.+?)/(?=.*?\1/)'  # 匹配重复目录前的部分

    # 使用递归替换来去除重复的目录
    def replace_repeated(match):
        return '/' + match.group(1)

    # 使用递归函数来处理嵌套的重复
    while re.search(pattern, url):
        url = re.sub(pattern, replace_repeated, url)

    return url


# pattern为rootdomains的正则, 只匹配子域名, 需要从主线程传递给子线程
def filter_url(line, pattern):
    url = line.strip()
    parsed_uri = urlparse(url)
    domain = '{uri.netloc}'.format(uri=parsed_uri)
    if pattern.search(domain):
        return url
    return None

def worker(chunk, output_file, pattern):
    results = set()
    for line in chunk:
        result = filter_url(line, pattern)
        if result is None:
            continue
        # if filter_deep_urls(result) is None:
        #     continue
        # result = clean_repeated_directories(result)
        # print(result)  # 不需要输出
        results.add(result)
    with open(output_file, 'a') as f:
        for url in results:
            f.write(url + '\n')

@timer
def main():
    with open('rootdomains.txt', 'r') as f:
        rootdomains_pattern = '|'.join(f.read().split())
    pattern = re.compile(rootdomains_pattern)

    # 读取大文件并将其分割成多个小块
    chunk_size = 1000000  # 可以根据实际情况调整, 10000size为1m, 单个url长度为100字节, 则一个线程处理100m
    temp_output_files = []
    counter = 0

    # 创建临时目录
    temp_dir = 'pureurl_temp_directory'
    os.makedirs(temp_dir, exist_ok=True)

    # 使用多进程处理
    num_processes = int(cpu_count() * 1.5)  # 使用所有可用的CPU核心

    with open('urls.txt', 'r') as infile, Pool(processes=num_processes) as pool:
        while True:
            lines = infile.readlines(chunk_size)
            if not lines:
                break
            temp_output = os.path.join(temp_dir, f'temp_output_{counter}.txt')
            temp_output_files.append(temp_output)
            counter += 1

            chunk = lines
            output_file = temp_output
            # 将当前块和输出文件名添加到任务队列
            pool.starmap(worker, [(chunk, output_file, pattern)])

    # 合并所有临时文件到最终输出文件
    with open('pureurls.txt', 'w') as outfile:
        for temp_file in temp_output_files:
            with open(temp_file, 'r') as f:
                outfile.write(f.read())

            # 清理临时文件
            os.remove(temp_file)

    # 清理临时目录
    os.rmdir(temp_dir)


if __name__ == '__main__':
    main()