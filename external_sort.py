import os, sys, array
from concurrent.futures import ProcessPoolExecutor

# 处理单个数据块：读取 -> 排序 -> 写出临时文件
def sort_and_flush_block(params):
    file_path, block_id, chunk_size, start_offset = params
    block_data = array.array('i')
    try:
        with open(file_path, 'rb') as f:
            f.seek(start_offset * 4)
            raw_bytes = f.read(chunk_size * 4)
            if not raw_bytes:
                return None
            block_data.frombytes(raw_bytes)
    except Exception as e:
        print(f"读取块 {block_id} 时出错: {e}")
        return None

    sorted_data = sorted(block_data)
    block_data = array.array('i', sorted_data)

    tmp_filename = f"tmp_{block_id:06d}.dat"
    try:
        with open(tmp_filename, 'wb') as out:
            out.write(block_data.tobytes())
    except Exception as e:
        print(f"写临时文件 {tmp_filename} 失败: {e}")
        return None

    return (block_id, tmp_filename, len(block_data))

def initial_distribute_sort(filename, memory_limit):
    total_bytes = os.path.getsize(filename)
    if total_bytes % 4 != 0:
        raise ValueError("文件长度必须能被4整除（每个int占4字节）")
    total_numbers = total_bytes // 4
    if total_numbers == 0:
        return [], [], 0

    num_blocks = (total_numbers + memory_limit - 1) // memory_limit
    task_list = [(filename, i, memory_limit, i * memory_limit) for i in range(num_blocks)]

    print(f"共 {total_numbers} 个整数，分为 {num_blocks} 块，每块上限 {memory_limit}")
    with ProcessPoolExecutor() as pool:
        results = list(pool.map(sort_and_flush_block, task_list))

    successful = [r for r in results if r is not None]
    successful.sort(key=lambda x: x[0])   # 按块号保持顺序
    temp_files = [x[1] for x in successful]
    block_sizes = [x[2] for x in successful]
    return temp_files, block_sizes, total_numbers

# 两路归并两个有序文件
def merge_pair_to_output(file_a, file_b, output_path, buffer_size):
    a_buffer = array.array('i')
    b_buffer = array.array('i')
    with open(file_a, 'rb') as fa, open(file_b, 'rb') as fb, \
         open(output_path, 'wb') as fout:
        # 预读第一批数据
        chunk = fa.read(buffer_size * 4)
        if chunk: a_buffer.frombytes(chunk)
        chunk = fb.read(buffer_size * 4)
        if chunk: b_buffer.frombytes(chunk)

        a_pos = 0
        b_pos = 0
        out_chunk = array.array('i')

        while True:
            # 补充 a 缓冲区
            if a_pos >= len(a_buffer):
                chunk = fa.read(buffer_size * 4)
                if chunk:
                    a_buffer = array.array('i'); a_buffer.frombytes(chunk); a_pos = 0
                else:
                    a_buffer = array.array('i'); a_pos = 0

            # 补充 b 缓冲区
            if b_pos >= len(b_buffer):
                chunk = fb.read(buffer_size * 4)
                if chunk:
                    b_buffer = array.array('i'); b_buffer.frombytes(chunk); b_pos = 0
                else:
                    b_buffer = array.array('i'); b_pos = 0

            # 两个缓冲区都空了就结束
            if len(a_buffer) == 0 and len(b_buffer) == 0:
                break

            a_val = a_buffer[a_pos] if a_pos < len(a_buffer) else None
            b_val = b_buffer[b_pos] if b_pos < len(b_buffer) else None

            if a_val is not None and (b_val is None or a_val <= b_val):
                out_chunk.append(a_val)
                a_pos += 1
            elif b_val is not None:
                out_chunk.append(b_val)
                b_pos += 1

            if len(out_chunk) >= buffer_size:
                fout.write(out_chunk.tobytes())
                out_chunk = array.array('i')

        if len(out_chunk) > 0:
            fout.write(out_chunk.tobytes())

# 用于并行调用的包装函数
def merge_and_clean(task_pack):
    f1, f2, out, buf = task_pack
    merge_pair_to_output(f1, f2, out, buf)
    os.remove(f1)
    os.remove(f2)
    return out

def multi_pass_merge(file_list, memory_limit):
    if not file_list:
        return None
    runs = file_list[:]
    workers = os.cpu_count() or 4
    # 每个任务能用的缓冲大小
    buf = max(1, (memory_limit // workers) // 4)
    print(f"归并阶段，进程数约 {workers}，每文件缓冲 {buf} 个整数")

    round_index = 0
    while len(runs) > 1:
        print(f"第 {round_index} 轮归并，当前文件数 {len(runs)}")
        tasks = []
        for i in range(0, len(runs), 2):
            if i + 1 < len(runs):
                out_name = f"merge_{round_index}_{i//2}.dat"
                tasks.append((runs[i], runs[i+1], out_name, buf))
            # 奇数个文件时，最后一个直接保留到下一轮

        with ProcessPoolExecutor() as pool:
            futures = [pool.submit(merge_and_clean, t) for t in tasks]
            new_runs = [f.result() for f in futures]

        if len(runs) % 2 == 1:
            new_runs.append(runs[-1])

        runs = new_runs
        round_index += 1

    return runs[0]

def external_sort(input_path, M):
    out_dir = os.path.dirname(input_path) or '.'
    base_name = os.path.basename(input_path)
    sorted_path = os.path.join(out_dir, f"sorted_{base_name}")

    temp_files, _, total = initial_distribute_sort(input_path, M)
    if total == 0:
        open(sorted_path, 'wb').close()
        print("输入文件为空，直接创建空输出")
        return

    final_file = multi_pass_merge(temp_files, M)
    os.rename(final_file, sorted_path)
    print(f"排序完成 -> {sorted_path}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python external_sort.py <二进制文件> <内存上限(整数个数)>")
        sys.exit(1)
    external_sort(sys.argv[1], int(sys.argv[2]))