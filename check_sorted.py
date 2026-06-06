import sys
import array

def verify_sorted(filename):
    data = array.array('i')
    try:
        with open(filename, 'rb') as f:
            raw = f.read()
            if not raw:
                print(f"{filename} 是空的，就当有序吧")
                return True
            data.frombytes(raw)
    except FileNotFoundError:
        print(f"找不到 {filename}")
        return False
    except Exception as e:
        print(f"读文件时炸了: {e}")
        return False

    total = len(data)
    print(f"检查 {filename}，共 {total} 个整数")

    if total <= 1:
        print("数量太少，自然有序")
        return True

    previous = data[0]
    for i in range(1, total):
        current = data[i]
        if current < previous:
            print(f"顺序不对！下标 {i}: {current} < {previous}")
            return False
        previous = current

    print("OK，全部升序")
    return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("用法: python check_sorted.py <二进制文件>")
        sys.exit(1)
    verify_sorted(sys.argv[1])