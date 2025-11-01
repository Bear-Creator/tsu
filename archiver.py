import argparse
import os
import sys
import tarfile
import time
from pathlib import Path
import bz2
import compression.zstd as zstd

# Парсер для параметров 
def parse_args():
    parser = argparse.ArgumentParser(
        description="Универсальный архиватор с поддержкой zstd и bz2, включая замер скорости и симпатичный прогресс.",
        epilog="""
            Примеры:
              python archiver.py -c data.txt data.zst -b
              python archiver.py -c folder backup.tar.bz2 -p
              python archiver.py -x archive.tar.zst output_dir
              python archiver.py -x dump.bz2 restore.txt --benchmark
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("-c", "--compress", action="store_true", help="Упаковать данные")
    parser.add_argument("-x", "--extract", action="store_true", help="Распаковать данные")
    parser.add_argument("source", help="Файл или директория для обработки")
    parser.add_argument("target", help="Путь для архива или распаковки")
    parser.add_argument("-b", "--benchmark", action="store_true", help="Показать время выполнения и размеры")
    parser.add_argument("-p", "--progress", action="store_true", help="Отображать прогресс (в стиле Pacman)")
    return parser.parse_args()

# Оценка размера
def calc_size(path):
    """Подсчёт общего размера файла или директории"""
    p = Path(path)
    if p.is_file():
        return p.stat().st_size
    if p.is_dir():
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    return 0

# Прогресс бар
def pacman_progress(done, total, label=""):
    """Отображает прогресс как в Pacman (Arch Linux)"""
    if total == 0:
        return
    width = 30
    percent = done / total
    filled = int(width * percent)
    empty = width - filled
    pac = ">"  # символ, имитирующий движение "пакмана"
    bar = "█" * filled + pac + "░" * (empty - 1 if empty > 0 else 0)
    sys.stdout.write(f"\r{label} [{bar}] {percent * 100:5.1f}%")
    sys.stdout.flush()
    if done >= total:
        print()  # переход строки после завершения

# Сжатие в ZSTD
def zstd_compress(src, dst, show_progress=False):
    """Сжимает файл с помощью zstd"""
    data = Path(src).read_bytes()
    compressor = zstd.ZstdCompressor()
    total = len(data)

    out = bytearray()
    chunk = 256 * 1024
    for i in range(0, total, chunk):
        part = data[i:i + chunk]
        out.extend(compressor.compress(part))
        if show_progress:
            pacman_progress(i + len(part), total, "Сжатие zstd")
    out.extend(compressor.flush())

    Path(dst).write_bytes(out)
    if show_progress:
        print("✔ Архивация завершена")

# Распаковка из ZSTD
def zstd_decompress(src, dst, show_progress=False):
    """Распаковывает zstd-файл"""
    data = Path(src).read_bytes()
    total = len(data)
    decompressor = zstd.ZstdDecompressor()

    out = bytearray()
    step = 256 * 1024
    i = 0
    while i < total:
        block = data[i:i + step]
        out.extend(decompressor.decompress(block))
        if show_progress:
            pacman_progress(i + len(block), total, "Извлечение zstd")
        i += step

    Path(dst).write_bytes(out)
    if show_progress:
        print("✔ Распаковка завершена")

# Сжатие в BZ2
def bz2_compress(src, dst, show_progress=False):
    """Сжимает файл в bz2"""
    data = Path(src).read_bytes()
    result = bz2.compress(data)
    Path(dst).write_bytes(result)
    if show_progress:
        pacman_progress(len(data), len(data), "Сжатие bz2")
        print("✔ Архив создан")

# Распаковка из BZ2
def bz2_decompress(src, dst, show_progress=False):
    """Извлекает файл bz2"""
    data = Path(src).read_bytes()
    result = bz2.decompress(data)
    Path(dst).write_bytes(result)
    if show_progress:
        pacman_progress(len(data), len(data), "Извлечение bz2")
        print("✔ Файл восстановлен")

# Создание TAR
def tar_build(folder, tar_path):
    """Создаёт tar-архив из директории"""
    print(f"→ Архивируется каталог: {folder}")
    with tarfile.open(tar_path, "w") as tar:
        tar.add(folder, arcname=os.path.basename(folder))

# Извлечение TAR
def tar_unpack(tar_path, dest_dir):
    """Распаковывает tar-архив"""
    os.makedirs(dest_dir, exist_ok=True)
    print(f"→ Извлечение содержимого в: {dest_dir}")
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(dest_dir)


def main():
    args = parse_args()

    if args.compress == args.extract:
        print("Укажите один режим: либо -c (сжать), либо -x (распаковать)")
        sys.exit(1)

    src = Path(args.source)
    dst = Path(args.target)
    start_time = time.time() if args.benchmark else None

    is_dir = src.is_dir()
    is_tar_target = len(dst.suffixes) == 2 and dst.suffixes[0] == ".tar"
    ext = dst.suffix if args.compress else src.suffix

    if ext not in [".zst", ".bz2"]:
        print("Поддерживаются только .zst и .bz2 (включая .tar.zst/.tar.bz2)")
        sys.exit(1)

    if args.compress:
        if is_dir:
            if not is_tar_target:
                print("Для каталогов используйте выходной формат .tar.zst или .tar.bz2")
                sys.exit(1)
            temp_tar = "tmp_data.tar"
            tar_build(src, temp_tar)
            input_obj = temp_tar
        else:
            input_obj = src

        if ext == ".zst":
            zstd_compress(input_obj, dst, args.progress)
        else:
            bz2_compress(input_obj, dst, args.progress)

        if is_dir and os.path.exists("tmp_data.tar"):
            os.remove("tmp_data.tar")

    else:
        is_tar_src = len(src.suffixes) == 2 and src.suffixes[0] == ".tar"
        if is_tar_src:
            temp_tar = "tmp_unpack.tar"
            if ext == ".zst":
                zstd_decompress(src, temp_tar, args.progress)
            else:
                bz2_decompress(src, temp_tar, args.progress)
            tar_unpack(temp_tar, dst)
            os.remove(temp_tar)
        else:
            if ext == ".zst":
                zstd_decompress(src, dst, args.progress)
            else:
                bz2_decompress(src, dst, args.progress)

    # --------------------- БЕНЧМАРК ---------------------
    if args.benchmark:
        elapsed = time.time() - start_time
        in_size = calc_size(args.source)
        out_size = calc_size(args.target)
        print("\n" + "═" * 45)
        print("📊  Статистика выполнения")
        print(f"⏱  Время:         {elapsed:.2f} сек")
        print(f"📦  Входной размер: {in_size / 1024:.1f} КБ")
        print(f"💾  Выходной размер: {out_size / 1024:.1f} КБ")
        if in_size:
            ratio = out_size / in_size
            print(f"🔻  Коэффициент сжатия: {ratio:.2f}x")
        print("═" * 45)

if __name__ == "__main__":
    main()
