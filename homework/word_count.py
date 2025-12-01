"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os
import shutil
import time
from itertools import groupby

from toolz.itertoolz import concat, pluck


def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    # Carpeta donde están los archivos base
    raw_folder = "files/raw"

    # Carpeta donde se van a copiar
    input_folder = "files/input"

    # Crear carpeta si no existe
    if os.path.exists(input_folder):
        shutil.rmtree(input_folder)
    os.makedirs(input_folder)

    # Obtener archivos base
    raw_files = glob.glob(os.path.join(raw_folder, "*"))

    # Copiar n veces
    for i in range(n):
        for file in raw_files:
            shutil.copy(file, os.path.join(input_folder, f"{i}_{os.path.basename(file)}"))


def load_input(input_directory):
    """Load input: lee todos los archivos y devuelve cada línea"""
    files = glob.glob(os.path.join(input_directory, "*"))
    return fileinput.input(files, encoding="utf8")


def preprocess_line(x):
    """Convierte a minúsculas y separa palabras"""
    return x.strip().lower().split()


def map_line(x):
    """Devuelve pares (palabra, 1)"""
    return [(word, 1) for word in preprocess_line(x)]


def mapper(sequence):
    """Mapper: aplica map_line a cada línea y concatena resultados"""
    return concat(map(map_line, sequence))


def shuffle_and_sort(sequence):
    """Agrupa por palabra"""
    sorted_seq = sorted(sequence, key=lambda x: x[0])
    return groupby(sorted_seq, key=lambda x: x[0])


def compute_sum_by_group(group):
    """Suma los valores de cada grupo"""
    key, values = group
    total = sum(pluck(1, values))
    return key, total


def reducer(sequence):
    """Reducer: aplica la suma por grupo"""
    return [compute_sum_by_group(g) for g in sequence]


def create_directory(directory):
    """Crea la carpeta de salida"""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)


def save_output(output_directory, sequence):
    """Guardar salida en part-00000"""
    output_file = os.path.join(output_directory, "part-00000")
    with open(output_file, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Crea archivo _SUCCESS"""
    success_file = os.path.join(output_directory, "_SUCCESS")
    open(success_file, "w").close()


def run_job(input_directory, output_directory):
    """Ejecución completa"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
