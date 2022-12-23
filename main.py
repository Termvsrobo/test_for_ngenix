"""Модуль тестового задания"""
import csv
import random
import string
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from functools import partial
from multiprocessing import Pool, Queue
from pathlib import Path

get_random_int_to_10 = partial(random.randint, 1, 10)
get_random_int_to_100 = partial(random.randint, 1, 100)

csv_levels_queue = Queue()
csv_names_queue = Queue()

COUNT_ZIPFILES = 50
COUNT_XMLFILES = 100

def get_random_string() -> str:
    """Генерирует случайную строку различной длины"""
    length = get_random_int_to_10()
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_xml_tree():
    """Создает дерево элементов"""
    root = ET.Element('root')
    ET.SubElement(root, 'var', attrib={'name': 'id', 'value': get_random_string()})
    ET.SubElement(root, 'var', attrib={'name': 'level', 'value': str(get_random_int_to_100())})
    objects = ET.SubElement(root, 'objects')

    count_of_objects = get_random_int_to_10()

    for _ in range(count_of_objects):
        ET.SubElement(objects, 'object', attrib={'name': get_random_string()})

    return ET.ElementTree(root)

def create_zip_files():
    """Создает zip-файлы"""
    result_dir = Path('./result')
    result_dir.mkdir(parents=True, exist_ok=True)
    for index in range(COUNT_ZIPFILES):
        with zipfile.ZipFile(result_dir / Path(f'xml_{index}.zip'), mode='w') as xml_zip:
            for _ in range(COUNT_XMLFILES):
                with tempfile.NamedTemporaryFile(suffix='.xml', dir='.') as tmpfile:
                    xml_tree = get_xml_tree()
                    xml_tree.write(tmpfile)
                    tmpfile.flush()
                    local_fname = Path(tmpfile.name)
                    xml_zip.write(local_fname.name)

def parse_zip_xml_file(zip_file):
    """Парсит xml-файл и кладет полученные данные в очереди для записи в csv"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        with zipfile.ZipFile(zip_file, mode='r') as xml_zip:
            xml_zip.extractall(tmpdirname)
            for file in Path(tmpdirname).glob('*.xml'):
                tree = ET.parse(file)
                root = tree.getroot()
                _id = root.findall('.//*/[@name="id"]')[0].attrib['value']
                _level = root.findall('.//*/[@name="level"]')[0].attrib['value']
                _objects_names = [obj.attrib['name'] for obj in root.find('objects')]
                csv_id_data = {'id': _id, 'level': _level}
                csv_levels_queue.put(csv_id_data)
                for obj_name in _objects_names:
                    csv_obj_data = {'id': _id, 'object_name': obj_name}
                    csv_names_queue.put(csv_obj_data)

def process():
    """Обрабатывает параллельно несколько zip-файлов"""
    result_dir = Path('./result')
    with Pool() as pool:
        pool.map(
            parse_zip_xml_file,
            list(result_dir.glob('*.zip'))
        )

def write_csv():
    """Сохраняет данные в csv файлы"""
    result_dir = Path('./csv')
    result_dir.mkdir(parents=True, exist_ok=True)
    file_levels = result_dir / Path('levels.csv')
    file_names = result_dir / Path('names.csv')
    with file_levels.open('w', newline='') as csv_levels:
        fieldnames = ['id', 'level']
        writer_levels = csv.DictWriter(csv_levels, fieldnames=fieldnames)
        writer_levels.writeheader()
        while not csv_levels_queue.empty():
            levels = csv_levels_queue.get()
            writer_levels.writerow(levels)

    with file_names.open('w', newline='') as csv_names:
        fieldnames = ['id', 'object_name']
        writer_names = csv.DictWriter(csv_names, fieldnames=fieldnames)
        writer_names.writeheader()
        while not csv_names_queue.empty():
            names = csv_names_queue.get()
            writer_names.writerow(names)

if __name__ == '__main__':
    create_zip_files()
    process()
    write_csv()
