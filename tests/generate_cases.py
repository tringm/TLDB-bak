from config import root_path
import shutil

TEST_PATH = root_path() / 'test' / 'io' / 'in' / 'cases'
INPUT_FOLDER = 'invoice_complex'
OUTPUT_FOLDER = 'invoice_complex_big'
N_LINES = 5000

(TEST_PATH / OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

for file in (TEST_PATH / INPUT_FOLDER).glob('**/*'):
    if file.stem != 'XML_query':
        with file.open() as f:
            lines = [f.readline() for i in range(N_LINES)]
        with (TEST_PATH / OUTPUT_FOLDER / file.name).open(mode='w') as f:
            f.writelines(lines)
    else:
        shutil.copy(str(file), str(TEST_PATH / OUTPUT_FOLDER / file.name))

