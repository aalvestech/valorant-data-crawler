from src.raw_data.get_raw_data import run_raw_data
from src.data_clean.make_cleaned import run_clean

def run():
    run_raw_data()
    run_clean()

if __name__ == "__main__":
    run()