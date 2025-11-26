import os
from dotenv import load_dotenv
from processor import process_files

def main():
    load_dotenv()
    process_files()

if __name__ == "__main__":
    main()