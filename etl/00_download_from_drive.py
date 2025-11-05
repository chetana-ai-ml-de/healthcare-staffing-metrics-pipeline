import os
from dotenv import load_dotenv
import gdown

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

load_dotenv()
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE")

def main():
    # Download entire folder (requires the folder to be shareable)
    print(f"Downloading folder id={FOLDER_ID} into {RAW_DIR} ...")
    gdown.download_folder(
        id=FOLDER_ID,
        output=RAW_DIR,
        quiet=False,
        use_cookies=False
    )
    print("Done.")

if __name__ == "__main__":
    main()
