import os
import sys
import fitz  # đúng tên module là fitz, không phải PyMuPDF
from typing import Tuple
import shutil
import concurrent.futures
import pandas as pd
from config import DATASET_CLEANING_DIR
if not os.path.exists(DATASET_CLEANING_DIR):
    os.makedirs(DATASET_CLEANING_DIR)


def is_scanned(file_path: str):
    """
    Check if a PDF file is scanned or text-based.

    Args:
        file_path (str): Path to the PDF file.
    Returns:
        Tuple[str, str]: A tuple containing the file type ("scanned" or "text-based")
                         and the extracted text (empty string if scanned).  
    """
    try:
        doc = fitz.open(file_path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        if text.strip():
            return "text-based"
        else:
            return "scanned"
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return "error"


def ocr_process(file_path: str) -> str:
    """
    Perform OCR on a scanned PDF file and save the extracted text.

    Args:
        file_path (str): Path to the scanned PDF file.
    Returns:
        str: Path to the text file containing extracted text.
    """
    try: 
        import pytesseract
        from PIL import Image
        from pdf2image import convert_from_path
        
        images = convert_from_path(file_path)
        
        text = ""
        for i, image in enumerate(images):
            text = text + pytesseract.image_to_string(image, lang='vie') + "\n"
        base_name = os.path.basename(file_path).rsplit('.', 1)[0]
        text_file_path = os.path.join(DATASET_CLEANING_DIR, f"{base_name}_ocr.txt")
        with open(text_file_path, "w", encoding="utf-8") as text_file:
            text_file.write(text)
        return text_file_path
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return "error"
    

def process_file(file_path):
    file_type = is_scanned(file_path=file_path)
    if file_type == "scanned":
        print(f"Performing OCR on scanned file: {file_path}")
        ocr_process(file_path=file_path)
    elif file_type == "text-based":
        print(f"File is text-based, saving as .txt: {file_path}")
        import fitz
        doc = fitz.open(file_path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        base_name = os.path.basename(file_path).rsplit('.', 1)[0]
        text_file_path = os.path.join(DATASET_CLEANING_DIR, f"{base_name}.txt")
        with open(text_file_path, "w", encoding="utf-8") as text_file:
            text_file.write(text)
    else:   
        print(f"Skipping file due to error: {file_path}")

    # xoa file goc
    os.remove(file_path)

def main():
    dataset_dir = "./dataset"
    dataframe = pd.read_csv('ban_an.csv')
    file_list = [f for f in os.listdir(dataset_dir) if f.lower().endswith('.pdf')] 
    pdf_link = dataframe['pdf_link'].tolist()
    file_name_list = []
    for link in pdf_link:
        if isinstance(link, str):
            file_name = link.split('/')[-1]
            if file_name in file_list:
                file_name_list.append(file_name)
                
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file, os.path.join(dataset_dir, file_name)) for file_name in file_name_list]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # để bắt lỗi nếu có
    
    print(f"Total files to process: {len(file_name_list)}")
    

if __name__ == "__main__":
    process_file("./test/[FINAL]_BA_LDPT_04_[Thanh_Hải__Trường_Tân_Tạo].pdf")
