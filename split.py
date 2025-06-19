import pandas as pd
import os
import logging

# Konfigurasi logging untuk memberikan feedback yang jelas
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def separate_text_column():
    """
    Membaca file Reviews.csv asli dan memisahkannya menjadi dua file:
    1. Reviews.csv (di-overwrite): Berisi semua data asli kecuali kolom 'Text'.
    2. unstructured_text_only.txt: Berisi HANYA teks mentah dari kolom 'Text'.
    """
    # Menentukan path file input dan output
    csv_path = os.path.join('datasets', 'Reviews.csv')
    text_output_path = os.path.join('datasets', 'unstructured_text_only.txt')

    logging.info(f"Mulai pra-pemrosesan untuk file: {csv_path}")

    try:
        # 1. Membaca dataset asli
        logging.info("Membaca file CSV asli...")
        df = pd.read_csv(csv_path)
        logging.info(f"Berhasil membaca {len(df)} baris data.")

        # 2. Membersihkan data sebelum diproses
        # Hapus baris di mana kolom 'Text' kosong untuk mencegah error/baris kosong
        df.dropna(subset=['Text'], inplace=True)
        # Pastikan kolom 'Text' adalah tipe data string
        df['Text'] = df['Text'].astype(str)
        logging.info(f"Data dibersihkan, tersisa {len(df)} baris valid untuk diproses.")

        # 3. Membuat dan menyimpan file teks tidak terstruktur
        logging.info(f"Menulis data teks ke: {text_output_path}")
        
        # Menggunakan metode 'to_csv' adalah cara pandas yang sangat cepat untuk ini
        # header=False dan index=False memastikan hanya konten teks yang ditulis
        df['Text'].to_csv(text_output_path, header=False, index=False)

        logging.info("File teks berhasil disimpan.")

        # 4. Menghapus kolom 'Text' dan meng-overwrite file CSV asli
        logging.info(f"Menghapus kolom 'Text' dan meng-overwrite file: {csv_path}")
        df_metadata = df.drop(columns=['Text'])
        
        # Simpan kembali ke path asli, tanpa kolom indeks pandas
        df_metadata.to_csv(csv_path, index=False)
        logging.info("File CSV asli berhasil di-overwrite.")
        
        logging.info("Pra-pemrosesan selesai dengan sukses!")

    except FileNotFoundError:
        logging.error(f"Error: File tidak ditemukan di {csv_path}. Pastikan file ada di folder 'datasets'.")
    except Exception as e:
        logging.error(f"Terjadi error yang tidak terduga: {e}")

if __name__ == "__main__":
    separate_text_column()