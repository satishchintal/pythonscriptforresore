import boto3
import pandas as pd
import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk, scrolledtext
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime, timedelta
from urllib.parse import urlparse

class S3DataRetrievalApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("S3 Data Retrieval")
        self.geometry("600x400")
        self.configure(bg='#f0f0f0')

        self.s3_client = self.get_s3_client()

        self.create_widgets()

    def create_widgets(self):
        # Title Label
        tk.Label(self, text="Choose Retrieval Method", font=("Helvetica", 16), bg='#f0f0f0').pack(pady=10)

        # Buttons
        tk.Button(self, text="Batch Retrieval", command=self.start_batch_retrieval_thread, height=2, width=20).pack(pady=5)
        tk.Button(self, text="Single Folder Retrieval", command=self.start_single_retrieval_thread, height=2, width=20).pack(pady=5)

        # Progress Bar
        self.progress = ttk.Progressbar(self, orient="horizontal", length=500, mode="determinate")
        self.progress.pack(pady=10)

        # Log Text Box
        self.log_text = scrolledtext.ScrolledText(self, wrap=tk.WORD, width=70, height=10, state='disabled', bg='#e0e0e0')
        self.log_text.pack(pady=10)

        # Initial log message to confirm the app is running
        self.log_message("Application started successfully.")

    def log_message(self, message):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.yview(tk.END)
        self.log_text.config(state='disabled')

    def get_s3_client(self):
        try:
            s3_client = boto3.client('s3')
            s3_client.list_buckets()  # Test credentials
            return s3_client
        except (NoCredentialsError, PartialCredentialsError):
            messagebox.showerror("Error", "AWS credentials not found or incomplete. Please reconfigure your credentials.")
            os.system('aws configure')
            return self.get_s3_client()

    def parse_s3_url(self, s3_url):
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc
        folder_name = parsed_url.path.lstrip('/')
        return bucket_name, folder_name

    def download_s3_folder(self, bucket_name, folder_name, days, retrieval_tier):
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
        
        # Calculate the total number of files
        file_count = sum([len(page.get('Contents', [])) for page in pages])
        self.progress['maximum'] = file_count

        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_name):
            for obj in page.get('Contents', []):
                last_modified = obj['LastModified']
                if last_modified >= cutoff_date:
                    file_path = obj['Key']
                    metadata = self.s3_client.head_object(Bucket=bucket_name, Key=file_path)

                    if metadata.get('StorageClass') in ['GLACIER', 'DEEP_ARCHIVE']:
                        self.s3_client.restore_object(
                            Bucket=bucket_name,
                            Key=file_path,
                            RestoreRequest={
                                'Days': 1,
                                'GlacierJobParameters': {
                                    'Tier': retrieval_tier
                                }
                            }
                        )
                        self.log_message(f"Initiated restore for {file_path} with tier {retrieval_tier}")
                    else:
                        self.s3_client.download_file(bucket_name, file_path, os.path.basename(file_path))
                        self.log_message(f"Downloaded {file_path}")
                        self.progress.step(1)
                        self.update_idletasks()

        messagebox.showinfo("Completed", "All files downloaded successfully!")
        self.log_message("All files downloaded successfully!")

    def process_excel_and_download(self, excel_path, retrieval_tier):
        df = pd.read_excel(excel_path)
        for index, row in df.iterrows():
            s3_url = row['S3 URL']
            days = row['Number of days']
            bucket_name, folder_name = self.parse_s3_url(s3_url)
            self.download_s3_folder(bucket_name, folder_name, days, retrieval_tier)

    def batch_retrieval(self):
        excel_path = filedialog.askopenfilename(title="Select Excel File", filetypes=[("Excel files", "*.xlsx")])
        retrieval_tier = simpledialog.askstring("Input", "Enter the retrieval tier (Expedited, Standard, Bulk):").capitalize()
        if retrieval_tier not in ["Expedited", "Standard", "Bulk"]:
            messagebox.showerror("Error", "Invalid retrieval tier. Please enter Expedited, Standard, or Bulk.")
            return
        if excel_path:
            self.process_excel_and_download(excel_path, retrieval_tier)

    def single_retrieval(self):
        bucket_name = simpledialog.askstring("Input", "Please enter the S3 bucket name:")
        folder_name = simpledialog.askstring("Input", "Please enter the folder path in the bucket:")
        days = simpledialog.askinteger("Input", "Enter the number of days to filter files:")
        retrieval_tier = simpledialog.askstring("Input", "Enter the retrieval tier (Expedited, Standard, Bulk):").capitalize()
        if retrieval_tier not in ["Expedited", "Standard", "Bulk"]:
            messagebox.showerror("Error", "Invalid retrieval tier. Please enter Expedited, Standard, or Bulk.")
            return
        if bucket_name and folder_name and days is not None:
            self.download_s3_folder(bucket_name, folder_name, days, retrieval_tier)

    def start_batch_retrieval_thread(self):
        threading.Thread(target=self.batch_retrieval).start()

    def start_single_retrieval_thread(self):
        threading.Thread(target=self.single_retrieval).start()

if __name__ == "__main__":
    app = S3DataRetrievalApp()
    app.mainloop()
