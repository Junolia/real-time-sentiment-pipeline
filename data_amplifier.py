import os
import time

# Update the input filename to match your Kaggle CSV
INPUT_FILE = 'kaggle_RC_2019-05.csv' 
OUTPUT_FILE = 'massive_macro_stream.csv'
MULTIPLIER = 50  

print(f"Starting data amplification to reach 2GB scale...")
start_time = time.time()

with open(INPUT_FILE, 'r', encoding='utf-8') as f_in:
    lines = f_in.readlines()

header = lines[0]
data_rows = lines[1:]

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
    f_out.write(header) 
    for i in range(MULTIPLIER):
        f_out.writelines(data_rows) 
        if (i + 1) % 10 == 0:
            print(f"Progress: {i + 1}/{MULTIPLIER} iterations completed")

end_time = time.time()
final_size_gb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024 * 1024)

print(f"Amplification finished in {end_time - start_time:.2f} seconds")
print(f"Final File Size: {final_size_gb:.2f} GB")