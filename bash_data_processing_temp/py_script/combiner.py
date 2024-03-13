import pandas as pd
import glob
import sys
import re
import os
import warnings

warnings.filterwarnings("ignore")

def output_filenames(root_folder: str, we_date: str) -> tuple:
    return (os.path.join(root_folder, f"concordance_we_{we_date}.csv"), os.path.join(root_folder, f"concordance_mms_we_{we_date}.csv"))


if __name__ == "__main__":
    week_ending_date = sys.argv[1] # YYYYMMDD
    folder_path = sys.argv[2]
    glob_path = os.path.join(folder_path, "*.xlsx")

    # print(folder_path)

    concordance_output, concordance_mms_output = output_filenames(folder_path, week_ending_date)

    print(concordance_output)
    print(concordance_mms_output)

    # print(glob_path)

    all_files = glob.glob(glob_path)
    
    all_files = [file for file in all_files if re.search(r"concordance", file, re.IGNORECASE)]
    
    combined_df = pd.DataFrame()

    for file in all_files:
        print(file)
        # Read the .xlsx file
        if re.search(r"mms", file, re.IGNORECASE):
            pd.read_excel(file).to_csv(concordance_mms_output, index=False)
        else:
            df = pd.read_excel(file)
            combined_df = pd.concat([combined_df, df], ignore_index=True)            
    
    if len(combined_df) > 0:        
        combined_df.to_csv(concordance_output, index=False)