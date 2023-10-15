from sec_edgar_downloader import Downloader
import os
from edgar import Edgar

# Initialize Edgar objects
edgar_obj = Edgar()
edgar_download = Downloader("DummyCompany", "dummy@email.com")

# Get the list of all CIKs
cik_list = list(edgar_obj.all_companies_dict.values())

# Define the list of years for which you want to download the 10-K filings
years = list(range(1993, 2023))

# Define the base save path
base_save_path = "downloaded_filings"

# Loop through each CIK and each year and download the 10-K filings
for cik in cik_list:
    for year in years:
        try:
            print(f"Downloading 10-K filings for CIK: {cik} for year: {year}")
            num_downloaded_files = edgar_download.get("10-K", cik, after=f"{year}-01-01", before=f"{year}-12-31")

            # Check if any files were downloaded
            if num_downloaded_files == 0:
                print(f"No 10-K filings available for CIK: {cik} for year: {year}")
                continue

            print(f"Downloaded {num_downloaded_files} 10-K filings for CIK: {cik} for year: {year}")

            # Move downloaded files to the structured folder
            src_folder = os.path.join("sec-edgar-filings", cik, "10-K")
            dst_folder = os.path.join(base_save_path, cik, str(year))
            os.makedirs(dst_folder, exist_ok=True)

            # Check if the source folder exists before moving files
            if os.path.exists(src_folder):
                for file_name in os.listdir(src_folder):
                    os.rename(os.path.join(src_folder, file_name), os.path.join(dst_folder, file_name))
            else:
                print(f"Source folder does not exist: {src_folder}")

        except ValueError as e:
            if "After date cannot be greater than the before date" in str(e):
                print(f"No 10-K filings available for CIK: {cik} for year: {year}")
                continue
            else:
                print(f"Error downloading 10-K filings for CIK: {cik} for year: {year} - {e}")
        except Exception as e:
            print(f"Error downloading 10-K filings for CIK: {cik} for year: {year} - {e}")
