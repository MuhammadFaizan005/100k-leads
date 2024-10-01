import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
import random
from GoogleDriveCSVHandler import GoogleDriveCSVHandler  # Import the class
import subprocess

def merge_local_and_drive_csv(local_csv, drive_csv):
    # Merge the two CSVs, keeping the most recent information
    merged_df = pd.concat([drive_csv, local_csv]).drop_duplicates(subset='TD SKU', keep='last').reset_index(drop=True)
    return merged_df

def open_tabs_and_fetch_product_info(product, df, local_csv_path):
    options = Options()
    options.add_experimental_option("debuggerAddress", "localhost:9222")
    link = f"https://ec.synnex.com/ecx/part/searchResult.html?keyword={product}&keywordRelation=2&prefferedWarehouse=0&criteriaType=4&resultType=7&begin=0&catId=-1&suggestionSearch=N&_source=SearchBar&partSearchType=FreeText"
    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(link)

        try:
            # Wait for the page to load completely
            time.sleep(random.uniform(1, 3))  # Random delay to avoid detection

            # Save the page source to a variable
            page_source = driver.page_source

            # Parse the page source with BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')

            # Fetch the image link
            image_div = soup.find("div", class_="product-img")
            image_tag = image_div.find("img")
            image_link = image_tag['src'] if image_tag else None
            img_url = f"https:{image_link}" if image_link else None

            # Fetch the product title
            title_strong = soup.find("strong", class_="ui-product-title")
            title_link = title_strong.find("a")
            product_title = title_link.get_text(strip=True) if title_link else None

            # Update the DataFrame if product title or image link is found
            if product_title or img_url:
                df.loc[df['TD SKU'] == product, 'Product Name'] = product_title
                df.loc[df['TD SKU'] == product, 'Image URL'] = img_url

            else:
                print(f"No Image/Title found for {product}")

            # Save DataFrame to temp.csv after processing each product
            df.to_csv(local_csv_path, index=False)

        except Exception as e:
            print(f"Error fetching data from {link}: {e}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    print("Opening Session-1")
    command = r'"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:/Chrome_Session1"'

    subprocess.Popen(command, shell=True)

    # print("Accessing Google Drive")
    # SCOPES = ['https://www.googleapis.com/auth/drive']
    # SERVICE_ACCOUNT_FILE = r'D:\Work\Scrapper Scripts\Scrape_600\insta-followers\driveApi\insta-followers-436814-171b89fe3492.json'
    # drive_file_name = r'C:\Users\Xcient\Desktop\scrapper\1_lac\final_lac_api\100k-leads\Faizan xeltch - Sheet1.csv'  # Set the full path to the drive CSV
    local_csv_path = r'C:\Users\Xcient\Desktop\scrapper\1_lac\final_lac_api\100k-leads\9222\temp.csv'  # Set the full path to the local CSV

    # Initialize the GoogleDriveCSVHandler class
    # csv_handler = GoogleDriveCSVHandler(SERVICE_ACCOUNT_FILE, SCOPES)

    # Step 1: Download the CSV from Google Drive
    # drive_csv = csv_handler.download_csv("Faizan xeltch - Sheet1.csv")
    # print(f"Downloaded the Csv {drive_csv}")
    # Step 2: Load the local CSV from your PC
    local_csv = pd.read_csv(local_csv_path)
    # Step 3: Merge the local and drive CSV files
    # merged_csv = merge_local_and_drive_csv(local_csv, drive_csv)
    Sku = local_csv['TD SKU'].to_list()
    empty = []
    valued = []
    entries = ["1","2"]
    for index_,entry in tqdm(enumerate(entries),desc="Processing Entry"):
        for index,product in enumerate(Sku):
            if str(product).startswith(entry):
                nan_products = local_csv.loc[index,'Product Name']
                if pd.isna(nan_products):
                    print(f"\nSerching For Product : {product}")
                    open_tabs_and_fetch_product_info(product,local_csv,local_csv_path)
                    empty.append(nan_products)
                else:
                    # print(nan_products)
                    valued.append(nan_products)

        print(f"\nChecking For {entry}\nEmpty values Length : {len(empty)}\nVales Length: {len(valued)}")
            
    # print(f"{len(ones)}\n{ones}\n{indexes}")

    # Step 4: Save the updated merged CSV locally
    # merged_csv.to_csv(local_csv_path, index=False)
    # print(merged_csv.head())
    # print("CSV file successfully merged and updated.")
    
    # # Step 5: Process product information from the updated CSV
    # nan_indices = merged_csv[merged_csv['Product Name'].isna()].index.tolist()
    # if nan_indices:
    #     products = merged_csv['TD SKU'].iloc[nan_indices].to_list()

    #     # Process each product, fetching its details
    #     for product in tqdm(products[21:5000], desc="Processing Products"):
    #         print(f"\nSearching for: {product}")
    #         open_tabs_and_fetch_product_info(product, merged_csv, local_csv_path)

    #     # Save the final updates to both Google Drive and local CSV after all products are processed
    #     csv_handler.upload_csv(merged_csv, drive_file_name)
    #     merged_csv.to_csv(local_csv_path, index=False)

    # else:
    #     print("All products have been processed.")
