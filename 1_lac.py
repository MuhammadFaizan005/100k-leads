import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
import random


def open_tabs_and_fetch_followers(product):
    options = Options()
    options.add_experimental_option("debuggerAddress", "localhost:9222")
    link = f"https://ec.synnex.com/ecx/part/searchResult.html?keyword={product}&keywordRelation=2&prefferedWarehouse=0&criteriaType=4&resultType=7&begin=0&catId=-1&suggestionSearch=N&_source=SearchBar&partSearchType=FreeText"
    driver = None
    try:
        driver = webdriver.Chrome(options=options)

        # for person, link in tqdm(profile_dict.items(), desc="Opening Links", unit="link"):
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
                img_url =f"https:{image_link}"
                # Fetch the product title
                title_strong = soup.find("strong", class_="ui-product-title")
                title_link = title_strong.find("a")
                product_title = title_link.get_text(strip=True) if title_link else None
                
                # Output the results
                print(f"Image Link: {image_link}")
                print(f"Product Title: {product_title}")
                # Update DataFrame with product title and image URL
                df.loc[df['TD SKU'] == product, 'Product Name'] = str(product_title)
                df.loc[df['TD SKU'] == product, 'Image URL'] = str(img_url)

                # Save the updated DataFrame to CSV after each update
                df.to_csv(file_path, index=False)  # Save changes


        except Exception as e:
                print(f"Error fetching data from {link}: {e}")

            # Pause between opening profiles to avoid triggering anti-scraping measures
        time.sleep(1)

    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    file_path = r"C:\Users\Xcient\Desktop\scrapper\1_lac\Faizan xeltch - Sheet1.csv"
    pre = 0
    next = 10000
    df = pd.read_csv(file_path)
    products = df["TD SKU"].to_list()
    for product in tqdm(products[pre:next],desc="Processing Product"):
        print(f"Searching for : {product}")
        open_tabs_and_fetch_followers(product)