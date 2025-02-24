import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
import requests
from pymongo import MongoClient
import os

# Constants
BASE_URL = "https://appsource.microsoft.com/en-us/marketplace/partner-dir"
PAGE_SIZE = 18
RETRY_DELAY = 30
MAX_RETRIES = 5
RADIUS = 100
MAX_PAGE_OFFSET = 90

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Azure_partners_db"
COLLECTION_NAME = "Azure_db"

# Temporary file for storing processed company IDs
TEMP_FILE = "processed_ids.txt"

class AzurePartnerScraper:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self.processed_count = 0
        self.current_alphabet_count = 0

    def get_search_url(self, alphabet, page_offset=0):
        """Generate URL with dynamic page offset."""
        return f"{BASE_URL}?filter=sort%3D0%3BpageSize%3D{PAGE_SIZE}%3BpageOffset%3D{page_offset}%3Bradius%3D{RADIUS}%3Bfreetext%3D{alphabet}%3Bsuggestion%3Dtrue"

    async def extract_list_data(self, element, selector):
        """Helper function to extract list data from elements."""
        items = await element.query_selector_all(selector)
        return [await item.text_content() for item in items] if items else []

    async def search_partners(self, page, alphabet, page_offset):
        """Search for partners using pagination and extract detailed data."""
        try:
            search_url = self.get_search_url(alphabet, page_offset)
            await page.goto(search_url, timeout=60000)
            print(f"\nProcessing alphabet '{alphabet}' - Page Offset: {page_offset}")
            await asyncio.sleep(5)  
            
            partner_elements = await page.query_selector_all('.partner-card')
            partners_data = []
            
            for element in partner_elements:
                try:
                    company_id = await element.get_attribute('id')
                    name_element = await element.query_selector('.partner-name')
                    desc_element = await element.query_selector('.partner-description')
                    website_element = await element.query_selector('a.partner-website')
                    linkedin_element = await element.query_selector('a.partner-linkedin')
                    logo_element = await element.query_selector('img.partner-logo')
                    
                    industry_focus = await self.extract_list_data(element, '.industry-focus-item')
                    products = await self.extract_list_data(element, '.product-item')
                    services = await self.extract_list_data(element, '.service-type-item')
                    solutions = await self.extract_list_data(element, '.solution-item')
                    target_sizes = await self.extract_list_data(element, '.target-size-item')
                    
                    partner_data = {
                        "partnerDetails": {
                            "id": company_id,
                            "name": await name_element.text_content() if name_element else None,
                            "description": await desc_element.text_content() if desc_element else None,
                            "url": await website_element.get_attribute('href') if website_element else None,
                            "linkedInOrganizationProfile": await linkedin_element.get_attribute('href') if linkedin_element else None,
                            "logo": await logo_element.get_attribute('src') if logo_element else None,
                            "industryFocus": industry_focus,
                            "product": products,
                            "serviceType": services,
                            "solutions": solutions,
                            "targetCustomerCompanySizes": target_sizes
                        }
                    }
                    partners_data.append(partner_data)
                except Exception as e:
                    print(f"Error extracting partner data: {e}")
            
            return partners_data, len(partner_elements) > 0

        except Exception as e:
            print(f"Error during partner search for alphabet '{alphabet}' at offset {page_offset}: {e}")
            return [], False

    def process_partner_data(self, data):
        """Process and clean partner data for storage."""
        if not data:
            return None

        partner_details = data.get("partnerDetails", {})
        return {
            "company_id": partner_details.get("id"),
            "name": partner_details.get("name"),
            "description": partner_details.get("description"),
            "website": partner_details.get("url"),
            "linkedin": partner_details.get("linkedInOrganizationProfile"),
            "logo": partner_details.get("logo"),
            "industry_focus": partner_details.get("industryFocus", []),
            "products": partner_details.get("product", []),
            "services": partner_details.get("serviceType", []),
            "solutions": partner_details.get("solutions", []),
            "target_company_sizes": partner_details.get("targetCustomerCompanySizes", []),
            "last_updated": datetime.now()
        }

    def check_id_in_temp_file(self, partner_id):
        """Check if the partner has already been processed."""
        if not os.path.exists(TEMP_FILE):
            return False
        
        with open(TEMP_FILE, 'r') as f:
            processed_ids = f.read().splitlines()
        return partner_id in processed_ids

    def append_id_to_temp_file(self, partner_id):
        """Record processed partner ID."""
        with open(TEMP_FILE, 'a') as f:
            f.write(f"{partner_id}\n")

    async def process_single_alphabet(self, page, alphabet):
        """Process all pages for a single alphabet up to offset 90."""
        self.current_alphabet_count = 0
        page_offset = 0
        
        print(f"\n{'='*50}")
        print(f"Starting processing for alphabet: {alphabet.upper()}")
        print(f"{'='*50}")
        
        while page_offset <= MAX_PAGE_OFFSET:
            partners_data, has_results = await self.search_partners(page, alphabet, page_offset)
            
            if not has_results:
                print(f"\nNo more results for alphabet '{alphabet}' at offset {page_offset}")
                break
            
            for partner_data in partners_data:
                partner_id = partner_data.get("partnerDetails", {}).get("id")
                
                if self.check_id_in_temp_file(partner_id):
                    print(f"Skipping already processed partner: {partner_id}")
                    continue

                processed_data = self.process_partner_data(partner_data)
                if processed_data:
                    self.collection.update_one(
                        {"company_id": partner_id},
                        {"$set": processed_data},
                        upsert=True
                    )
                    self.processed_count += 1
                    self.current_alphabet_count += 1
                    self.append_id_to_temp_file(partner_id)
                    print(f"Processed: {processed_data['name']} (ID: {partner_id})")
                
                await asyncio.sleep(1)  
            
            print(f"\nCompleted offset {page_offset} for alphabet '{alphabet}'")
            print(f"Current alphabet progress: {self.current_alphabet_count} partners")
            print(f"Total partners processed: {self.processed_count}")
            
            page_offset += PAGE_SIZE
        
        print(f"\n{'='*50}")
        print(f"Completed alphabet: {alphabet.upper()}")
        print(f"Total partners for this alphabet: {self.current_alphabet_count}")
        print(f"Overall total: {self.processed_count}")
        print(f"{'='*50}\n")

    async def run(self):
        """Main execution method."""
        print("\nStarting Azure Partner Directory scraping...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            for alphabet in 'abcdefghijklmnopqrstuvwxyz':
                await self.process_single_alphabet(page, alphabet)
                await asyncio.sleep(5) 

            await browser.close()
            print("\nScraping completed!")
            print(f"Total partners processed across all alphabets: {self.processed_count}")

if __name__ == "__main__":
    scraper = AzurePartnerScraper()
    asyncio.run(scraper.run())