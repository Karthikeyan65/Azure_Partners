import asyncio
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
import requests
from pymongo import MongoClient
import os

BASE_URL = "https://appsource.microsoft.com/en-us/marketplace/partner-dir"

API_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners"
PAGE_SIZE = 18
RETRY_DELAY = 30
TIMEOUT = 60 * 1000
MAX_RETRIES = 5
RADIUS = 100  
LOCATION_NOT_REQUIRED = True  
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Azure_partners_db"
COLLECTION_NAME = "info"

TEMP_FILE = "processed_ids.txt"

class AzurePartnerScraper:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self.processed_count = 0

    async def search_appsource_alphabets(self, page, alphabet):
        """Search AppSource using a given alphabet by modifying the URL."""
        print(f"Initiating AppSource search for alphabet '{alphabet}' using URL...")
        
        try:
            search_url = f"{BASE_URL}?filter=sort%3D0%3BpageSize%3D18%3Bradius%3D100%3Bfreetext%3D{alphabet}%3Bsuggestion%3Dtrue%3BlocationNotRequired%3Dtrue"

            await page.goto(search_url, timeout=60000)

            print(f"AppSource search for alphabet '{alphabet}' using URL executed successfully!")
            await asyncio.sleep(5)  

        except Exception as e:
            print(f"Error during AppSource search for alphabet '{alphabet}' using URL: {e}")

    async def fetch_batch(self, page_offset):
        """Fetch a batch of company IDs from the API."""
        url = f"{API_URL}?filter=sort%3D0%3BpageSize%3D{PAGE_SIZE}%3BpageOffset%3D{page_offset}%3Bradius%3D100%3Bfreetext%3Db%3Bsuggestion%3Dtrue%3BlocationNotRequired%3Dtrue"
        print("Fetching batch...")
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    company_ids = [item['partnerId'] for item in data.get('matchingPartners', {}).get('items', [])]
                    return company_ids
                print(f"Error {response.status_code} - Attempt {attempt + 1}/{MAX_RETRIES}")
            except Exception as e:
                print(f"Error fetching batch: {e} - Attempt {attempt + 1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY)
        return []

    def process_partner_data(self, data):
        """Process and clean partner data for storage."""
        if not data:
            return None

        processed_data = {
            "company_id": data.get("partnerDetails", {}).get("id", "Not available"),  
            "Name": data.get("partnerDetails", {}).get("name", "Not available"),
            "Description": data.get("partnerDetails", {}).get("description", "Not available"),
            "Website": data.get("partnerDetails", {}).get("url", "Not available"),
            "Linkedin": data.get("partnerDetails", {}).get("linkedInOrganizationProfile", "Not available"),
            "Industry_focus": data.get("partnerDetails", {}).get("industryFocus", "Not available"),
            "Logo": data.get("partnerDetails", {}).get("logo", "Not available"),
            "Products": data.get("partnerDetails", {}).get("product", "Not available"),
            "Services": data.get("partnerDetails", {}).get("serviceType", "Not available"),
            "Solutions": data.get("partnerDetails", {}).get("solutions", "Not available"),
            "Target_company_sizes": data.get("partnerDetails", {}).get("targetCustomerCompanySizes", "Not available"),
            "Last_modified": datetime.now()
        }
        return processed_data


    async def fetch_company_details(self, company_id, alphabet):
        """Fetch detailed information for a company using the given URL."""
        detailed_url = f"https://appsource.microsoft.com/en-us/marketplace/partner-dir?filter=sort%3D0%3BpageSize%3D{PAGE_SIZE}%3Bradius%3D{RADIUS}%3Bfreetext%3D{alphabet}%3Bsuggestion%3Dtrue%3BlocationNotRequired%3D"

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(detailed_url, timeout=30)
                if response.status_code == 200:
                 
                    return response.text
                else:
                    print(f"Error fetching details for company {company_id}: {response.status_code} - Attempt {attempt + 1}/{MAX_RETRIES}")
            except requests.exceptions.RequestException as e:
                print(f"Request error for company {company_id}: {e} - Attempt {attempt + 1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY)  
        return None

    async def process_batch(self, company_ids, alphabet):
        """Process a batch of company IDs and store their details in the DB."""
        for company_id in company_ids:
            if self.check_id_in_temp_file(company_id):
                print(f"Company ID {company_id} already processed. Skipping...")
                continue

            try:
                partner_data = await self.fetch_company_details(company_id, alphabet)
                if partner_data:
                    processed_data = self.process_partner_data({"partnerDetails": {"id": company_id, "name": "name"}})  
                    if processed_data:
                        self.collection.update_one(
                            {"company_id": processed_data["company_id"]},
                            {"$set": processed_data},
                            upsert=True  
                        )
                        self.processed_count += 1
                        print(f"Processed and stored data for company {processed_data['name']}")
                        self.append_id_to_temp_file(company_id)
            except Exception as e:
                print(f"Error processing company {company_id}: {e}")
            await asyncio.sleep(1)  

    def check_id_in_temp_file(self, company_id):
        """Check if the company ID is already in the temporary file."""
        if not os.path.exists(TEMP_FILE):
            return False  
    
        try:
            with open(TEMP_FILE, 'r') as f:
                processed_ids = f.read().splitlines()
        except FileNotFoundError:
            return False
        
        return company_id in processed_ids

    def append_id_to_temp_file(self, company_id):
        """Append the processed company ID to the temporary file."""
        with open(TEMP_FILE, 'a') as f:
            f.write(company_id + '\n')

    async def run(self):
        """Main execution method."""
        print("Starting Azure Partner Directory scraping...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            for alphabet in 'abcdefghijklmnopqrstuvwxyz':
                await self.search_appsource_alphabets(page, alphabet)

                page_offset = 0
                while page_offset <= 90:  
                    print(f"Fetching batch at offset {page_offset} for alphabet '{alphabet}'...")
                    company_ids = await self.fetch_batch(page_offset)
                    
                    if not company_ids:
                        print(f"No more companies to process for alphabet '{alphabet}'.")
                        break
                    
                    print(f"Processing batch of {len(company_ids)} companies for alphabet '{alphabet}'...")
                    await self.process_batch(company_ids, alphabet)
                    
                    print(f"Completed batch for alphabet '{alphabet}'. Total processed: {self.processed_count}")
                    page_offset += PAGE_SIZE

            print(f"Scraping completed. Total partners processed: {self.processed_count}")
            await browser.close()  

if __name__ == "__main__":
    scraper = AzurePartnerScraper()
    asyncio.run(scraper.run())
