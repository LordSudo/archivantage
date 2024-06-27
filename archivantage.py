import requests
from bs4 import BeautifulSoup
import csv
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import re
import time
import datetime

# Set up logging
logging.basicConfig(filename='archivantage.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Constants for the Wayback Machine API and base URL
WAYBACK_API_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_BASE_URL = "http://web.archive.org/web/"

def validate_url(url):
    """
    Validate the URL format.
    
    Parameters:
    url (str): The URL to validate.
    
    Returns:
    bool: True if the URL is valid, False otherwise.
    """
    pattern = re.compile(
        r'^(?:http|https)://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(pattern, url) is not None

def validate_years(years):
    """
    Validate the years input.
    
    Parameters:
    years (str): The years input to validate.
    
    Returns:
    list: A list of valid years as strings.
    """
    return [year.strip() for year in years.split(',') if year.strip().isdigit()]

def validate_output_formats(output_formats):
    """
    Validate the output formats.
    
    Parameters:
    output_formats (str): The output formats to validate.
    
    Returns:
    list: A list of valid output formats.
    """
    valid_formats = ['text', 'csv', 'json', 'html']
    return [fmt.strip().lower() for fmt in output_formats.split(',') if fmt.strip().lower() in valid_formats]

def fetch_snapshots_by_year(url, year, retries=3, backoff_factor=1.0):
    """
    Fetch snapshots from the Wayback Machine for a given URL and year.
    
    Parameters:
    url (str): The URL to fetch snapshots for.
    year (str): The year to filter snapshots by.
    retries (int): Number of retries for failed requests.
    backoff_factor (float): Backoff factor for retries.
    
    Returns:
    list: A list of snapshots, each represented by a [timestamp, original_url] pair.
    """
    params = {
        'url': url,
        'output': 'json',
        'fl': 'timestamp,original',
        'from': f"{year}0101",
        'to': f"{year}1231"
    }
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(WAYBACK_API_URL, params=params)
            response.raise_for_status()
            if response.status_code == 200:
                logging.info(f"Data fetched for the year {year}.")
                return response.json()[1:]
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred: {err}")
            attempt += 1
            if attempt < retries:
                time.sleep(backoff_factor * attempt)
            else:
                logging.error(f"Failed to fetch data for the year {year} after {retries} attempts.")
    return []

def create_output_directory(url):
    """
    Create a directory named after the URL to save the output files.
    
    Parameters:
    url (str): The URL used to name the directory.
    
    Returns:
    str: The path to the created directory.
    """
    directory_name = url.replace('http://', '').replace('https://', '').replace('/', '_').replace('.', '_')
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    return directory_name

def format_timestamp(timestamp):
    """
    Convert a timestamp from 'YYYYMMDDHHMMSS' to a human-readable format.
    
    Parameters:
    timestamp (str): The timestamp to convert.
    
    Returns:
    str: The human-readable timestamp.
    """
    dt = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def output_yearly_snapshots(directory, url, year, snapshots, output_formats, keyword=None):
    """
    Output the fetched snapshots to files in the specified formats.
    
    Parameters:
    directory (str): The directory where the files will be saved.
    url (str): The original URL.
    year (str): The year for which snapshots were fetched.
    snapshots (list): A list of snapshots to output.
    output_formats (list): The formats to output the snapshots (text, csv, json, html).
    keyword (str): The keyword used to search within the snapshots (if any).
    """
    year_directory = os.path.join(directory, year)
    if not os.path.exists(year_directory):
        os.makedirs(year_directory)
    
    if keyword:
        keyword_directory = os.path.join(year_directory, keyword.replace(' ', '_'))
        if not os.path.exists(keyword_directory):
            os.makedirs(keyword_directory)
        base_filename = f"{keyword_directory}/{url.replace('http://', '').replace('https://', '').replace('/', '_')}_{year}_snapshots"
    else:
        base_filename = f"{year_directory}/{url.replace('http://', '').replace('https://', '').replace('/', '_')}_{year}_snapshots"

    for output_format in output_formats:
        if output_format == 'text':
            output_filename = base_filename + ".txt"
            with open(output_filename, "w") as f:
                for snapshot in snapshots:
                    timestamp, original_url = snapshot
                    human_readable_timestamp = format_timestamp(timestamp)
                    if original_url.startswith(WAYBACK_BASE_URL):
                        original_url = original_url[len(WAYBACK_BASE_URL):]
                    wayback_url = f"{WAYBACK_BASE_URL}{timestamp}/{original_url}"
                    f.write(f"{human_readable_timestamp}: {wayback_url}\n")
            logging.info(f"Snapshot links saved to {output_filename}")
            print(f"Snapshot links saved to {output_filename}")
        
        elif output_format == 'csv':
            output_filename = base_filename + ".csv"
            with open(output_filename, "w", newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Timestamp', 'Wayback URL'])
                for snapshot in snapshots:
                    timestamp, original_url = snapshot
                    human_readable_timestamp = format_timestamp(timestamp)
                    if original_url.startswith(WAYBACK_BASE_URL):
                        original_url = original_url[len(WAYBACK_BASE_URL):]
                    wayback_url = f"{WAYBACK_BASE_URL}{timestamp}/{original_url}"
                    writer.writerow([human_readable_timestamp, wayback_url])
            logging.info(f"Snapshot links saved to {output_filename}")
            print(f"Snapshot links saved to {output_filename}")
        
        elif output_format == 'json':
            output_filename = base_filename + ".json"
            snapshot_list = []
            for snapshot in snapshots:
                timestamp, original_url = snapshot
                human_readable_timestamp = format_timestamp(timestamp)
                if original_url.startswith(WAYBACK_BASE_URL):
                    original_url = original_url[len(WAYBACK_BASE_URL):]
                wayback_url = f"{WAYBACK_BASE_URL}{timestamp}/{original_url}"
                snapshot_list.append({'timestamp': human_readable_timestamp, 'wayback_url': wayback_url})
            with open(output_filename, "w") as jsonfile:
                json.dump(snapshot_list, jsonfile, indent=4)
            logging.info(f"Snapshot links saved to {output_filename}")
            print(f"Snapshot links saved to {output_filename}")
        
        elif output_format == 'html':
            output_filename = base_filename + ".html"
            with open(output_filename, "w") as htmlfile:
                htmlfile.write("<html><body>\n")
                htmlfile.write("<h1>Snapshot Links</h1>\n")
                htmlfile.write("<ul>\n")
                for snapshot in snapshots:
                    timestamp, original_url = snapshot
                    human_readable_timestamp = format_timestamp(timestamp)
                    if original_url.startswith(WAYBACK_BASE_URL):
                        original_url = original_url[len(WAYBACK_BASE_URL):]
                    wayback_url = f"{WAYBACK_BASE_URL}{timestamp}/{original_url}"
                    htmlfile.write(f'<li><a href="{wayback_url}">{human_readable_timestamp}</a></li>\n')
                htmlfile.write("</ul>\n")
                htmlfile.write("</body></html>\n")
            logging.info(f"Snapshot links saved to {output_filename}")
            print(f"Snapshot links saved to {output_filename}")
        
        else:
            logging.warning(f"Unsupported output format: {output_format}")
            print(f"Unsupported output format: {output_format}")

def search_keywords_in_snapshots(snapshots, keyword):
    """
    Search for a keyword in the fetched snapshots.
    
    Parameters:
    snapshots (list): A list of snapshots to search within.
    keyword (str): The keyword to search for.
    
    Returns:
    list: A list of snapshots where the keyword was found, each represented by a [timestamp, wayback_url] pair.
    """
    keyword_matches = []
    for snapshot in snapshots:
        timestamp, original_url = snapshot
        wayback_url = f"{WAYBACK_BASE_URL}{timestamp}/{original_url}"
        try:
            response = requests.get(wayback_url)
            response.raise_for_status()
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                if keyword.lower() in soup.get_text().lower():
                    keyword_matches.append((timestamp, wayback_url))
        except requests.exceptions.ConnectionError:
            logging.error("Connection error occurred while searching snapshots. Please check your network and try again.")
            print("Connection error occurred while searching snapshots. Please check your network and try again.")
            break
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:  # HTTP 429 Too Many Requests
                logging.warning("API rate limit reached while searching snapshots. Please try again later.")
                print("API rate limit reached while searching snapshots. Please try again later.")
                break
            else:
                logging.error(f"HTTP error occurred while searching snapshots: {http_err}")
                print(f"HTTP error occurred while searching snapshots: {http_err}")
                break
        except requests.exceptions.Timeout:
            logging.error("Request timed out while searching snapshots. Please try again later.")
            print("Request timed out while searching snapshots. Please try again later.")
            break
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred while searching snapshots: {err}")
            print(f"An error occurred while searching snapshots: {err}")
            break
    return keyword_matches

def process_year(url, year, keywords, output_formats, output_directory):
    """
    Process snapshots for a given year.
    
    Parameters:
    url (str): The URL to fetch snapshots for.
    year (str): The year to fetch snapshots for.
    keywords (list): A list of keywords to search for.
    output_formats (list): The formats to output the snapshots (text, csv, json, html).
    output_directory (str): The directory where the files will be saved.
    """
    year_snapshots = fetch_snapshots_by_year(url, year)
    if year_snapshots:
        if keywords:
            for keyword in keywords:
                keyword = keyword.strip()
                keyword_matches = search_keywords_in_snapshots(year_snapshots, keyword)
                if keyword_matches:
                    output_yearly_snapshots(output_directory, url, year, keyword_matches, output_formats, keyword)
                    logging.info(f"Keyword '{keyword}' found and saved in snapshots for year {year}.")
                    print(f"Keyword '{keyword}' found and saved in snapshots for year {year}.")
                else:
                    logging.info(f"No snapshots contained the keyword '{keyword}' for year {year}.")
                    print(f"No snapshots contained the keyword '{keyword}' for year {year}.")
                    output_yearly_snapshots(output_directory, url, year, year_snapshots, output_formats)
        else:
            output_yearly_snapshots(output_directory, url, year, year_snapshots, output_formats)
    else:
        logging.info(f"No snapshots found for the year {year}.")
        print(f"No snapshots found for the year {year}.")


def print_intro():
    intro_text = """
    -----------------------------------------------------------------------------
                           _      _                       _
        /\                | |    (_)                     | |
       /  \    _ __   ___ | |__   _ __   __  __ _  _ __  | |_   __ _   __ _   ___
      / /\ \  | '__| / __|| '_ \ | |\ \ / / / _` || '_ \ | __| / _` | / _` | / _ \\
     / ____ \ | |   | (__ | | | || | \ V / | (_| || | | || |_ | (_| || (_| ||  __/
    /_/    \_\|_|    \___||_| |_||_|  \_/   \__,_||_| |_| \__| \__,_| \__, | \___|
                                                                       __/ |
                                                                      |___/
                 A gateway to analyze and explore the web archives
                                    Version 1
    -----------------------------------------------------------------------------
    """
    print(intro_text)


if __name__ == "__main__":
# Print the intro
    print_intro()

    # Prompt the user to enter a URL and multiple years
    while True:
        url = input("Enter URL (must start with http:// or https://): ")
        if validate_url(url):
            break
        else:
            print("Invalid URL. Please enter a valid URL that starts with http:// or https://.")

    while True:
        years = input("Enter years to fetch snapshots (comma-separated, e.g., 2004,2008): ")
        valid_years = validate_years(years)
        if valid_years:
            break
        else:
            print("Invalid years. Please enter a comma-separated list of valid years.")

    keywords = input("Enter keywords to search for (comma-separated, leave empty to skip): ").split(',')

    while True:
        output_formats = input("Enter output formats (comma-separated, choose from text, csv, json, html): ")
        valid_output_formats = validate_output_formats(output_formats)
        if valid_output_formats:
            break
        else:
            print("Invalid output formats. Please enter a comma-separated list of valid formats (text, csv, json, html).")

    # Create output directory
    output_directory = create_output_directory(url)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_year, url, year.strip(), keywords, valid_output_formats, output_directory) for year in valid_years]

    for future in futures:
        future.result()

print("All tasks are complete.")
print("Thank you for using ArchiVantage! You are now part of digital history.")
