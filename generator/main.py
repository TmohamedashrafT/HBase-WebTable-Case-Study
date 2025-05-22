
import happybase
import random
from faker import Faker
from datetime import datetime
from typing import Dict, List, Optional

# Constants
DOMAINS = ['example.com', 'test.org', 'demo.net', 'mysite.io', 'sample.co']
SUBDOMAINS = ['www', 'blog', 'news', 'shop', 'api']
NUMBERED_PATHS = ['posts', 'articles', 'page', 'entry']
STATUS_CODES = [200, 404, 500]
HTML_TAGS = ['p', 'div', 'span', 'h2', 'h3']

# Configuration
DEFAULT_HBASE_HOST = 'hmaster'
DEFAULT_TABLE_NAME = 'web_pages'
BATCH_SIZE = 100


def connect_to_hbase(host: str = DEFAULT_HBASE_HOST,
                     table_name: str = DEFAULT_TABLE_NAME,
                     retries: int = 3) -> Optional[happybase.Table]:
    """Establish connection to HBase with retry logic"""
    for attempt in range(retries):
        try:
            connection = happybase.Connection(host)
            return connection.table(table_name)
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == retries - 1:
                raise
    return None


def generate_random_path() -> str:
    """Generate either a numbered path or random URI path with zero-padded numbers"""
    if random.random() < 0.5:
        base = random.choice(NUMBERED_PATHS)
        number = random.randint(1, 50)
        # Pad number with leading zeros to 3 digits (001, 050, etc.)
        padded_number = f"{number:03d}"
        return f"{base}/{padded_number}"
    else:
        path = Faker().uri_path()
        # Check if path ends with a number
        parts = path.split('/')
        if parts[-1].isdigit():
            # Pad the number at the end to 3 digits
            parts[-1] = f"{int(parts[-1]):03d}"
            return '/'.join(parts)
        return path

def generate_html_content(title: str) -> str:
    """Generate realistic HTML content with proper structure"""
    paragraphs = [f"<p>{Faker().paragraph()}</p>" for _ in range(random.randint(2, 5))]
    list_items = "".join([f"<li>{Faker().word()}</li>" for _ in range(random.randint(3, 7))])

    extra_elements = []
    for _ in range(random.randint(1, 3)):
        tag = random.choice(HTML_TAGS)
        content = Faker().sentence() if tag in ['h2', 'h3'] else Faker().paragraph()
        extra_elements.append(f"<{tag}>{content}</{tag}>")

    return f"""<!DOCTYPE html>
<html>
    <head><title>{title}</title><meta charset="utf-8"></head>
    <body>
        <h1>{title}</h1>
        {''.join(paragraphs)}
        <ul>{list_items}</ul>
        {''.join(extra_elements)}
    </body>
</html>""".strip()


def generate_random_links(domain_list: List[str], count: int) -> List[str]:
    """Generate a list of random links"""
    return [f"http://{random.choice(domain_list)}/{Faker().uri_path()}"
            for _ in range(count)]


def create_row_key(full_domain: str, path: str) -> str:
    """Create HBase row key from domain and path"""
    return f"{'.'.join(reversed(full_domain.split('.')))}#{path}"


def generate_page_data(domain: str) -> Dict[str, str]:
    """Generate complete page data dictionary"""
    subdomain = random.choice(SUBDOMAINS)
    full_domain = f"{subdomain}.{domain}"
    path = generate_random_path()
    title = Faker().sentence()
    html_content = generate_html_content(title)

    return {
        'row_key': create_row_key(full_domain, path),
        'content:html': html_content,
        'metadata:title': title,
        'metadata:created': Faker().date_time_between(start_date='-100d', end_date='now').isoformat(),
        'metadata:status': str(random.choice(STATUS_CODES)),
        'metadata:size': str(len(html_content.encode('utf-8'))),
        'outlinks:urls': ','.join(generate_random_links(DOMAINS, random.randint(0, 3))),
        'inlinks:urls': ','.join(generate_random_links(DOMAINS, random.randint(0, 3)))
    }


def generate_sample_data(num_records: int) -> List[Dict[str, str]]:
    """Generate multiple page records"""
    return [generate_page_data(random.choice(DOMAINS)) for _ in range(num_records)]


def insert_to_hbase(table: happybase.Table, data: List[Dict[str, str]]) -> None:
    """Insert batch of records into HBase"""
    with table.batch(batch_size=BATCH_SIZE) as batch:
        for record in data:
            try:
                batch.put(
                    record['row_key'],
                    {k.encode('utf-8'): v.encode('utf-8')
                     for k, v in record.items() if k != 'row_key'}
                )
            except Exception as e:
                print(f"Failed to insert record {record['row_key']}: {str(e)}")


def main(num_records: int = 20, display: bool = True) -> None:
    """Main execution flow"""
    # Generate sample data
    sample_data = generate_sample_data(num_records)

    if display:
        for i, record in enumerate(sample_data, 1):
            print(f"\nRecord {i}:")
            print(f"Row Key: {record['row_key']}")
            print(f"Title: {record['metadata:title']}")
            print(f"Status: {record['metadata:status']}")
            print(f"Size: {record['metadata:size']} bytes")

    # Insert to HBase
    try:
        table = connect_to_hbase()
        if table:
            print(f"\nInserting {num_records} records to HBase...")
            insert_to_hbase(table, sample_data)
            print("Insertion completed successfully")
        else:
            print("Failed to connect to HBase")
    except Exception as e:
        print(f"Error during HBase operations: {str(e)}")


if __name__ == '__main__':

    num_records = 20
    no_display = True
    main(num_records=num_records, display=no_display)