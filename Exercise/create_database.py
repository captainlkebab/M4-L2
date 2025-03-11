import sqlite3
import pandas as pd
import os

# Define the database file path
DB_PATH = 'news_articles.db'

# Define the CSV file path
CSV_PATH = 'Exercise\scraped_data.csv'

def create_database():
    """Create the SQLite3 database with the required tables."""
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create Articles table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Articles (
        article_id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT,
        label TEXT,
        theme TEXT,
        badge TEXT,
        datetime TEXT NOT NULL,
        author TEXT,
        text TEXT NOT NULL
    )
    ''')
    
    # Create Reports table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date TEXT UNIQUE NOT NULL,
        content TEXT NOT NULL
    )
    ''')
    
    # Create Article_Report_Link table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Article_Report_Link (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id INTEGER,
        report_id INTEGER,
        FOREIGN KEY (article_id) REFERENCES Articles(article_id) ON DELETE CASCADE,
        FOREIGN KEY (report_id) REFERENCES Reports(report_id) ON DELETE CASCADE
    )
    ''')
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("Database and tables created successfully.")

def import_csv_data():
    """Import data from the CSV file into the Articles table."""
    # Check if CSV file exists
    if not os.path.exists(CSV_PATH):
        print(f"Error: CSV file not found at {CSV_PATH}")
        return
    
    # Read CSV file
    df = pd.read_csv(CSV_PATH, encoding='utf-8')  # Added explicit encoding
    print(f"Found {len(df)} articles in CSV file")
    
    # Replace NaN values with None (which will be converted to NULL in SQLite)
    df = df.where(pd.notnull(df), None)
    
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Counter for successful imports
    success_count = 0
    
    # Insert data into Articles table, skipping duplicates
    for _, row in df.iterrows():
        try:
            cursor.execute('''
            INSERT INTO Articles (url, title, label, theme, badge, datetime, author, text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['url'],
                row['title'],
                row['label'],
                row['theme'],
                row['badge'],
                row['datetime'],
                row['author'],
                row['text']
            ))
            success_count += 1
        except sqlite3.IntegrityError as e:
            print(f"Skipping duplicate article with URL: {row['url']}")
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error importing article: {row['url']}")
            print(f"Error: {e}")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print(f"Successfully imported {success_count} out of {len(df)} articles from CSV.")

def add_sample_report():
    """Add a sample report and link it to some articles."""
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Add a sample report
    report_date = "2025-03-10"
    report_content = "This is a sample daily report summarizing key events from March 9, 2025. " \
                    "The report covers various topics including the situation in Syria, " \
                    "analysis of Europe-US relations, and developments in Germany."
    
    cursor.execute('''
    INSERT INTO Reports (report_date, content)
    VALUES (?, ?)
    ''', (report_date, report_content))
    
    # Get the report_id
    report_id = cursor.lastrowid
    
    # Get IDs of some articles to link to the report (first 3 articles)
    cursor.execute("SELECT article_id FROM Articles LIMIT 3")
    article_ids = [row[0] for row in cursor.fetchall()]
    
    # Link articles to the report
    for article_id in article_ids:
        cursor.execute('''
        INSERT INTO Article_Report_Link (article_id, report_id)
        VALUES (?, ?)
        ''', (article_id, report_id))
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print(f"Added sample report dated {report_date} and linked it to {len(article_ids)} articles.")

def main():
    """Main function to execute all steps."""
    create_database()
    import_csv_data()
    add_sample_report()
    print("Database setup complete!")

if __name__ == "__main__":
    main()