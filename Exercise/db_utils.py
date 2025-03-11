import sqlite3
import pandas as pd

# Define the database file path
DB_PATH = 'news_articles.db'

def get_connection():
    """Get a connection to the database."""
    return sqlite3.connect(DB_PATH)

def insert_article(url, title, label, theme, badge, datetime, author, text):
    """Insert a new article into the database."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT INTO Articles (url, title, label, theme, badge, datetime, author, text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (url, title, label, theme, badge, datetime, author, text))
        
        article_id = cursor.lastrowid
        conn.commit()
        print(f"Article inserted successfully with ID: {article_id}")
        return article_id
    except sqlite3.IntegrityError:
        print(f"Article with URL '{url}' already exists in the database.")
        return None
    finally:
        conn.close()

def insert_report(report_date, content):
    """Insert a new report into the database."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT INTO Reports (report_date, content)
        VALUES (?, ?)
        ''', (report_date, content))
        
        report_id = cursor.lastrowid
        conn.commit()
        print(f"Report inserted successfully with ID: {report_id}")
        return report_id
    except sqlite3.IntegrityError:
        print(f"Report for date '{report_date}' already exists in the database.")
        return None
    finally:
        conn.close()

def link_article_to_report(article_id, report_id):
    """Link an article to a report."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT INTO Article_Report_Link (article_id, report_id)
        VALUES (?, ?)
        ''', (article_id, report_id))
        
        link_id = cursor.lastrowid
        conn.commit()
        print(f"Article {article_id} linked to report {report_id} with link ID: {link_id}")
        return link_id
    except sqlite3.IntegrityError as e:
        print(f"Error linking article to report: {e}")
        return None
    finally:
        conn.close()

def get_all_articles():
    """Get all articles from the database."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Articles", conn)
    conn.close()
    return df

def get_all_reports():
    """Get all reports from the database."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Reports", conn)
    conn.close()
    return df

def get_articles_for_report(report_id):
    """Get all articles linked to a specific report."""
    conn = get_connection()
    query = """
    SELECT a.* FROM Articles a
    JOIN Article_Report_Link l ON a.article_id = l.article_id
    WHERE l.report_id = ?
    """
    df = pd.read_sql_query(query, conn, params=(report_id,))
    conn.close()
    return df

def get_reports_for_article(article_id):
    """Get all reports linked to a specific article."""
    conn = get_connection()
    query = """
    SELECT r.* FROM Reports r
    JOIN Article_Report_Link l ON r.report_id = l.report_id
    WHERE l.article_id = ?
    """
    df = pd.read_sql_query(query, conn, params=(article_id,))
    conn.close()
    return df