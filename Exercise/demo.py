import db_utils
import pandas as pd
from datetime import datetime

def display_database_info():
    """Display information about the database contents."""
    # Get all articles
    articles = db_utils.get_all_articles()
    print(f"\nTotal articles in database: {len(articles)}")
    if not articles.empty:
        print("\nSample articles:")
        print(articles[['article_id', 'title', 'datetime']].head())
    
    # Get all reports
    reports = db_utils.get_all_reports()
    print(f"\nTotal reports in database: {len(reports)}")
    if not reports.empty:
        print("\nSample reports:")
        print(reports[['report_id', 'report_date']].head())
        
        # For the first report, show linked articles
        if len(reports) > 0:
            report_id = reports.iloc[0]['report_id']
            linked_articles = db_utils.get_articles_for_report(report_id)
            print(f"\nArticles linked to report {report_id}:")
            if not linked_articles.empty:
                print(linked_articles[['article_id', 'title']].head())
            else:
                print("No linked articles found.")

def add_new_article():
    """Add a new article to the database."""
    # Sample article data
    article_data = {
        'url': 'https://www.example.com/new-article',
        'title': 'New Sample Article',
        'label': 'Breaking News',
        'theme': 'Technology',
        'badge': 'Exclusive',
        'datetime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        'author': 'John Doe',
        'text': 'This is a sample article text for demonstration purposes.'
    }
    
    # Insert the article
    article_id = db_utils.insert_article(
        article_data['url'],
        article_data['title'],
        article_data['label'],
        article_data['theme'],
        article_data['badge'],
        article_data['datetime'],
        article_data['author'],
        article_data['text']
    )
    
    return article_id

def add_new_report_and_link():
    """Add a new report and link it to articles."""
    # Sample report data
    report_data = {
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'content': 'This is a new sample report summarizing recent articles.'
    }
    
    # Insert the report
    report_id = db_utils.insert_report(
        report_data['report_date'],
        report_data['content']
    )
    
    if report_id:
        # Get some article IDs to link to the report
        articles = db_utils.get_all_articles()
        if not articles.empty:
            # Link the first 2 articles to the report
            for article_id in articles['article_id'].head(2):
                db_utils.link_article_to_report(article_id, report_id)

def main():
    """Main function to demonstrate database functionality."""
    print("Database Demonstration")
    print("=====================")
    
    # Display initial database info
    print("\n--- Initial Database State ---")
    display_database_info()
    
    # Add a new article
    print("\n--- Adding a New Article ---")
    add_new_article()
    
    # Add a new report and link it to articles
    print("\n--- Adding a New Report and Linking Articles ---")
    add_new_report_and_link()
    
    # Display updated database info
    print("\n--- Updated Database State ---")
    display_database_info()

if __name__ == "__main__":
    main()