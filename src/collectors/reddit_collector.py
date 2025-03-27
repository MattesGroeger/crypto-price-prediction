import os
import praw
import json
import logging
from datetime import datetime
import time
from typing import Dict, Any, List
from praw.models import Submission, Comment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedditCollector:
    def __init__(self, callback=None):
        self.client_id = os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = os.getenv('REDDIT_USER_AGENT')
        
        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Reddit API credentials not found in environment variables")
            
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )
        
        self.subreddits = ['CryptoCurrency', 'CryptoMarkets', 'Bitcoin']
        self.keywords = ['BTC', 'ETH', 'BNB', 'crypto', 'bitcoin', 'ethereum']
        self.callback = callback
        self.running = False

    def collect_subreddit_data(self, subreddit_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Collect posts and comments from a specific subreddit"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            posts = []
            
            for submission in subreddit.hot(limit=limit):
                if self._is_relevant(submission.title):
                    post_data = self._process_submission(submission)
                    posts.append(post_data)
            
            return posts
        except Exception as e:
            logger.error(f"Error collecting data from subreddit {subreddit_name}: {e}")
            return []

    def _is_relevant(self, text: str) -> bool:
        """Check if text contains relevant keywords"""
        return any(keyword.lower() in text.lower() for keyword in self.keywords)

    def _process_submission(self, submission: Submission) -> Dict[str, Any]:
        """Process a submission and its comments"""
        try:
            data = {
                'id': submission.id,
                'title': submission.title,
                'text': submission.selftext,
                'score': submission.score,
                'created_utc': datetime.fromtimestamp(submission.created_utc).isoformat(),
                'num_comments': submission.num_comments,
                'url': submission.url,
                'comments': [],
                'subreddit': submission.subreddit.display_name,
                'source': 'reddit'
            }

            # Process top-level comments
            submission.comments.replace_more(limit=0)  # Remove MoreComments objects
            for comment in submission.comments.list()[:10]:  # Limit to top 10 comments
                if isinstance(comment, Comment):
                    comment_data = {
                        'id': comment.id,
                        'text': comment.body,
                        'score': comment.score,
                        'created_utc': datetime.fromtimestamp(comment.created_utc).isoformat()
                    }
                    data['comments'].append(comment_data)

            if self.callback:
                self.callback(json.dumps(data))
            logger.info(f"Processed Reddit post from r/{data['subreddit']}")
            
            return data
        except Exception as e:
            logger.error(f"Error processing submission {submission.id}: {e}")
            return None

    def stream_subreddit(self, subreddit_name: str, callback):
        """Stream new submissions from a subreddit"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            for submission in subreddit.stream.submissions():
                if self._is_relevant(submission.title):
                    data = self._process_submission(submission)
                    if data:
                        callback(data)
        except Exception as e:
            logger.error(f"Error streaming subreddit {subreddit_name}: {e}")

    def process_submission(self, submission):
        try:
            data = {
                'id': submission.id,
                'title': submission.title,
                'text': submission.selftext,
                'score': submission.score,
                'created_utc': datetime.fromtimestamp(submission.created_utc).isoformat(),
                'subreddit': submission.subreddit.display_name,
                'url': submission.url,
                'source': 'reddit'
            }
            
            if self.callback:
                self.callback(json.dumps(data))
            logger.info(f"Processed Reddit post from r/{data['subreddit']}")
            
        except Exception as e:
            logger.error(f"Error processing Reddit submission: {e}")

    def start(self):
        self.running = True
        logger.info("Starting Reddit collector")
        
        try:
            while self.running:
                for subreddit_name in self.subreddits:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    for submission in subreddit.hot(limit=10):
                        if not self.running:
                            break
                        self.process_submission(submission)
                    time.sleep(1)  # Rate limiting
                time.sleep(60)  # Wait before next cycle
                
        except Exception as e:
            logger.error(f"Error in Reddit collector: {e}")
            self.stop()

    def stop(self):
        self.running = False
        logger.info("Reddit collector stopped") 