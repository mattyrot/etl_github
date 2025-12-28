import os
import json
import time
import logging
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- Configuration ---
# Hardcoded token as requested (Best practice: use os.getenv("GITHUB_TOKEN"))
GITHUB_TOKEN = "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"

REPO_OWNER = "home-assistant"
REPO_NAME = "core"
OUTPUT_DIR = "data"

# With a token, we can safely fetch more PRs.
LIMIT_PRS = 100 

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GitHubExtractor")

class GitHubExtractor:
    def __init__(self, token: str, owner: str, repo: str):
        self.base_url = f"https://api.github.com/repos/{owner}/{repo}"
        self.session = requests.Session()
        
        # Headers: Auth + Accept type
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]:
        """
        Executes HTTP GET with retry logic and rate limit handling.
        """
        url = f"{self.base_url}/{endpoint}"
        retries = 3
        backoff_factor = 2

        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params)
                
                # Rate Limit Handling 
                if response.status_code in [403, 429]:
                    # Check if rate limit issue
                    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
                    if remaining == 0:
                        reset_timestamp = int(response.headers.get("X-RateLimit-Reset", 0))
                        sleep_time = max(reset_timestamp - time.time(), 0) + 1
                        logger.warning(f"Rate limit hit! Sleeping for {sleep_time:.1f} seconds...")
                        time.sleep(sleep_time)
                        continue 

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request Error (Attempt {attempt+1}/{retries}) for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(backoff_factor ** attempt)
                else:
                    return None

    def fetch_pr_metadata(self, state: str = "closed", limit: int = 100) -> List[Dict]:
        """
        Fetches the base list of PRs handling pagination.
        """
        prs = []
        page = 1
        per_page = 50 
        
        logger.info(f"Step 1: Fetching metadata for {limit} '{state}' PRs...")

        while len(prs) < limit:
            data = self._make_request("pulls", params={"state": state, "per_page": per_page, "page": page})
            
            if not data:
                break 
            
            for item in data:
                # Extract Metadata
                pr_meta = {
                    "number": item.get("number"),
                    "title": item.get("title"),
                    "state": item.get("state"),
                    "merged_at": item.get("merged_at"),
                    "user_login": item.get("user", {}).get("login"),
                    "user_id": item.get("user", {}).get("id"),
                    "base_branch": item.get("base", {}).get("ref"),
                    "head_branch": item.get("head", {}).get("ref"),
                    "head_sha": item.get("head", {}).get("sha") 
                }
                prs.append(pr_meta)
                if len(prs) >= limit:
                    break
            
            page += 1

        logger.info(f"Fetched {len(prs)} PRs. Starting enrichment...")
        return prs

    def fetch_pr_details(self, pr: Dict) -> Dict:
        """
        Enriches a PR with Reviews, Status, and Commits.
        """
        pr_num = pr["number"]
        head_sha = pr["head_sha"]

        # Review Information
        reviews_data = self._make_request(f"pulls/{pr_num}/reviews")
        reviews = []
        approved_count = 0
        
        if reviews_data:
            for r in reviews_data:
                state = r.get("state")
                reviews.append({"state": state})
                if state == "APPROVED":
                    approved_count += 1

        # Assignment asks for "Combined status" (commits/{ref}/status)
        status_data = self._make_request(f"commits/{head_sha}/status")
        statuses = []
        
        if status_data and "statuses" in status_data:
            for s in status_data["statuses"]:
                statuses.append({
                    "name": s.get("context"),
                    "conclusion": s.get("state"), 
                    "completed_at": s.get("updated_at")
                })

        # Associated Commits
        commits_data = self._make_request(f"pulls/{pr_num}/commits")
        commits_info = []
        
        if commits_data:
            for c in commits_data:
                commits_info.append({
                    "sha": c.get("sha"),
                    "author_name": c.get("commit", {}).get("author", {}).get("name")
                })

        #  Assemble Data Structure
        pr["reviews"] = reviews
        pr["approved_review_count"] = approved_count
        pr["status_checks"] = statuses
        pr["commits"] = commits_info
        pr["commit_count"] = len(commits_info)

        return pr

    def run(self):
        # Fetch Metadata
        raw_prs = self.fetch_pr_metadata(limit=LIMIT_PRS)
        
        enriched_data = []
        
        # Enrich Data
        # Increased workers to 5 since we have a token
        logger.info(f"Starting concurrent enrichment for {len(raw_prs)} PRs...")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_pr = {executor.submit(self.fetch_pr_details, pr): pr for pr in raw_prs}
            
            for i, future in enumerate(as_completed(future_to_pr)):
                try:
                    result = future.result()
                    enriched_data.append(result)
                    if (i + 1) % 10 == 0:
                        logger.info(f"Enriched {i + 1}/{len(raw_prs)} PRs...")
                except Exception as exc:
                    logger.error(f"Generated an exception: {exc}")

        # 3. Save to Local JSON
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{OUTPUT_DIR}/prs_extract_{timestamp}.json"
        
        with open(filename, "w", encoding='utf-8') as f:
            json.dump(enriched_data, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Process Complete. Data saved to: {filename}")

if __name__ == "__main__":
    extractor = GitHubExtractor(GITHUB_TOKEN, REPO_OWNER, REPO_NAME)
    extractor.run()