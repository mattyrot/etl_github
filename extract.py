import os
import json
import asyncio
import logging
import httpx
from typing import List, Dict, Any, Optional
from airflow.models import Variable
from datetime import datetime

# --- Configuration ---
# GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_TOKEN = Variable.get("GITHUB_TOKEN", default_var=None)
REPO_OWNER = "home-assistant"
REPO_NAME = "core"
OUTPUT_DIR = "data"
LIMIT_PRS = 100
MAX_CONCURRENT_REQUESTS = 10

logger = logging.getLogger(__name__)

# Silence httpx logs so they don't spam Airflow logs
logging.getLogger("httpx").setLevel(logging.WARNING)

class GitHubExtractor:
    def __init__(self, token: str, owner: str, repo: str, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        self.base_url = f"https://api.github.com/repos/{owner}/{repo}"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Python-Async-Scraper"
        }
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def _make_request(self, client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]:
        url = f"{self.base_url}/{endpoint}"
        retries = 3
        backoff_factor = 2

        for attempt in range(retries):
            try:
                async with self.semaphore:
                    response = await client.get(url, params=params)

                if response.status_code in [403, 429]:
                    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
                    if remaining == 0:
                        reset_timestamp = int(response.headers.get("X-RateLimit-Reset", 0))
                        current_time = datetime.now().timestamp()
                        sleep_time = max(reset_timestamp - current_time, 0) + 1
                        logger.warning(f"Rate limit hit! Sleeping for {sleep_time:.1f} seconds...")
                        await asyncio.sleep(sleep_time)
                        continue

                response.raise_for_status()
                return response.json()

            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.error(f"Request Error: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff_factor ** attempt)
                else:
                    return None
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None

    async def fetch_pr_metadata(self, client: httpx.AsyncClient, state: str = "closed", limit: int = 100) -> List[Dict]:
        prs = []
        page = 1
        per_page = 50
        
        logger.info(f"Fetching metadata for {limit} '{state}' PRs...")

        while len(prs) < limit:
            data = await self._make_request(client, "pulls", params={"state": state, "per_page": per_page, "page": page})
            if not data: break
            
            for item in data:
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
                if len(prs) >= limit: break
            page += 1
        
        logger.info(f"Fetched {len(prs)} PRs. Starting enrichment...")
        return prs

    async def fetch_pr_details(self, client: httpx.AsyncClient, pr: Dict) -> Dict:
        pr_num = pr["number"]
        head_sha = pr["head_sha"]

        reviews_task = self._make_request(client, f"pulls/{pr_num}/reviews")
        status_task = self._make_request(client, f"commits/{head_sha}/status")
        commits_task = self._make_request(client, f"pulls/{pr_num}/commits")

        review_data, status_data, commit_data = await asyncio.gather(reviews_task, status_task, commits_task)

        reviews = []
        approved_count = 0
        if review_data:
            for r in review_data:
                state = r.get("state")
                reviews.append({"state": state})
                if state == "APPROVED": approved_count += 1

        statuses = []
        if status_data and "statuses" in status_data:
            for s in status_data["statuses"]:
                statuses.append({
                    "name": s.get("context"),
                    "conclusion": s.get("state"),
                    "completed_at": s.get("updated_at")
                })

        commits_info = []
        if commit_data:
            for c in commit_data:
                commits_info.append({
                    "sha": c.get("sha"),
                    "author_name": c.get("commit", {}).get("author", {}).get("name")
                })

        pr["reviews"] = reviews
        pr["approved_review_count"] = approved_count
        pr["status_checks"] = statuses
        pr["commits"] = commits_info
        pr["commit_count"] = len(commits_info)

        return pr

    async def run(self) -> str:
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            raw_prs = await self.fetch_pr_metadata(client, limit=LIMIT_PRS)
            
            logger.info(f"Starting async enrichment for {len(raw_prs)} PRs...")
            tasks = [self.fetch_pr_details(client, pr) for pr in raw_prs]
            
            enriched_data = []
            for i, task in enumerate(asyncio.as_completed(tasks)):
                result = await task
                enriched_data.append(result)
                if (i + 1) % 10 == 0:
                    logger.info(f"Enriched {i + 1}/{len(raw_prs)} PRs...")

            # Save to Local JSON using Absolute Path
            abs_output_dir = os.path.abspath(self.output_dir)
            if not os.path.exists(abs_output_dir):
                os.makedirs(abs_output_dir)
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(abs_output_dir, f"prs_extract_{timestamp}.json")
            
            with open(filename, "w", encoding='utf-8') as f:
                json.dump(enriched_data, f, indent=4, ensure_ascii=False)
                
            logger.info(f"Process Complete. Data saved to: {filename}")
            
            return filename

if __name__ == "__main__":
    if not GITHUB_TOKEN:
        raise ValueError("Token missing")
    extractor = GitHubExtractor(GITHUB_TOKEN, REPO_OWNER, REPO_NAME)
    asyncio.run(extractor.run())