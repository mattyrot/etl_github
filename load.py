import os
import json
import asyncio
import logging
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime

# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    raise ValueError("Error: GITHUB_TOKEN environment variable is missing.")

REPO_OWNER = "home-assistant"
REPO_NAME = "core"
OUTPUT_DIR = "data"
LIMIT_PRS = 100

# Control how many requests happen at the EXACT same time to avoid GitHub Abuse Detection
MAX_CONCURRENT_REQUESTS = 10 

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
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Python-Async-Scraper"
        }
        # Semaphore limits the number of active concurrent tasks
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def _make_request(self, client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]:
        """
        Executes HTTP GET asynchronously with retry logic and rate limit handling.
        """
        url = f"{self.base_url}/{endpoint}"
        retries = 3
        backoff_factor = 2

        for attempt in range(retries):
            try:
                # We acquire the semaphore before making the request
                async with self.semaphore:
                    response = await client.get(url, params=params)

                # Rate Limit Handling (403/429)
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

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error {e.response.status_code} for {url}")
                return None
            except httpx.RequestError as e:
                logger.error(f"Request Error (Attempt {attempt+1}/{retries}) for {url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff_factor ** attempt)
                else:
                    return None
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None

    async def fetch_pr_metadata(self, client: httpx.AsyncClient, state: str = "closed", limit: int = 100) -> List[Dict]:
        """
        Fetches the base list of PRs handling pagination.
        """
        prs = []
        page = 1
        per_page = 50
        
        logger.info(f"Fetching metadata for {limit} '{state}' PRs...")

        while len(prs) < limit:
            data = await self._make_request(client, "pulls", params={"state": state, "per_page": per_page, "page": page})
            
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

    async def fetch_pr_details(self, client: httpx.AsyncClient, pr: Dict) -> Dict:
        """
        Enriches a PR with Reviews, Status, and Commits.
        """
        pr_num = pr["number"]
        head_sha = pr["head_sha"]

        # Prepare coroutines for parallel execution
        # We fetch reviews, status, and commits at the same time for this PR
        reviews_task = self._make_request(client, f"pulls/{pr_num}/reviews")
        status_task = self._make_request(client, f"commits/{head_sha}/status")
        commits_task = self._make_request(client, f"pulls/{pr_num}/commits")

        # Execute all 3 requests in parallel
        review_data, status_data, commit_data = await asyncio.gather(reviews_task, status_task, commits_task)

        # Process Reviews
        reviews = []
        approved_count = 0
        if review_data:
            for r in review_data:
                state = r.get("state")
                reviews.append({"state": state})
                if state == "APPROVED":
                    approved_count += 1

        # Process Status
        statuses = []
        if status_data and "statuses" in status_data:
            for s in status_data["statuses"]:
                statuses.append({
                    "name": s.get("context"),
                    "conclusion": s.get("state"),
                    "completed_at": s.get("updated_at")
                })

        # Process Commits
        commits_info = []
        if commit_data:
            for c in commit_data:
                commits_info.append({
                    "sha": c.get("sha"),
                    "author_name": c.get("commit", {}).get("author", {}).get("name")
                })

        # Assemble Data Structure
        pr["reviews"] = reviews
        pr["approved_review_count"] = approved_count
        pr["status_checks"] = statuses
        pr["commits"] = commits_info
        pr["commit_count"] = len(commits_info)

        return pr

    async def run(self):
        # We use a single persistent client for connection pooling
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            
            # 1. Fetch Metadata (Sequential pages, but fast)
            raw_prs = await self.fetch_pr_metadata(client, limit=LIMIT_PRS)
            
            # 2. Enrich Data (Concurrent)
            logger.info(f"Starting async enrichment for {len(raw_prs)} PRs...")
            
            tasks = [self.fetch_pr_details(client, pr) for pr in raw_prs]
            
            # as_completed allows us to show progress as they finish
            enriched_data = []
            for i, task in enumerate(asyncio.as_completed(tasks)):
                result = await task
                enriched_data.append(result)
                if (i + 1) % 10 == 0:
                    logger.info(f"Enriched {i + 1}/{len(raw_prs)} PRs...")

            # 3. Save to Local JSON
            if not os.path.exists(OUTPUT_DIR):
                os.makedirs(OUTPUT_DIR)
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{OUTPUT_DIR}/prs_extract_async_{timestamp}.json"
            
            with open(filename, "w", encoding='utf-8') as f:
                json.dump(enriched_data, f, indent=4, ensure_ascii=False)
                
            logger.info(f"Process Complete. Data saved to: {filename}")

if __name__ == "__main__":
    extractor = GitHubExtractor(GITHUB_TOKEN, REPO_OWNER, REPO_NAME)
    
    # Entry point for asyncio
    asyncio.run(extractor.run())