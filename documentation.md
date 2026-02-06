What is Apify?
https://mindrift.zendesk.com/hc/en-us/articles/23830047185052-What-is-apify


Apify Proxy
Currently, the proxy is configured to forward requests to the Apify API.

Base URL

https://agents.toloka.ai/api/proxy/apify
Authentication
Use your Expert API key in the Authorization header:


Authorization: Bearer <EXPERT_APIKEY>
Your API key: <EXPERT_APIKEY>

Usage Examples
Using curl

curl -X GET 'https://agents.toloka.ai/api/proxy/apify/v2/users/me' \
  -H "Authorization: Bearer $EXPERT_APIKEY"
Response:


{
  "data": {
    "id": "wAEjb9o4qqqFYKJ2T",
    "username": "Toloka",
    "email": "svc.apify@toloka.ai",
    ...
  }, ...
}
Using Apify CLI
You can use the proxy with the official Apify CLI by setting the APIFY_CLIENT_BASE_URL environment variable:


export APIFY_CLIENT_BASE_URL=https://agents.toloka.ai/api/proxy/apify

apify login -t $EXPERT_APIKEY
# Success: You are logged in to Apify as Toloka.

apify runs info <RUN_ID>
# Actor: trudax/reddit-scraper-lite (oAuCIx3ItNrs2okjQ)
# Status: Succeeded (exit code: 0)
# ...
Using Apify Python SDK

#!/usr/bin/env python3
"""
Test script to verify Apify proxy is working with Python SDK
What is Apify? https://mindrift.zendesk.com/hc/en-us/articles/23830047185052-What-is-apify
"""
import os
from apify_client import ApifyClient

# Set up the proxy configuration
APIFY_CLIENT_BASE_URL = "https://agents.toloka.ai/api/proxy/apify"
EXPERT_APIKEY = "your_apikey"

try:
    # Initialize the Apify client with custom base URL
    client = ApifyClient(
        token=EXPERT_APIKEY,
        api_url=APIFY_CLIENT_BASE_URL
    )
    
    print("
✓ ApifyClient initialized successfully")
    
    # Test 1: Get current user info
    print("
[Test 1] Getting current user info...")
    user = client.user().get()
    print(f"✓ Successfully retrieved user info:")
    print(f"  - ID: {user.get('id')}")
    print(f"  - Username: {user.get('username')}")
    print(f"  - Email: {user.get('email')}")
    
    # Test 2: List actors (if available)
    print("
[Test 2] Listing actors...")
    try:
        actors = client.actors().list(limit=5)
        print(f"✓ Successfully retrieved actors list:")
        if actors and actors.items:
            for actor in actors.items[:3]:
                print(f"  - {actor.get('name')} ({actor.get('id')})")
        else:
            print("  (No actors found)")
    except Exception as e:
        print(f"⚠ Could not list actors: {e}")
    
    # Test 3: Get a specific run (using the run ID from your example)
    print("
[Test 3] Getting run info for uSe0869jYTAW7hWUS...")
    try:
        run = client.run("uSe0869jYTAW7hWUS").get()
        print(f"✓ Successfully retrieved run info:")
        print(f"  - Status: {run.get('status')}")
        print(f"  - Actor ID: {run.get('actId')}")
        print(f"  - Started at: {run.get('startedAt')}")
        print(f"  - Finished at: {run.get('finishedAt')}")
    except Exception as e:
        print(f"⚠ Could not get run info: {e}")
    
    print("
" + "=" * 80)
    print("✓ All tests completed successfully!")
    print("✓ The Apify proxy is working correctly with the Python SDK")
    print("=" * 80)
    
except Exception as e:
    print(f"
✗ Error occurred: {type(e).__name__}")
    print(f"  Message: {str(e)}")
    print("
✗ The proxy test failed")
    import traceback
    traceback.print_exc()
Using Apify TypeScript SDK

import { ApifyClient } from 'apify-client';

const APIFY_CLIENT_BASE_URL = 'https://agents.toloka.ai/api/proxy/apify';
const EXPERT_APIKEY = 'your_apikey';
const RUN_ID = 'uSe0869jYTAW7hWUS'; // Example run ID

const client = new ApifyClient({
    baseUrl: APIFY_CLIENT_BASE_URL,
    token: EXPERT_APIKEY,
});

async function main() {
    try {
        console.log('Attempting to fetch user info...');
        const user = await client.user().get();
        console.log('User info retrieved successfully:');
        console.log(`ID: ${user?.id}`);
        console.log(`Username: ${user?.username}`);
        console.log(`Email: ${user?.email}`);
        
        console.log(`Fetching info for run: ${RUN_ID}...`);
        const run = await client.run(RUN_ID).get();
        if (run) {
            console.log('Run info retrieved successfully:');
            console.log(`ID: ${run.id}`);
            console.log(`Actor ID: ${run.actId}`);
            console.log(`Status: ${run.status}`);
            console.log(`Started at: ${run.startedAt}`);
            console.log(`Finished at: ${run.finishedAt}`);
            console.log(`Exit code: ${run.exitCode}`);
            console.log(`Default Key-Value Store: ${run.defaultKeyValueStoreId}`);
            console.log(`Default Dataset: ${run.defaultDatasetId}`);
        } else {
            console.log(`Run ${RUN_ID} not found.`);
        }

    } catch (error) {
        console.error('Error occurred:', error);
    }
}

main();

OpenRouter Proxy
Use the same API key with OpenRouter via the OpenAI-compatible SDK.

Base URL

https://agents.toloka.ai/api/proxy/openrouter/api/v1
Usage Examples
Using Python

from openai import OpenAI

client = OpenAI(
  base_url="https://agents.toloka.ai/api/proxy/openrouter/api/v1",
  api_key="<API Key>",
)

completion = client.chat.completions.create(
  model="openai/gpt-4o",
  messages=[
    {
      "role": "user",
      "content": "What is the meaning of life?"
    }
  ]
)

print(completion.choices[0].message.content)
