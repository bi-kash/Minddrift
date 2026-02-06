What is Apify?
https://mindrift.zendesk.com/hc/en-us/articles/23830047185052-What-is-apify

Apify Proxy

# Minddrift — Proxy Examples & Quick Guide

This repository contains examples and notes for using the Apify and OpenRouter proxies exposed by the Toloka agents endpoint.

## Table of contents

- Apify proxy
- OpenRouter proxy
- Examples (curl, Apify CLI, Python, TypeScript)
- Local scripts

---

## Apify proxy

- Base URL: `https://agents.toloka.ai/api/proxy/apify`
- Authentication: provide your Expert API key in the `Authorization` header:

  Authorization: Bearer <EXPERT_APIKEY>

### Quick curl example

curl -s -X GET "https://agents.toloka.ai/api/proxy/apify/v2/users/me" \
 -H "Authorization: Bearer $EXPERT_APIKEY"

### Apify CLI

Set a custom base URL so the CLI talks to the proxy:

export APIFY_CLIENT_BASE_URL=https://agents.toloka.ai/api/proxy/apify
apify login -t $EXPERT_APIKEY
apify runs info <RUN_ID>

### Python (Apify SDK) — minimal example

```python
from apify_client import ApifyClient

client = ApifyClient(token="$EXPERT_APIKEY", api_url="https://agents.toloka.ai/api/proxy/apify")
user = client.user().get()
print(user)
```

Replace `$EXPERT_APIKEY` with your key or use environment variables.

### TypeScript (Apify SDK) — minimal example

```ts
import { ApifyClient } from "apify-client";

const client = new ApifyClient({
  baseUrl: "https://agents.toloka.ai/api/proxy/apify",
  token: process.env.EXPERT_APIKEY,
});
const user = await client.user().get();
console.log(user);
```

---

## OpenRouter proxy

- Base URL: `https://agents.toloka.ai/api/proxy/openrouter/api/v1`
- Use the same API key with an OpenAI-compatible client.

### Python (OpenAI-compatible client) — minimal example

```python
from openai import OpenAI

client = OpenAI(base_url="https://agents.toloka.ai/api/proxy/openrouter/api/v1", api_key="<API Key>")
resp = client.chat.completions.create(model="openai/gpt-4o", messages=[{"role":"user","content":"Hello"}])
print(resp.choices[0].message.content)
```

---

## Notes & links

- What is Apify? https://mindrift.zendesk.com/hc/en-us/articles/23830047185052-What-is-apify
