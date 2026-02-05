# mocodigi

A system under development for digitizing biological museum specimens, specifically pinned insects. The system processes multiple photographs of each specimen taken from different angles, extracts text from specimen labels using a vision-language model, and consolidates the transcriptions into accurate, complete label data using a LLM. The extracted information is cached in json files.

## Development principles

See PRINCIPLES.md

## File structure

- Digitization pipeline:
   - `app/fetch_specimens.py` - Fetches specimen data and images from FinBIF API
   - `app/digitize.py` - Transcribes labels from specimen images using LLM, caching the results to JSON files
   - `app/consolidate.py` - Using LLM, consolidates multiple transcriptions from different angles into a single refined label text, caching the results to JSON files
   - `app/align.py` - Using alignment algorithms, aligns multiple transcriptions from different angles into a single refined label text, caching the results to JSON files
   - `app/darwin_core.py` - Converts the digitized/consolidated label text to Darwin Core (DwC) format using LLM, caching the results to JSON files
   - `app/test.py` - Tests the consolidation and alignment data against ground truth
   - `app/compare.py` - Compares the transcriptions to existing data and builds an comparisonHTML table
   - `app/images/` - Specimen image folders (one folder and multiple imagesper specimen)
   - `app/google_geocode.py` - Geocodes text into coordinates using Google Maps API, caching the results to JSON files

- Utilities:
   - `app/cache_utils.py` - Utilities for caching data from LLM calls to JSON files
   - `app/image_utils.py` - Utilities for reading image files and converting them for use in Gemini API calls
   - `app/gemini_utils.py` - Utilities for calling the Gemini API

- RAG:
   - `app/rag_utils.py` - Utilities for retrieval-augmented generation (RAG)
   - `app/database/` - Locality name database schema and initialization scripts for RAG

## Setup

### Prerequisites

- Docker and Docker Compose
- Gemini API key

### Quick start

1. Create `.env` file with:
   ```
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=mocodigi
   GEMINI_API_KEY=your_api_key
   ```

2. Start services:
   ```bash
   docker-compose up --build
   ```

3. Access database UI:
   - Adminer: http://localhost:8080
   - Server: `postgres`, Database: `mocodigi`, Username/Password from `.env`

## Useful commands

Print Darwin Core file contents:

   find . -type f -name 'darwin_core.json' -exec cat {} +

Rename Darwin Core files:

   find . -type f -name 'darwin_core.json' -print0 | while IFS= read -r -d '' f; do   dir=${f%/darwin_core.json};   mv -- "$f" "$dir/darwin_core_v5_temp.json"; done