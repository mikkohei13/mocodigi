# mocodigi

A system under development for digitizing biological museum specimens, specifically pinned insects. The system processes multiple photographs of each specimen taken from different angles, extracts text from specimen labels using a vision-language model, and consolidates the transcriptions into accurate, complete label data using a LLM. The extracted information is cached in json files.

## Development principles

See PRINCIPLES.md

## File structure

- `docker-compose.yml` - Docker services
- `app/digitize.py` - Transcribes labels from specimen images using LLM
- `app/consolidate.py` - Consolidates multiple transcriptions from different angles into a single refined label text
- `app/google_geocode.py` - Geocodes text into coordinates using Google Maps API
- `app/*_utils.py` - Utility modules (image handling, Gemini API, caching, database)
- `app/database/` - Database schema and initialization scripts
- `app/images/` - Specimen image folders (one per specimen)

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
