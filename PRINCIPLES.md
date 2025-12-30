## Development principles

A system under development for digitizing biological museum specimens, specifically pinned insects. The system processes multiple photographs of each specimen taken from different angles, extracts text from specimen labels using a vision-language model, and consolidates the transcriptions into accurate, complete label data using a LLM. The extracted information is cached in json files.

### Simplicity first

- This is a one-person development project, not a production system. Do not aim for production-grade architecture; instead, favor simple solutions (KISS).
- Don't use automatic fallback mechanisms.
- Avoid over-engineering and premature optimization. Focus on solving the actual problem rather than hypothetical future needs.
- Keep the architecture simple and understandable for AI-assisted programming tools.
- Use clear comments to explain why something is done rather than what is done; make the code self-documenting where possible.
- There is no need for thorough error checking or detailed error messages. The system is used only by me, so minimal error handling is sufficient.

### Code organization

- Code is run from the command line. Arguments are hard-coded into the scripts.
- Main scripts are located in the `app` folder.
- Utility functions are also in the `app` folder, in files ending with `_utils.py`.
- Images are stored in the `images` subfolder. Each specimen has its own subfolder (e.g. `A01`). Output files are stored in an `artifacts` subfolder within each specimen folder.
- `database` subfolder contains scripts for database hat is used for retrieval-augmented generation (RAG)

