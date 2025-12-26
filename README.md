# mocodigi

This is an ongoing project to test and develop methods for digitizing biological museum specimens. The basic idea is to take multiple photographs of a pinned insect and its labels from different angles, then use LLMs and other algorithms to perform OCR and refine the extracted label data.

## Development principles

### Simplicity first

- This is a one-person development project, not a production system. Do not aim for production-grade architecture; instead, favor simple solutions (KISS).
- Avoid over-engineering and premature optimization. Focus on solving the actual problem rather than hypothetical future needs.
- Keep the architecture simple and understandable for AI-assisted programming tools.
- Use clear comments to explain why something is done rather than what is done; make the code self-documenting where possible.
- There is no need for thorough error checking or detailed error messages. The system is used only by me, so minimal error handling is sufficient.

### Code organization

- Code is run from the command line. Arguments are hard-coded into the scripts.
- Main scripts are located in the `app` folder.
- Utility functions are also in the `app` folder, in files ending with `_utils.py`.
- Images are stored in the `images` subfolder. Each specimen has its own subfolder (e.g. `A01`). Output files are stored in an `artifacts` subfolder within each specimen folder.

## Setup 

- Database UI at http://localhost:5050
- Login locally with admin@example.com / admin