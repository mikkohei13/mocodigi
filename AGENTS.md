## Development principles

A digitization pipeline biological museum specimens using LLMs. For technical details, see PIPELINE.md.

### Simplicity first

- This is a one-person development project, not a production system. Do not aim for production-grade architecture; instead, favor simple solutions.
- Avoid over-engineering and premature optimization. Focus on solving the actual problem rather than hypothetical future needs.
- Keep the architecture simple and understandable for AI-assisted programming tools.
- Use clear comments to explain why something is done rather than what is done; make the code self-documenting where possible.

### Code organization

- Code is run on Docker Compose. Don't try to run Python scripts directly.
- Main scripts are located in the `app/pipeline` folder.
- Utility functions are in the `app/utils` folder`.


