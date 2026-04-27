## Development principles

A system under development for digitizing pipeline for biological museum specimens, specifically pinned insects and herbarium specimens. The extracted information is cached in json files.

### Simplicity first

- This is a one-person development project, not a production system. Do not aim for production-grade architecture; instead, favor simple solutions (KISS).
- Avoid over-engineering and premature optimization. Focus on solving the actual problem rather than hypothetical future needs.
- Keep the architecture simple and understandable for AI-assisted programming tools.
- Use clear comments to explain why something is done rather than what is done; make the code self-documenting where possible.

### Code organization

- Code is run from the command line.
- Main scripts are located in the `app/pipeline` folder.
- Utility functions are in the `app/utils` folder`.


