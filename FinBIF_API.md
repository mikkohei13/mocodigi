# This file documents the FinBIF API and how to use it to get taxonomic and occurrence data

OpenAPI Specification (Swagger):

- https://api.laji.fi/openapi
- machine readable format: https://api.laji.fi/openapi-json

## Basic usage

Using the API requires an Access Token. Each request you make to the API must have Authorization: Bearer <ACCESS TOKEN> header.

Base URL: https://api.laji.fi/

## Endpoints

The following explains the most important endpoints:

### Occurrence data

- Warehouse – Data warehouse endpoint for querying occurrence data. Can be also used to send data to the data warehouse.
- Collection – Metadata about occurrence datasets aka. collections. All occurrences belong to one collection and the metadata provides information about the dataset. This endpoint also contains metadata of datasets that has not yet been shared to FinBIF as occurrence data (or they might not even be in digital format yet).
- Source – Data source. Each occurrence has a source. The source might be an IT-system, but also an Excel spreadsheet copied to FinBIF for long term storage, etc.

Example, fetch 100 latest occurrence records (i.e. nature observations):

    https://api.laji.fi/warehouse/query/unit/list?pageSize=100&page=1&cache=false&useIdentificationAnnotations=true&includeSubTaxa=true&includeNonValidTaxa=true&individualCountMin=1&includeNullLoadDates=false&wild=WILD%2CUNKNOWN&qualityIssues=NO_ISSUES

#### Occurrence data model

- Document – Metadata about the batch of occurrences (collection id, source id, created date, modified date, creator, owners, etc.)
- GatheringEvent – Data shared by all gathering events, for example background variables of different observation schemes.
- Gathering – Gathering event happens in some time at some place by some persons.
- Unit – Occurrences (i.e. nature observations) recorded during the gathering event.
- Identification – Identifications (possibly done later).
- Media – Image, audio. Documents, Gatherings and Units can have zero to many media. Document level media are for example images of specimen labels. Gathering level media are about the location/habitat. Unit media are about the occurrence.
- NamedPlace – For example an observation scheme area, that is surveyed across many years.
- Individual – For example a ringed bird.

### Taxonomy

- Taxa – Information about naming of organisms, classifying organisms in a hierarchical system or in taxonomic ranks, distribution data and biological interactions, identifiers across different systems, etc.
- InformalTaxonGroup – Informal groups may be taxonomic groups (such as Aves) or can be used to group similar species together (for example Aphyllophoroid fungi). Some species do not belong to any informal groups and some may belong to several. Informal groups can be used to filter taxa and occurrences. This endpoint provides a list and hierarchy of the groups.
- Publication – Taxa can contain scientific citations to publications (for example source of name, source of status in Finland, etc). This endpoint provides information about the publications (name, link, doi, etc).
- Checklist – Taxa endpoint contains information about several different independent checklists. The most important checklist is the FinBIF master checklist that is used as the base for occurrence data and contains most information.

