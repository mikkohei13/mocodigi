from pydantic import BaseModel, Field
from typing import Optional


class HerbariumSpecimen(BaseModel):
    """Structured data extracted from a herbarium specimen label."""

    collectionName: Optional[str] = Field(
        default=None,
        description=(
            "Full name of the source collection, herbarium, and/or museum."
        ),
    )
    specimenIdentifier: Optional[str] = Field(
        default=None,
        description=(
            "Museum accession, catalog number, or other identifier assigned to the specimen."
        ),
    )
    collectorFieldNumber: Optional[str] = Field(
        default=None,
        description=(
            "Identifier given by the collector in the field, usually a number."
        ),
    )
    scientificName: Optional[str] = Field(
        default=None,
        description=(
            "Scientific name without authorship, preferring the most recent determination if multiple are present."
        ),
    )
    scientificNameAuthorship: Optional[str] = Field(
        default=None,
        description=(
            "Author citation for the scientificName, and year if available."
        ),
    )
    identifiedBy: Optional[str] = Field(
        default=None,
        description=(
            "Name(s) of the person(s) who determined the scientific name, often indicated by 'det.', 'determ.', or 'conf.'. Use the most recent determination if multiple are present. Separate multiple names with a semicolon."
        ),
    )
    dateIdentified: Optional[str] = Field(
        default=None,
        description=(
            "Year or full date of the (most recent) determination."
        ),
    )
    family: Optional[str] = Field(
        default=None,
        description=(
            "Scientific family name if explicitly stated on the label, typically ending in 'aceae' or 'ae'."
        ),
    )
    eventDate: Optional[str] = Field(
        default=None,
        description="Collection date.",
    )
    localityDescription: Optional[str] = Field(
        default=None,
        description=(
            "Full locality description, preserving original wording and language. May include country, region, site name, and/or directions."
        ),
    )
    country: Optional[str] = Field(
        default=None,
        description=(
            "Country name, which may be a historical or non-English."
        ),
    )
    countryInterpretation: Optional[str] = Field(
        default=None,
        description=(
            "Current, interpreted non-verbatim country name in English."
        ),
    )
    stateProvince: Optional[str] = Field(
        default=None,
        description=(
            "State, province, department, or equivalent first-level administrative unit."
        ),
    )
    municipality: Optional[str] = Field(
        default=None,
        description=(
            "Municipality, county, district, or equivalent second-level administrative unit."
        ),
    )
    coordinates: Optional[str] = Field(
        default=None,
        description=(
            "Verbatim coordinate string including punctuation — may be in a modern or historical format."
        ),
    )
    coordinateSystemInterpretation: Optional[str] = Field(
        default=None,
        description=(
            "Interpretation of the coordinate system used, e.g. 'UTM', 'WGS84', or such. None if this cannot be determined."
        ),
    )
    latitude: Optional[str] = Field(
        default=None,
        description=(
            "Latitude verbatim."
        ),
    )
    longitude: Optional[str] = Field(
        default=None,
        description=(
            "Longitude verbatim."
        ),
    )
    elevation: Optional[str] = Field(
        default=None,
        description=(
            "Elevation or altitude including units if available."
        ),
    )
    habitat: Optional[str] = Field(
        default=None,
        description=(
            "Habitat description, vegetation community, substrate, microhabitat, or such."

        ),
    )
    recordedBy: Optional[str] = Field(
        default=None,
        description=(
            "Name(s) of the collector(s), often indicated by 'leg.', 'Coll.', or similar. Separate multiple names with a semicolon."
        ),
    )
    occurrenceRemarks: Optional[str] = Field(
        default=None,
        description=(
            "Descriptive notes about the specimen, occurrence, or collecting event."

        ),
    )
    nonWildInterpretation: Optional[bool] = Field(
        default=None,
        description=(
            "Interpretation of wildness: True if the specimen was collected from a cultivated plant or botanic garden. False if collected from a wild population. None if this cannot be determined."
        ),
    )