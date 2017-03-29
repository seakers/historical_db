from rdflib import URIRef

agencyClass = URIRef("http://ceosdb/class/SpaceAgency")
missionClass = URIRef("http://ceosdb/class/Mission")

isFromCountry = URIRef("http://ceosdb/schemas/relationship/isFromCountry")
hasFullName = URIRef("http://ceosdb/schemas/relationship/hasFullName")
builtBy = URIRef("http://ceosdb/schemas/relationship/builtBy")
hasStatus = URIRef("http://ceosdb/schemas/relationship/hasStatus")
hasLaunchDate = URIRef("http://ceosdb/schemas/relationship/hasLaunchDate")
hasEOLDate = URIRef("http://ceosdb/schemas/relationship/hasEOLDate")
hasApplications = URIRef("http://ceosdb/schemas/relationship/hasApplications")
hasOrbitDetails = URIRef("http://ceosdb/schemas/relationship/hasOrbitDetails")