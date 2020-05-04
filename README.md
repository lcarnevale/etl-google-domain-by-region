# ETL of Google Domain/Languages by Country

This project aims to make a dataset of Google domain/languages by country.

### Extraction
The ETL process extracts data from the following resources: 
- list of all countries and their languages (https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory) (web scraping)
- list of all Google domains (https://ipfs.io/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/List_of_Google_domains.html) (web scraping)

It results into two tables with the following columns:
```
    Region | Languages
```
```
    Region | Google Domain
```

### Transformation
All columns have been cleaned up from:
- superscript wrapped into square brackets;
- spaces and new line characters.

The languages column was initially represented as a List and then transformed in String. This column has even cleaned up from metadata wrapped into round brackets.

The two original tables are then joined in a new one, according with the Region column.



### Load
The dataset is stored on an excel spreadsheet with the following columns:
```
    Region | Languages | Google Domain
```