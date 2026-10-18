# Included Uber Movement historical dataset

Current codebase snapshot includes the dataset archive supplied with the project so the data-contract/profiling step can run without a separate download.

Included travel-time files:

- `Travel_Times - Bogota.csv`
- `Travel_Times - Boston.csv`
- `Travel_Times - Johannesburg and Pretoria.csv`
- `Travel_Times - Manila.csv`
- `Travel_Times - Paris.csv`
- `Travel_Times - Sydney.csv`
- `Travel_Times - Washington DC.csv`

The archive also includes:

- `Uber Movement - Travel Times Methodology.pdf`
- `datapackage.json`

According to the included `datapackage.json`, the data are Uber Movement aggregated travel-time statistics derived from anonymized trips, and the package declares a **CC BY-NC** license. Keep the metadata/methodology files with the dataset when redistributing this educational snapshot.

Current codebase treats each CSV as one location partition and normalizes every processed record to:

```text
location
source_file
origin_id
destination_id
mean_tt
lower_tt
upper_tt
date_range
```

The natural OD key is `(location, origin_id, destination_id)`.
