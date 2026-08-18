# Writing Paimon Tables

**Note:** The cluster running the Lakehouse Engine must have the paimon connector installed in order to write to paimon tables
(org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions).

Since the paimon connector does not create a schema file when writing to an empty location this feature was added into the Lakehouse Engine.

This means that the library transparently creates a schema file when none is present if paimon format is selected.

In addition to this, logic was also added to the engine to automatically handle any change in the schema of the table.
If new columns are added to the table, the engine will automatically update the schema file to reflect this change.
In addition to this, if columns are removed they are not removed from the schema, but instead they are filled with NULL values.