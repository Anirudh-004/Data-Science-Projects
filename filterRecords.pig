-- Loading all CSV files
csvMetaData1 = LOAD 'hdfs://cluster-9f4d-m/csvFiles/movie_metadata.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'READ_INPUT_HEADER');

-- Filtering records that have invalid data. In our case, if Id or OwnerUserId is null
filteredRecords = FILTER csvMetaData1 BY NOT ($1 IS NULL OR $9 IS NULL OR $11 IS NULL);

-- Storing transformed data
STORE filteredRecords INTO 'hdfs://cluster-9f4d-m/postsPigResult' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS', 'WRITE_OUTPUT_HEADER');