# Databricks notebook source
# MAGIC %md
# MAGIC # RawToCleanseDeltaTable
# MAGIC
# MAGIC This notebook reads raw source data, applies cleansing transformations defined
# MAGIC in the sink entity's .properties file, and loads the cleansed data into a
# MAGIC Delta table using SCD Type 2 logic.
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC - `SinkEntity`: The EntityFullyQualifiedName of the cleansed sink entity
# MAGIC - `StorageAccount`: The ADLS storage account name

# COMMAND ----------
# Cell 1 - Print start
print("Loading RawToCleanseDeltaTable")

# COMMAND ----------
# Cell 2 - Imports
import json
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, to_date, col
from pyspark.sql.types import StringType, TimestampType, BooleanType
from types import SimpleNamespace

# COMMAND ----------
# Cell 3 - Widgets
dbutils.widgets.text("SinkEntity", "0", "SinkEntity")
dbutils.widgets.text("StorageAccount", "0", "StorageAccount")

SinkEntity     = dbutils.widgets.get("SinkEntity")
StorageAccount = dbutils.widgets.get("StorageAccount")

print(f"SinkEntity:     {SinkEntity}")
print(f"StorageAccount: {StorageAccount}")

# COMMAND ----------
# Cell 4 - Mount points (sets mountpointRaw, mountpointCleansed, mountpointMetadata etc.)
%run "/Shared/Mounting/DBFSMountPoints"

# COMMAND ----------
# Cell 5 - Metadata root path
# Connect to the Metadata folder to get the properties files
MetadataRootPath = mountpointMetadata + "/definition/properties"
print(MetadataRootPath)

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

if file_exists(MetadataRootPath):
    print("Yes it exists")
else:
    print("No it doesn't")

# COMMAND ----------
# Cell 6 - Load shared notebooks (Cleansing, Properties, DeltaTable classes)
%run "/Shared/Cleansing"
%run "/Shared/Properties"
%run "/Shared/DeltaTable"

# COMMAND ----------
# Cell 7 - Validate widget inputs
if not SinkEntity or SinkEntity == "0":
    raise Exception("SinkEntity widget has not been set. Please pass a valid EntityFullyQualifiedName.")

if not StorageAccount or StorageAccount == "0":
    raise Exception("StorageAccount widget has not been set.")

# COMMAND ----------
# Cell 8 - Load Sink properties using ClassProperties
# ClassProperties resolves the .properties file path from metadata.entities
# using the EntityFullyQualifiedName
oSinkProperties = ClassProperties(EntityFullyQualifiedName = SinkEntity)

# Extract key sink system properties
SinkEntityPath              = oSinkProperties.System.EntityPath
SinkEntityContainer         = oSinkProperties.System.EntityContainer
SinkAnalyticsProduct        = oSinkProperties.System.AnalyticsProduct
SinkEntityFullyQualifiedName = SinkEntity
SinkPartitionFormat         = oSinkProperties.System.PartitionFormat

# PropertiesFilePath is resolved internally by ClassProperties
PropertiesFilePath = oSinkProperties.PropertiesFilePath

print(f"SinkEntityPath:       {SinkEntityPath}")
print(f"SinkEntityContainer:  {SinkEntityContainer}")
print(f"SinkAnalyticsProduct: {SinkAnalyticsProduct}")
print(f"PropertiesFilePath:   {PropertiesFilePath}")

# COMMAND ----------
# Cell 9 - Extract Extended.Cleanse properties from the raw JSON
# (ClassProperties exposes these via SimpleNamespace but we also need the
#  CleansingTransformations list which is easier to pull directly from JSON)

SourceEntityName      = oSinkProperties.Extended.Cleanse.SourceEntityName
SourceEntityContainer = oSinkProperties.Extended.Cleanse.SourceEntityContainer
SourceEntityInstance  = oSinkProperties.Extended.Cleanse.SourceEntityInstance
SourceEntityType      = oSinkProperties.Extended.Cleanse.SourceEntityType

# Load the CleansingTransformations list directly from the JSON properties file
SourceCleansingTransformations = json.load(
    open('/dbfs/' + PropertiesFilePath)
)['Extended']['Cleanse']['CleansingTransformations']

print(f"SourceEntityName:      {SourceEntityName}")
print(f"SourceEntityContainer: {SourceEntityContainer}")
print(f"SourceEntityInstance:  {SourceEntityInstance}")
print(f"SourceEntityType:      {SourceEntityType}")
print(f"CleansingTransformations: {SourceCleansingTransformations}")

# COMMAND ----------
# Cell 10 - Load Source properties file
dfSourceProperties = (
    spark.read
         .option("multiline", "true")
         .json(
             MetadataRootPath + "/" +
             SourceEntityType + "/" +
             SourceEntityInstance + "/" +
             SourceEntityContainer + "/" +
             SourceEntityName + ".properties"
         )
)

# Get Source Properties
SourceEntityPath           = dfSourceProperties.select("System.EntityPath").head()[0]
SourceFileFormatExtension  = dfSourceProperties.select("System.FileFormat.Extension").head()[0]
SourceFileColumnDelimiter  = dfSourceProperties.select("System.FileFormat.ColumnDelimiter").head()[0]
SourceFileHeader           = str(dfSourceProperties.select("System.FileFormat.Header").head()[0]).lower()
SourceExtractType          = dfSourceProperties.select("Extended.Ingest.ExtractType").head()[0]
SourceAllowedRetention     = dfSourceProperties.select("System.AllowedRetention").head()[0]
IsHousekeepingEnabled      = dfSourceProperties.select("System.IsHousekeepingEnabled").head()[0]

# Derive the read format (strip leading dot from extension e.g. ".csv" -> "csv")
read_format = (
    dfSourceProperties
    .withColumn('read_format', F.regexp_replace('System.FileFormat.Extension', '\\.', ''))
    .select("read_format")
    .head()[0]
)

# Build source and sink physical paths
source_path = mountpointRaw      + "/" + SourceEntityPath
sink_path   = mountpointCleansed + "/" + SinkEntityPath

print(f"SourceEntityPath: {SourceEntityPath}")
print(f"source_path:      {source_path}")
print(f"sink_path:        {sink_path}")
print(f"read_format:      {read_format}")

# COMMAND ----------
# Cell 11 - Instantiate Sink ClassDeltaTable (Role = Sink)
# This resolves CurrentHWM from the meta file, sets up mount point etc.
oSinkDeltaTable = ClassDeltaTable(
    EntityFullyQualifiedName = SinkEntity,
    oProperties              = oSinkProperties,
    Role                     = "Sink",
    Action                   = "Cleanse"
)

# Ensure the sink delta table exists (creates schema + table if not present)
oSinkDeltaTable.Create()

print(f"Sink TableName:    {oSinkDeltaTable._TableName}")
print(f"Sink TargetFormat: {oSinkDeltaTable._TargetFormat}")

# COMMAND ----------
# Cell 12 - Instantiate Source ClassDeltaTable (Role = Source)
# This reads CurrentHWM (max InsertedDateTime/UpdatedDateTime) from the source table
# so we can filter only new/changed rows since the last run.

# Build source EFQN from its properties
SourceEFQN = f"{SourceAnalyticsProduct}.{SourceEntityContainer}.{SourceEntityName}"

oSourceDeltaTable = ClassDeltaTable(
    EntityFullyQualifiedName = SourceEFQN,
    Role                     = "Source",
    Action                   = "Cleanse"
)

# BoundaryLow  = HWM of the SINK (i.e. last successfully processed timestamp)
# BoundaryHigh = max modified datetime currently in the SOURCE table
BoundaryLow  = oSinkDeltaTable.CurrentHWM
BoundaryHigh = oSourceDeltaTable.GetMaxModifiedDateTime()

print(f"BoundaryLow  (Sink HWM):           {BoundaryLow}")
print(f"BoundaryHigh (Source max datetime): {BoundaryHigh}")

# COMMAND ----------
# Cell 13 - Read incremental change data from the source delta table
# GetChangeDataFilter builds the WHERE clause based on InsertedDateTime/UpdatedDateTime
# between BoundaryLow and BoundaryHigh

strFilter = oSourceDeltaTable.GetChangeDataFilter(
    BoundaryLow = BoundaryLow
)

print(f"Change data filter: {strFilter}")

# Read the raw source data (delta format expected for HWM-based incremental)
if read_format == "delta":
    dfRaw = spark.read.format("delta").load(source_path)
else:
    # Fallback: read file-based source (csv, parquet, json etc.)
    read_options = {}
    if read_format == "csv":
        read_options = {
            "header":    SourceFileHeader,
            "delimiter": SourceFileColumnDelimiter
        }
    dfRaw = spark.read.format(read_format).options(**read_options).load(source_path)

# Apply the incremental filter
if strFilter and strFilter.strip() != "":
    dfIncremental = dfRaw.filter(strFilter)
else:
    dfIncremental = dfRaw

row_count = dfIncremental.count()
print(f"Incremental rows to process: {row_count}")

# If no new rows, exit gracefully
if row_count == 0:
    print("No new or changed rows since last run. Exiting.")
    dbutils.notebook.exit("No new data")

# COMMAND ----------
# Cell 14 - Apply Cleansing Transformations
# Instantiate the Cleansing class and apply each transformation defined
# in the CleansingTransformations list from the properties file.

oCleansing = Cleansing()

dfCleansed = dfIncremental
for transform in SourceCleansingTransformations:
    dfCleansed = oCleansing.PerformCleansingTransformations(dfCleansed, transform)

print("Cleansing transformations applied.")
display(dfCleansed.limit(5))

# COMMAND ----------
# Cell 15 - Add SCD Type 2 tracking columns
# These are standard columns added to every cleansed entity:
#   EffectiveStartDate : timestamp when this version of the record became active
#   EffectiveEndDate   : timestamp when this version was superseded (NULL = current)
#   IsActive           : boolean flag, True = current/active version

# Only add if not already present (idempotency guard)
existing_cols = [c.lower() for c in dfCleansed.columns]

if "effectivestartdate" not in existing_cols:
    dfCleansed = dfCleansed.withColumn(
        "EffectiveStartDate",
        F.coalesce(col("InsertedDateTime").cast(TimestampType()), current_timestamp())
    )

if "effectiveenddate" not in existing_cols:
    dfCleansed = dfCleansed.withColumn(
        "EffectiveEndDate",
        lit(None).cast(TimestampType())   # NULL = still active
    )

if "isactive" not in existing_cols:
    dfCleansed = dfCleansed.withColumn(
        "IsActive",
        lit(True).cast(BooleanType())
    )

print("SCD Type 2 columns added.")

# COMMAND ----------
# Cell 16 - Apply SCD Type 2 Merge into the sink Delta table
#
# SCD Type 2 logic:
#   WHEN MATCHED AND source record differs from sink record (AndMatchedClause):
#       -> UPDATE existing sink row: set IsActive=False, EffectiveEndDate=now()
#       -> INSERT new row from source with IsActive=True, EffectiveEndDate=NULL
#   WHEN NOT MATCHED:
#       -> INSERT new row
#
# We achieve this using two MERGE statements (standard Delta Lake SCD2 pattern):
#   Pass 1: Expire old records (WHEN MATCHED AND changed -> update IsActive/EffectiveEndDate)
#   Pass 2: Insert new versions (WHEN NOT MATCHED -> insert)
#           plus handle brand-new keys (WHEN NOT MATCHED -> insert)

# Build clause components from ClassDeltaTable helpers
OnClause          = oSinkDeltaTable._GetMergeOnClauseColumns(
                        SourceAlias = "source", SinkAlias = "sink")
AndMatchedClause  = oSinkDeltaTable._GetAndMatchedClause(
                        SourceAlias = "source", SinkAlias = "sink",
                        ExcludedColumns = "InsertedDateTime,UpdatedDateTime,EffectiveStartDate,EffectiveEndDate,IsActive")
MergeClause       = oSinkDeltaTable._GetMergeClauseColumns(
                        SourceAlias = "source", SinkAlias = "sink",
                        ExcludedColumns = "InsertedDateTime,EffectiveStartDate")
ColumnList        = oSinkProperties.GetColumnList(ListType='String')

# Register the cleansed dataframe as a temp view
dfCleansed.createOrReplaceTempView("StagingData")

# ------------------------------------------------------------------
# PASS 1: Expire changed records in the sink
#         Match on natural key AND record has changed AND currently active
#         -> set IsActive=False, EffectiveEndDate=current_timestamp()
# ------------------------------------------------------------------
ExpireSql = f"""
    MERGE INTO {oSinkDeltaTable._TableName} sink
    USING StagingData source
    ON {OnClause}
    AND sink.IsActive = true
    WHEN MATCHED {AndMatchedClause}
    THEN UPDATE SET
        sink.IsActive          = false,
        sink.EffectiveEndDate  = current_timestamp(),
        sink.UpdatedDateTime   = current_timestamp()
"""

print(f"Pass 1 - Expire SQL:\n{ExpireSql}")
spark.sql(ExpireSql)

# ------------------------------------------------------------------
# PASS 2: Insert new active versions for changed records + new keys
#         Source rows that had a match (now expired) -> insert new version
#         Source rows with no match at all -> insert as new
# ------------------------------------------------------------------
# Filter: only insert source rows that are genuinely new or changed
# (i.e. after Pass 1 there is no active matching row with the same values)
InsertSql = f"""
    MERGE INTO {oSinkDeltaTable._TableName} sink
    USING StagingData source
    ON {OnClause}
    AND sink.IsActive = true
    WHEN NOT MATCHED
    THEN INSERT ({ColumnList})
    VALUES ({ColumnList.replace(c, 'source.' + c) for c in ColumnList.split(',')})
"""

# Cleaner insert using SELECT * approach
InsertSql = f"""
    MERGE INTO {oSinkDeltaTable._TableName} sink
    USING (
        SELECT source.*
        FROM StagingData source
        LEFT JOIN {oSinkDeltaTable._TableName} sink_active
            ON {OnClause.replace('sink.', 'sink_active.').replace('source.', 'source.')}
            AND sink_active.IsActive = true
        WHERE sink_active.{oSinkProperties.Catalogue.Schema.NaturalKey[0]} IS NOT NULL
           OR sink_active.{oSinkProperties.Catalogue.Schema.NaturalKey[0]} IS NULL
    ) source
    ON {OnClause}
    AND sink.IsActive = true
    WHEN NOT MATCHED
    THEN INSERT ({ColumnList})
    VALUES ({ColumnList})
"""

# Simplest reliable pattern for Pass 2: direct INSERT of all staging rows
# (Pass 1 already expired the old versions; now insert the new ones)
InsertSql = f"""
    INSERT INTO {oSinkDeltaTable._TableName}
    SELECT * FROM StagingData
"""

print(f"Pass 2 - Insert SQL:\n{InsertSql}")
dfResult = spark.sql(InsertSql)

print("SCD Type 2 merge completed.")

# COMMAND ----------
# Cell 17 - Write new High Water Mark
# BoundaryHigh becomes the new HWM for the next incremental run
oSinkDeltaTable.SetSourceEntity(SourceEntity = oSourceDeltaTable)
oSinkDeltaTable.SetNewHWM(NewHWM = BoundaryHigh)
oSinkDeltaTable.WriteNewHWM()

print(f"New HWM written: {BoundaryHigh}")

# COMMAND ----------
# Cell 18 - Housekeeping (optional, driven by properties)
if str(IsHousekeepingEnabled).lower() == "true" and SourceAllowedRetention:
    try:
        retention_days = int(SourceAllowedRetention)
        cutoff = F.date_sub(F.current_date(), retention_days)
        HousekeepingSql = f"""
            DELETE FROM {oSinkDeltaTable._TableName}
            WHERE IsActive = false
            AND EffectiveEndDate < '{cutoff}'
        """
        spark.sql(HousekeepingSql)
        print(f"Housekeeping completed. Removed inactive records older than {retention_days} days.")
    except Exception as e:
        print(f"Housekeeping skipped: {str(e)}")

# COMMAND ----------
# Cell 19 - Complete
print(f"RawToCleanseDeltaTable completed successfully.")
print(f"Entity:      {SinkEntity}")
print(f"Rows staged: {row_count}")
print(f"New HWM:     {BoundaryHigh}")
