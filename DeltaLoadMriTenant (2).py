# Databricks notebook source
# MAGIC %md
# MAGIC # DeltaLoadMriTenant
# MAGIC
# MAGIC Entity-specific action function for MriTenant cleanse.
# MAGIC
# MAGIC Referenced in MriTenant.properties:
# MAGIC   Extended.Cleanse.ActionFunctions: ["DeltaLoadMriTenant"]
# MAGIC
# MAGIC Called by IBusCleansingWrapper:
# MAGIC   eval("DeltaLoadMriTenant")(Entity=SinkEntity, Action="Cleanse")
# MAGIC
# MAGIC This function is SELF-CONTAINED:
# MAGIC   1. Resolves source entity details from sink properties
# MAGIC      (Extended.Cleanse.SourceEntity* fields)
# MAGIC   2. Reads source CSV incrementally using HWM
# MAGIC   3. Applies CleansingTransformations (Rename, Cast, Calculate etc.)
# MAGIC   4. Arranges columns in catalogue order
# MAGIC   5. Ensures sink Delta table exists
# MAGIC   6. Adds SCD Type 2 columns
# MAGIC   7. Two-pass SCD2 merge into sink Delta table
# MAGIC   8. Writes new HWM
# MAGIC   9. Housekeeping
# MAGIC
# MAGIC To create a new entity notebook:
# MAGIC   Copy, rename file and function. No other changes needed.

# COMMAND ----------
# Cell 1
print("Loading DeltaLoadMriTenant")

# COMMAND ----------
# Cell 2 - Imports
import json
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------
# Cell 3 - Action function
def DeltaLoadMriTenant(Entity, Action, **kwargs):
    """
    Self-contained cleanse-to-delta SCD2 action function for MriTenant.

    Parameters
    ----------
    Entity : ClassDeltaTable (Role="Sink")
        Instantiated by IBusCleansingWrapper via Utility.GetEntity.
        Entity.oProperties holds all sink .properties file content.
        Entity._TableName, Entity.CurrentHWM, Entity._MountPoint etc.
        are all set on instantiation.
    Action : str
        "Cleanse"
    """

    Log.Write(f"** DeltaLoadMriTenant - Started | Action: {Action}")

    # ------------------------------------------------------------------
    # STEP 1 - Resolve sink properties
    # Everything comes from Entity which is already fully instantiated.
    # No re-loading of properties needed.
    # ------------------------------------------------------------------
    oSinkProperties  = Entity.oProperties
    SinkTableName    = Entity._TableName
    SinkMountPoint   = Entity._MountPoint
    PropertiesFilePath = oSinkProperties.PropertiesFilePath

    Log.Write(f"SinkTableName:     {SinkTableName}")
    Log.Write(f"SinkMountPoint:    {SinkMountPoint}")
    Log.Write(f"PropertiesFilePath:{PropertiesFilePath}")

    # ------------------------------------------------------------------
    # STEP 2 - Get source entity details
    # Source details are defined in the SINK properties file under
    # Extended.Cleanse — NOT in a separate SourceEntities[].EFQN structure.
    # This matches our actual properties file structure.
    # ------------------------------------------------------------------
    SourceEntityName      = oSinkProperties.Extended.Cleanse.SourceEntityName
    SourceEntityContainer = oSinkProperties.Extended.Cleanse.SourceEntityContainer
    SourceEntityInstance  = oSinkProperties.Extended.Cleanse.SourceEntityInstance
    SourceEntityType      = oSinkProperties.Extended.Cleanse.SourceEntityType

    Log.Write(f"SourceEntityName:      {SourceEntityName}")
    Log.Write(f"SourceEntityContainer: {SourceEntityContainer}")
    Log.Write(f"SourceEntityInstance:  {SourceEntityInstance}")
    Log.Write(f"SourceEntityType:      {SourceEntityType}")

    # ------------------------------------------------------------------
    # STEP 3 - Load source properties file
    # Path built from metadata root + source entity details.
    # This mirrors the pattern in iBusCleansing cell 10.
    # ------------------------------------------------------------------
    MetadataRootPath = mountpointMetadata + "/definition/properties"

    SourcePropertiesPath = (
        MetadataRootPath + "/" +
        SourceEntityType + "/" +
        SourceEntityInstance + "/" +
        SourceEntityContainer + "/" +
        SourceEntityName + ".properties"
    )

    Log.Write(f"Loading source properties from: {SourcePropertiesPath}")

    dfSourceProperties = (
        spark.read
             .option("multiline", "true")
             .json(SourcePropertiesPath)
    )

    # Extract source file properties
    SourceEntityPath       = dfSourceProperties.select("System.EntityPath").head()[0]
    SourceFileExtension    = dfSourceProperties.select("System.FileFormat.Extension").head()[0]
    SourceColumnDelimiter  = dfSourceProperties.select("System.FileFormat.ColumnDelimiter").head()[0]
    SourceFileHeader       = str(dfSourceProperties.select("System.FileFormat.Header").head()[0]).lower()
    SourceAllowedRetention = dfSourceProperties.select("System.AllowedRetention").head()[0]
    IsHousekeepingEnabled  = dfSourceProperties.select("System.IsHousekeepingEnabled").head()[0]

    # Derive read format: strip leading dot e.g. ".csv" -> "csv"
    ReadFormat = (
        dfSourceProperties
        .withColumn("read_format", F.regexp_replace("System.FileFormat.Extension", "\\.", ""))
        .select("read_format")
        .head()[0]
    )

    Log.Write(f"SourceEntityPath:      {SourceEntityPath}")
    Log.Write(f"ReadFormat:            {ReadFormat}")
    Log.Write(f"SourceColumnDelimiter: {SourceColumnDelimiter}")
    Log.Write(f"SourceFileHeader:      {SourceFileHeader}")

    # ------------------------------------------------------------------
    # STEP 4 - Instantiate source ClassDeltaTable (Role="Source")
    # This resolves the source mountpoint via ClassProperties and sets
    # CurrentHWM = GetMaxModifiedDateTime() from the source table.
    # We use this to get BoundaryLow (last sink HWM) and
    # BoundaryHigh (max datetime currently in source).
    # ------------------------------------------------------------------

    # BoundaryLow = last successfully processed HWM from the sink metafile
    # (set when Role="Sink" in ClassDeltaTable.__init__ via MetaFile.GetHighWaterMark)
    BoundaryLow = Entity.CurrentHWM
    Log.Write(f"BoundaryLow (Sink CurrentHWM): {BoundaryLow}")

    # Build full source file path using the source mount point
    # Source MountPoint is resolved by ClassProperties when it mounts
    # the SourceEntityContainer on initialisation.
    # We resolve it the same way: mountpointRaw + SourceEntityPath
    SourceFilePath = mountpointRaw + "/" + SourceEntityPath
    Log.Write(f"SourceFilePath: {SourceFilePath}")

    # ------------------------------------------------------------------
    # STEP 5 - Read source data
    # Read all source files — HWM filtering applied after reading
    # using the fileDate column (derived via CalculateAttribute in
    # CleansingTransformations).
    # inferSchema=false: all columns arrive as string from CSV.
    # Type casting is handled by CastAttributes transformation.
    # ------------------------------------------------------------------
    Log.Write(f"Reading source data from: {SourceFilePath}")

    if ReadFormat == "csv":
        dfRaw = (
            spark.read
                 .format("csv")
                 .option("header",      SourceFileHeader)
                 .option("delimiter",   SourceColumnDelimiter)
                 .option("inferSchema", "false")
                 .load(SourceFilePath)
        )
    else:
        dfRaw = (
            spark.read
                 .format(ReadFormat)
                 .option("inferSchema", "false")
                 .load(SourceFilePath)
        )

    raw_count = dfRaw.count()
    Log.Write(f"Raw rows loaded: {raw_count}")

    if raw_count == 0:
        Log.Write("DeltaLoadMriTenant: No source data found. Exiting.")
        return spark.createDataFrame([], schema="value INT")

    if Debug == 1:
        display(dfRaw.limit(5))

    # ------------------------------------------------------------------
    # STEP 6 - Apply Cleansing Transformations
    # CleansingTransformations list is read directly from the SINK
    # properties JSON file (not via SimpleNamespace — list of dicts).
    # Each transform is converted to SimpleNamespace so that
    # PerformCleansingTransformations can use vars(t).items().
    #
    # Order in properties file must be:
    #   1. RenameAttributes  -> rename source cols to target names
    #   2. CastAttributes    -> cast to catalogue-defined types
    #   3. CalculateAttribute-> custom expressions (timestamps, hashes etc.)
    # ------------------------------------------------------------------
    from types import SimpleNamespace

    CleansingTransformations = json.load(
        open('/dbfs/' + PropertiesFilePath.lstrip('/mnt'), 'r')
    )['Extended']['Cleanse']['CleansingTransformations']

    Log.Write(f"Applying {len(CleansingTransformations)} cleansing transformations")

    oCleansing = Cleansing()
    dfCleansed = dfRaw

    for transform in CleansingTransformations:
        # Convert dict to SimpleNamespace so vars(t).items() works
        t = SimpleNamespace(**transform)
        dfCleansed = oCleansing.PerformCleansingTransformations(dfCleansed, t)

    Log.Write("Cleansing transformations applied")

    if Debug == 1:
        display(dfCleansed.limit(5))

    # ------------------------------------------------------------------
    # STEP 7 - Apply HWM filter (incremental load)
    # After cleansing, fileDate column exists (added by CalculateAttribute).
    # Only keep rows where fileDate > BoundaryLow (last processed date).
    # This ensures we only process new incremental files.
    # ------------------------------------------------------------------
    try:
        BoundaryLowDate = BoundaryLow[:10]  # "1970-01-01T00:00:00..." -> "1970-01-01"
        dfIncremental = dfCleansed.filter(
            col("fileDate") > F.lit(BoundaryLowDate).cast("date")
        )
        Log.Write(f"HWM filter applied: fileDate > {BoundaryLowDate}")
    except Exception as e:
        Log.Write(f"HWM filter skipped (fileDate column not found): {str(e)}")
        dfIncremental = dfCleansed

    row_count = dfIncremental.count()
    Log.Write(f"Incremental rows after HWM filter: {row_count}")

    if row_count == 0:
        Log.Write("DeltaLoadMriTenant: No new rows after HWM filter. Exiting.")
        return spark.createDataFrame([], schema="value INT")

    # ------------------------------------------------------------------
    # STEP 8 - Arrange columns in catalogue order
    # GetColumnList returns comma-separated string of column names
    # in OrdinalPosition order from Catalogue.Schema.Attributes.
    # ------------------------------------------------------------------
    ColumnOrder  = oSinkProperties.GetColumnList(ListType="String")
    ColumnList   = [c.strip() for c in ColumnOrder.split(",")]

    # Only select columns that exist in the dataframe
    # (SCD2 cols not yet added — they are appended after this step)
    ExistingCols = [c for c in ColumnList if c in dfIncremental.columns]
    dfIncremental = dfIncremental.select(ExistingCols)

    Log.Write(f"Columns arranged in catalogue order: {ExistingCols}")

    # ------------------------------------------------------------------
    # STEP 9 - Ensure sink Delta table exists
    # Create() is idempotent — skips if already present.
    # Schema is built from Catalogue.Schema.Attributes via _GetSchemaString.
    # Also creates the schema (database) if not exists.
    # ------------------------------------------------------------------
    Entity.Create()
    Log.Write(f"Sink Delta table confirmed: {SinkTableName}")

    # ------------------------------------------------------------------
    # STEP 10 - Add SCD Type 2 tracking columns
    # Added AFTER catalogue column arrangement so they appear at the end.
    # Idempotency guard: only add if not already present.
    # ------------------------------------------------------------------
    existing_lower = [c.lower() for c in dfIncremental.columns]

    if "effectivestartdate" not in existing_lower:
        dfIncremental = dfIncremental.withColumn(
            "EffectiveStartDate",
            F.coalesce(
                col("InsertedDateTime").cast(TimestampType()),
                current_timestamp()
            )
        )
        Log.Write("EffectiveStartDate added")

    if "effectiveenddate" not in existing_lower:
        dfIncremental = dfIncremental.withColumn(
            "EffectiveEndDate",
            lit(None).cast(TimestampType())
        )
        Log.Write("EffectiveEndDate added")

    if "isactive" not in existing_lower:
        dfIncremental = dfIncremental.withColumn(
            "IsActive",
            lit(True).cast(BooleanType())
        )
        Log.Write("IsActive added")

    # Register as temp view for SQL MERGE
    dfIncremental.createOrReplaceTempView("StagingData")

    # ------------------------------------------------------------------
    # STEP 11 - Build SCD2 merge clause components
    # Uses ClassDeltaTable helpers which read from Catalogue.Schema:
    #   NaturalKey   -> OnClause (JOIN condition)
    #   Attributes   -> AndMatchedClause (changed column detection)
    # No hardcoding of column names.
    # ------------------------------------------------------------------
    OnClause = Entity._GetMergeOnClauseColumns(
        SourceAlias = "source",
        SinkAlias   = "sink"
    )

    AndMatchedClause = Entity._GetAndMatchedClause(
        SourceAlias     = "source",
        SinkAlias       = "sink",
        ExcludedColumns = "InsertedDateTime,UpdatedDateTime,EffectiveStartDate,EffectiveEndDate,IsActive"
    )

    InsertColumnList = ", ".join(dfIncremental.columns)
    InsertValuesList = ", ".join([f"source.{c.strip()}" for c in dfIncremental.columns])

    Log.Write(f"OnClause:         {OnClause}")
    Log.Write(f"AndMatchedClause: {AndMatchedClause}")

    # ------------------------------------------------------------------
    # STEP 12 - SCD2 PASS 1: Expire changed records
    # WHEN MATCHED on NaturalKey AND IsActive=True AND values changed:
    #   -> IsActive=False, EffectiveEndDate=now(), UpdatedDateTime=now()
    # ------------------------------------------------------------------
    ExpireSql = f"""
        MERGE INTO {SinkTableName} sink
        USING StagingData source
        ON {OnClause}
        AND sink.IsActive = true
        WHEN MATCHED {AndMatchedClause}
        THEN UPDATE SET
            sink.IsActive         = false,
            sink.EffectiveEndDate = current_timestamp(),
            sink.UpdatedDateTime  = current_timestamp()
    """

    Log.Write("Executing Pass 1 - Expire changed records")
    if Debug == 1:
        Log.Write(f"ExpireSql: {ExpireSql}")

    spark.sql(ExpireSql)
    Log.Write("Pass 1 complete")

    # ------------------------------------------------------------------
    # STEP 13 - SCD2 PASS 2: Insert new active versions
    # WHEN NOT MATCHED on NaturalKey with IsActive=True:
    #   -> INSERT as new active row
    # Covers: changed records (expired in Pass 1) + brand new keys
    # Unchanged records: still have active row, so WHEN NOT MATCHED
    # is False -> not duplicated.
    # ------------------------------------------------------------------
    InsertSql = f"""
        MERGE INTO {SinkTableName} sink
        USING StagingData source
        ON {OnClause}
        AND sink.IsActive = true
        WHEN NOT MATCHED
        THEN INSERT ({InsertColumnList})
        VALUES ({InsertValuesList})
    """

    Log.Write("Executing Pass 2 - Insert new active versions")
    if Debug == 1:
        Log.Write(f"InsertSql: {InsertSql}")

    dfResult = spark.sql(InsertSql)
    Log.Write("Pass 2 complete")

    # Capture rows affected
    try:
        rows_affected = dfResult.collect()[0][0]
    except Exception:
        rows_affected = row_count

    Log.Write(f"Rows affected: {rows_affected}")

    # ------------------------------------------------------------------
    # STEP 14 - Write new High Water Mark
    # NewHWM = max fileDate from the incremental dataframe.
    # This represents the latest file processed in this run.
    # Written to the sink metafile for next incremental run.
    # ------------------------------------------------------------------
    try:
        NewHWM = (
            dfIncremental
            .select(F.max(col("fileDate")).cast("string"))
            .collect()[0][0]
        )
    except Exception:
        NewHWM = str(F.current_date())

    Entity.SetNewHWM(NewHWM = NewHWM)
    Entity.WriteNewHWM(RowsAffected = rows_affected)
    Log.Write(f"New HWM written: {NewHWM}")

    # ------------------------------------------------------------------
    # STEP 15 - Housekeeping (optional)
    # Removes expired SCD2 rows beyond allowed retention window.
    # Driven by System.AllowedRetention and System.IsHousekeepingEnabled
    # in the SOURCE properties file.
    # Non-fatal: logged and skipped on any error.
    # ------------------------------------------------------------------
    if str(IsHousekeepingEnabled).lower() == "true" and SourceAllowedRetention:
        try:
            retention_days  = int(SourceAllowedRetention)
            HousekeepingSql = f"""
                DELETE FROM {SinkTableName}
                WHERE IsActive = false
                AND EffectiveEndDate < date_sub(current_date(), {retention_days})
            """
            spark.sql(HousekeepingSql)
            Log.Write(f"Housekeeping done: removed inactive records older than {retention_days} days")
        except Exception as e:
            Log.Write(f"Housekeeping skipped: {str(e)}")
    else:
        Log.Write("Housekeeping not enabled - skipping")

    Log.Write(f"** DeltaLoadMriTenant - Completed | Raw: {raw_count} | Incremental: {row_count} | Affected: {rows_affected} | NewHWM: {NewHWM}")

    return dfResult
