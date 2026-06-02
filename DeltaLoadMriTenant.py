# Databricks notebook source
# MAGIC %md
# MAGIC # DeltaLoadMriTenant
# MAGIC
# MAGIC Entity-specific action function notebook for MriTenant cleanse entity.
# MAGIC
# MAGIC NOT called directly by the pipeline. Called by the Wrapper notebook via
# MAGIC main.PerformTransformation(), which evals the function name from:
# MAGIC     Extended.Cleanse.ActionFunctions[0]  ->  "DeltaLoadMriTenant"
# MAGIC
# MAGIC The framework calls:
# MAGIC     dfChangeData = DeltaLoadMriTenant(Entity, Action)
# MAGIC
# MAGIC where Entity is already an instantiated ClassDeltaTable (Role="Sink").
# MAGIC
# MAGIC Source: CSV files in ADLS raw container
# MAGIC Sink:   Cleansed Delta table with SCD Type 2 history

# COMMAND ----------
# Cell 1 - Load confirmation
print("Loading DeltaLoadMriTenant")

# COMMAND ----------
# Cell 2 - Imports
# NOTE: All framework classes (Cleansing, ClassProperties, ClassDeltaTable,
# Utility, Log, Debug) are already in scope from the Wrapper's %run initialisations.
# Only imports specific to this function are needed here.

import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------
# Cell 3 - Action function definition

def DeltaLoadMriTenant(Entity, Action, **kwargs):
    """
    Entity-specific cleanse-to-delta action function for MriTenant.

    Source: CSV files on ADLS raw container.
    Sink:   Cleansed Delta table (SCD Type 2).

    Parameters
    ----------
    Entity : ClassDeltaTable
        Sink entity, Role="Sink", already fully instantiated by the Wrapper.
        All properties accessible via Entity.oProperties.
    Action : str
        Action being performed — "Cleanse".

    Returns
    -------
    dfChangeData : DataFrame
        Cleansed, SCD2-enriched dataframe.
        main.PerformTransformation() calls Entity.SetChangeData(dfChangeData)
        after this function returns.
    """

    Log.Write(f"** DeltaLoadMriTenant - Started | Action: {Action}")

    # ------------------------------------------------------------------
    # STEP 1 - Resolve sink properties from the already-instantiated Entity
    #
    # Entity is ClassDeltaTable(Role="Sink") — oProperties is already loaded.
    # We do NOT re-instantiate ClassProperties — we use what is on Entity.
    # ------------------------------------------------------------------
    oSinkProperties  = Entity.oProperties
    SinkTableName    = Entity._TableName
    SinkMountPoint   = Entity._MountPoint
    SinkEntityPath   = Entity._EntityPath

    Log.Write(f"SinkTableName:  {SinkTableName}")
    Log.Write(f"SinkEntityPath: {SinkEntityPath}")

    # ------------------------------------------------------------------
    # STEP 2 - Ensure the sink Delta table exists
    # Create() is idempotent — skips silently if the table already exists.
    # ------------------------------------------------------------------
    Entity.Create()
    Log.Write(f"Sink Delta table confirmed: {SinkTableName}")

    # ------------------------------------------------------------------
    # STEP 3 - Resolve source entity properties
    #
    # The source EFQN lives in Extended.Cleanse.SourceEntities[0].EFQN.
    # For a CSV/adlsg2 source we instantiate ClassProperties directly
    # to get the file-level details (path, format, delimiter, header).
    # We do NOT use Utility.GetEntity() here because the source is a
    # file-based entity (ClassDelimitedFile), not a Delta table, and we
    # need to read the files ourselves via spark.read — the framework's
    # ClassDelimitedFile.Read() is not used in the cleanse action function
    # pattern; file reading is handled directly in this function.
    # ------------------------------------------------------------------
    SourceEFQN = oSinkProperties.Extended.Cleanse.SourceEntities[0].EFQN
    Log.Write(f"Source EFQN: {SourceEFQN}")

    # Load source properties via ClassProperties so we get the file details.
    # ClassProperties resolves the .properties file path from metadata.entities.
    oSourceProperties = ClassProperties(EntityFullyQualifiedName = SourceEFQN)

    # File format details from source properties
    SourceEntityPath      = oSourceProperties.System.EntityPath
    SourceFileExtension   = oSourceProperties.System.FileFormat.Extension     # e.g. ".csv"
    SourceColumnDelimiter = oSourceProperties.System.FileFormat.ColumnDelimiter
    SourceHeader          = str(oSourceProperties.System.FileFormat.Header).lower()

    # Housekeeping settings (optional — gracefully defaulted below)
    try:
        IsHousekeepingEnabled  = oSourceProperties.System.IsHousekeepingEnabled
        SourceAllowedRetention = oSourceProperties.System.AllowedRetention
    except Exception:
        IsHousekeepingEnabled  = False
        SourceAllowedRetention = None

    # Derive Spark read format from extension (strip leading dot: ".csv" -> "csv")
    ReadFormat = SourceFileExtension.lstrip(".")

    # Build the full physical path to the raw CSV files.
    # SinkMountPoint is the mount root (e.g. /mnt/analyticsproduct).
    # Source files live under the raw container, not the cleansed one.
    # The raw mount point is constructed from the source properties.
    # oSourceProperties.MountPoint is set by ClassProperties when TargetFormat
    # is adlsg2 — it resolves to /mnt/<AnalyticsProduct>/<EntityContainer>.
    RawMountPoint = oSourceProperties.MountPoint
    SourceFilePath = f"{RawMountPoint}/{SourceEntityPath}"

    Log.Write(f"SourceEntityPath: {SourceEntityPath}")
    Log.Write(f"SourceFilePath:   {SourceFilePath}")
    Log.Write(f"ReadFormat:       {ReadFormat}")

    # ------------------------------------------------------------------
    # STEP 4 - Determine the High Water Mark boundaries
    #
    # BoundaryLow = CurrentHWM of the SINK
    #               The timestamp of the last successfully processed batch,
    #               stored in the sink entity's metafile.
    #               On first run this defaults to 1970-01-01T00:00:00.
    #
    # For CSV file sources there is no source Delta table to query for
    # a max modified datetime. BoundaryHigh = current_timestamp() at the
    # start of this run. After a successful merge this becomes the new HWM
    # so the next run can identify files/records added after this point.
    #
    # The incremental strategy for files is:
    #   - Read ALL files from the raw path (spark.read is lazy)
    #   - Apply CleansingTransformations (which calculate fileDate,
    #     fileName, InsertedDateTime, UpdatedDateTime etc.)
    #   - AFTER cleansing, filter to rows where fileDate > BoundaryLow
    #     (uses the fileDate CalculateAttribute from your properties file)
    #   - The SCD2 merge handles idempotency on the natural key
    # ------------------------------------------------------------------
    BoundaryLow  = Entity.GetCurrentHWM()
    BoundaryHigh = str(current_timestamp())

    # Use Spark to get a proper string timestamp for BoundaryHigh
    BoundaryHigh = spark.sql("SELECT current_timestamp()").collect()[0][0]
    BoundaryHigh = str(BoundaryHigh)

    # Candidate new HWM — persisted on success in Step 11
    Entity.SetNewHWM(NewHWM = BoundaryHigh)

    Log.Write(f"BoundaryLow  (Sink HWM):      {BoundaryLow}")
    Log.Write(f"BoundaryHigh (current run ts): {BoundaryHigh}")

    # ------------------------------------------------------------------
    # STEP 5 - Read ALL raw CSV files from the source path
    #
    # We read all files — not filtered yet. The incremental filtering
    # happens AFTER cleansing (Step 7) because fileDate is a calculated
    # column added by CleansingTransformations and does not exist in
    # the raw file. InsertedDateTime/UpdatedDateTime are also calculated.
    # ------------------------------------------------------------------
    Log.Write(f"Reading raw CSV files from: {SourceFilePath}")

    dfRaw = (
        spark.read
             .format(ReadFormat)
             .option("header",    SourceHeader)
             .option("delimiter", SourceColumnDelimiter)
             .option("inferSchema", "false")   # schema enforced by cleansing/catalogue
             .load(SourceFilePath)
    )

    total_raw_count = dfRaw.count()
    Log.Write(f"Total raw rows read: {total_raw_count}")

    if total_raw_count == 0:
        Log.Write("No raw files found at source path. Returning empty dataframe.")
        from pyspark.sql.types import StructType
        dfChangeData = spark.createDataFrame([], StructType([]))
        return dfChangeData

    # ------------------------------------------------------------------
    # STEP 6 - Apply CleansingTransformations
    #
    # The list of transformations lives in:
    #     Extended.Cleanse.CleansingTransformations
    # in the sink entity's properties file.
    #
    # For MriTenant, based on the properties file pattern seen, these include:
    #   RenameAttributes    -> rename raw column names to canonical names
    #   CalculateAttribute  -> hashedKey, InsertedDateTime, UpdatedDateTime,
    #                          filePath, fileName, fileDate, businessYear
    #
    # The Cleansing class (in scope from Wrapper %run) dispatches each
    # transform by name via its TransformationFunctions dispatch map.
    # ------------------------------------------------------------------
    CleansingTransformations = oSinkProperties.Extended.Cleanse.CleansingTransformations

    Log.Write(f"Applying {len(CleansingTransformations)} cleansing transformation(s).")

    oCleansing = Cleansing()
    dfCleansed = oCleansing.PerformCleansingTransformations(dfRaw, CleansingTransformations)

    Log.Write("CleansingTransformations applied.")

    if Debug == 1:
        display(dfCleansed.limit(5))

    # ------------------------------------------------------------------
    # STEP 7 - Apply incremental filter AFTER cleansing
    #
    # Now that CleansingTransformations have run, fileDate exists on the
    # dataframe. Filter to rows where fileDate > BoundaryLow so we only
    # process files/records that arrived since the last successful run.
    #
    # If BoundaryLow is the epoch default (1970-01-01) this passes all rows
    # — correct behaviour for a first run (full load).
    #
    # We use fileDate (date-level) rather than InsertedDateTime (which is
    # current_timestamp at cleanse time and therefore always > BoundaryLow).
    # ------------------------------------------------------------------
    try:
        dfIncremental = dfCleansed.filter(
            col("fileDate") > F.to_date(F.lit(BoundaryLow))
        )
    except Exception as e:
        # fileDate column may not exist if not in CleansingTransformations
        # — fall back to processing all rows (full load behaviour)
        Log.Write(f"fileDate filter not applied ({str(e)}) — processing all rows.")
        dfIncremental = dfCleansed

    row_count = dfIncremental.count()
    Log.Write(f"Incremental rows after fileDate filter: {row_count}")

    if row_count == 0:
        Log.Write("No new rows since last run. Returning empty dataframe.")
        dfChangeData = spark.createDataFrame([], dfCleansed.schema)
        return dfChangeData

    # ------------------------------------------------------------------
    # STEP 8 - Reorder columns to catalogue order
    #
    # Align the dataframe column order with the catalogue attribute sequence
    # defined in the sink .properties file. This ensures schema consistency
    # when writing to the Delta table.
    # ------------------------------------------------------------------
    ColumnOrder     = oSinkProperties.GetColumnList()   # comma-separated string
    ColumnList      = [c.strip() for c in ColumnOrder.split(",")]
    existing_cols   = dfIncremental.columns
    ColumnList_safe = [c for c in ColumnList if c in existing_cols]

    dfIncremental = dfIncremental.select(ColumnList_safe)
    Log.Write(f"Columns reordered to catalogue order ({len(ColumnList_safe)} columns).")

    # ------------------------------------------------------------------
    # STEP 9 - Add SCD Type 2 tracking columns
    #
    # These three columns are added to every row entering the cleansed table:
    #
    #   EffectiveStartDate : when this version of the record became active.
    #                        Defaults to InsertedDateTime if present, else now().
    #   EffectiveEndDate   : when this version was superseded.
    #                        NULL for a newly active version.
    #   IsActive           : True = this is the current active version.
    #
    # Idempotency guard: only add if not already present (they may be in the
    # catalogue schema and have been added by a CalculateAttribute transform).
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

    if "effectiveenddate" not in existing_lower:
        dfIncremental = dfIncremental.withColumn(
            "EffectiveEndDate",
            lit(None).cast(TimestampType())     # NULL = still active
        )

    if "isactive" not in existing_lower:
        dfIncremental = dfIncremental.withColumn(
            "IsActive",
            lit(True).cast(BooleanType())
        )

    Log.Write("SCD Type 2 columns verified / added.")

    # Register as a temp view for SQL merge statements
    dfIncremental.createOrReplaceTempView("StagingData")

    # ------------------------------------------------------------------
    # STEP 10 - Build SCD Type 2 merge clause components
    #
    # Use ClassDeltaTable helper methods to build the SQL fragments.
    # These read the NaturalKey and Attributes from the catalogue schema
    # in the sink .properties file — no hardcoding of column names here.
    #
    # OnClause         : JOIN condition on natural key columns
    # AndMatchedClause : AND (...) detecting a changed record (non-key cols differ)
    # MergeSetClause   : SET col = source.col for the UPDATE statement
    # InsertColumnList : Comma-separated column list for the INSERT statement
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

    # Full column list from catalogue (used for INSERT).
    # This must include ALL columns that exist in StagingData including
    # the SCD2 columns added in Step 9.
    InsertColumnList = ",".join(dfIncremental.columns)

    Log.Write(f"OnClause:         {OnClause}")
    Log.Write(f"AndMatchedClause: {AndMatchedClause}")
    Log.Write(f"InsertColumnList: {InsertColumnList}")

    # ------------------------------------------------------------------
    # STEP 11 - SCD Type 2 Merge: PASS 1 — Expire changed records
    #
    # For each source row that:
    #   (a) matches an existing ACTIVE sink row on the natural key, AND
    #   (b) at least one non-key, non-system column value has changed
    #
    # -> Mark the existing sink row as INACTIVE
    # -> Set EffectiveEndDate = current_timestamp()
    # -> Set UpdatedDateTime  = current_timestamp()
    #
    # Rows that match but are UNCHANGED are left untouched.
    # Brand-new keys (no match at all) are also untouched in this pass.
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

    Log.Write(f"Pass 1 - Expire SQL:\n{ExpireSql}")
    spark.sql(ExpireSql)
    Log.Write("Pass 1 - Expire complete.")

    # ------------------------------------------------------------------
    # STEP 12 - SCD Type 2 Merge: PASS 2 — Insert new active versions
    #
    # After Pass 1, there are NO active rows for keys whose values changed.
    # We now merge all staging rows against the sink:
    #
    #   WHEN NOT MATCHED (source key has no active counterpart in sink):
    #     -> INSERT the source row as a new active version.
    #        This covers BOTH changed records (expired in Pass 1)
    #        AND genuinely new records (no prior history).
    #
    #   WHEN MATCHED (source key HAS an active counterpart — record unchanged):
    #     -> Do nothing. The existing active row is correct and up to date.
    #
    # This pattern ensures unchanged records are NOT duplicated and
    # changed records get a fresh active version.
    # ------------------------------------------------------------------
    InsertSql = f"""
        MERGE INTO {SinkTableName} sink
        USING StagingData source
        ON {OnClause}
        AND sink.IsActive = true
        WHEN NOT MATCHED
        THEN INSERT ({InsertColumnList})
        VALUES ({", ".join([f"source.{c.strip()}" for c in InsertColumnList.split(",")])})
    """

    Log.Write(f"Pass 2 - Insert SQL:\n{InsertSql}")
    dfResult = spark.sql(InsertSql)
    Log.Write("Pass 2 - Insert complete.")

    # Capture rows affected for HWM metafile
    try:
        rows_affected = dfResult.collect()[0][0]
    except Exception:
        rows_affected = row_count    # fallback to staged row count

    Log.Write(f"Rows affected (Pass 2 insert): {rows_affected}")

    # ------------------------------------------------------------------
    # STEP 13 - Write new High Water Mark
    #
    # BoundaryHigh (current_timestamp at start of this run) is now persisted
    # as the new HWM so the next run's fileDate filter starts from here.
    #
    # WriteNewHWM writes the metafile via ClassMetaFile.
    # RowsAffected is also recorded in the metafile for observability.
    # ------------------------------------------------------------------
    Entity.WriteNewHWM(RowsAffected = rows_affected)
    Log.Write(f"New HWM written: {BoundaryHigh}")

    # ------------------------------------------------------------------
    # STEP 14 - Housekeeping (optional, driven by source properties)
    #
    # If configured, delete inactive (expired) SCD2 rows older than the
    # allowed retention window. This prevents unbounded table growth.
    # Failure here is non-fatal — logged and skipped.
    # ------------------------------------------------------------------
    try:
        if str(IsHousekeepingEnabled).lower() == "true" and SourceAllowedRetention:
            retention_days = int(SourceAllowedRetention)
            HousekeepingSql = f"""
                DELETE FROM {SinkTableName}
                WHERE IsActive = false
                AND EffectiveEndDate < date_sub(current_date(), {retention_days})
            """
            spark.sql(HousekeepingSql)
            Log.Write(f"Housekeeping done. Removed inactive records older than {retention_days} days.")
    except Exception as e:
        Log.Write(f"Housekeeping skipped: {str(e)}")

    # ------------------------------------------------------------------
    # STEP 15 - Return dfChangeData
    #
    # main.PerformTransformation() expects the action function to return
    # a dataframe. It then calls Entity.SetChangeData(ChangeData=dfChangeData).
    # We return dfIncremental (the staged, SCD2-enriched data for this run).
    # ------------------------------------------------------------------
    Log.Write(f"** DeltaLoadMriTenant - Completed | Rows staged: {row_count} | New HWM: {BoundaryHigh}")

    return dfIncremental

