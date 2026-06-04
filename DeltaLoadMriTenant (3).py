# Databricks notebook source
# MAGIC %md
# MAGIC # DeltaLoadMriTenant
# MAGIC
# MAGIC Entity-specific action function for MriTenant cleanse.
# MAGIC
# MAGIC Referenced in MriTenant.properties:
# MAGIC   Extended.Cleanse.ActionFunctions:  ["DeltaLoadMriTenant"]
# MAGIC   Extended.Cleanse.DataSourceType:   "multitable"
# MAGIC   Extended.Cleanse.SourceEntities:   [{"EFQN": "ibus.raw.MriTenant"}]
# MAGIC   Extended.Cleanse.TargetProcess:    "Merge"  (used for reference only — SCD2 done here)
# MAGIC
# MAGIC Called by main.PerformTransformation via eval("DeltaLoadMriTenant")(Entity, Action).
# MAGIC
# MAGIC Because DataSourceType="multitable", main:
# MAGIC   - DOES:  read source, apply CleansingTransformations, arrange columns,
# MAGIC            SetChangeData, SetNewHWM
# MAGIC   - DOES NOT: call _CommitData or _WriteHWM
# MAGIC
# MAGIC This function is responsible for:
# MAGIC   1. Getting the cleansed dataframe from Entity.GetChangeData()
# MAGIC   2. Ensuring the sink Delta table exists
# MAGIC   3. Adding SCD Type 2 tracking columns
# MAGIC   4. Two-pass SCD2 merge into the sink Delta table
# MAGIC   5. Writing the new HWM
# MAGIC   6. Housekeeping
# MAGIC
# MAGIC To create a new entity notebook:
# MAGIC   Copy this notebook, rename file and function name only.
# MAGIC   All logic is properties-driven — no other changes needed.

# COMMAND ----------
# Cell 1
print("Loading DeltaLoadMriTenant")

# COMMAND ----------
# Cell 2 - Imports
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------
# Cell 3 - Action function
def DeltaLoadMriTenant(Entity, Action, **kwargs):
    """
    SCD Type 2 delta load action function for MriTenant.

    By the time this is called, main.PerformTransformation has already:
      - Instantiated ClassDelimitedFile for the source via Utility.GetEntity
        using SourceEntities[0].EFQN from the properties file.
        ClassDelimitedFile resolves FilePath = MountPoint + EntityPath
        via ClassProperties — so the container is correctly resolved.
      - Called SourceEntity.Read(HighWaterMark=SinkCurrentHWM, FilePathFilter=...)
        which filters source CSV files by file_modification_time > SinkCurrentHWM.
      - Applied all CleansingTransformations one at a time
        (RenameAttributes -> CastAttributes -> CalculateAttribute).
      - Arranged columns in Catalogue OrdinalPosition order.
      - Called Entity.SetChangeData(ChangeData=dfCleansed).
      - Called Entity.SetNewHWM(NewHWM=SourceCurrentHWM).

    Parameters
    ----------
    Entity : ClassDeltaTable (Role="Sink")
        Entity.oProperties    : all sink properties
        Entity.dfChangeData   : cleansed dataframe set by main
        Entity.NewHWM         : new HWM set by main
        Entity._TableName     : sink Delta table name
        Entity.CurrentHWM     : last run HWM from metafile
    Action : str
        "Cleanse"
    """

    Log.Write(f"** DeltaLoadMriTenant - Started | Action: {Action}")
    Log.Write(f"Entity: {Entity.oProperties.EntityFullyQualifiedName}")

    # ------------------------------------------------------------------
    # STEP 1 - Get the cleansed dataframe
    # Set by main.PerformTransformation via Entity.SetChangeData().
    # Contains source data that has been:
    #   - read incrementally (file_modification_time > SinkCurrentHWM)
    #   - renamed, cast, calculated per CleansingTransformations
    #   - arranged in catalogue column order
    # ------------------------------------------------------------------
    dfCleansed = Entity.GetChangeData()

    if dfCleansed is None:
        Log.Write("DeltaLoadMriTenant: No change data on Entity. Nothing to process.")
        return spark.createDataFrame([], schema="value INT")

    row_count = dfCleansed.count()
    Log.Write(f"DeltaLoadMriTenant: Rows to process: {row_count}")

    if row_count == 0:
        Log.Write("DeltaLoadMriTenant: Zero rows. Nothing to process.")
        return dfCleansed

    if Debug == 1:
        Log.Write("DeltaLoadMriTenant: Displaying cleansed dataframe sample")
        display(dfCleansed.limit(5))

    # ------------------------------------------------------------------
    # STEP 2 - Ensure sink Delta table exists
    # Entity.Create() is idempotent.
    # Creates schema (database) and unmanaged Delta table if not present.
    # Schema is built from Catalogue.Schema.Attributes in properties file
    # via ClassDeltaTable._GetSchemaString().
    # Partition clause from System.PartitionKey via _GetPartitionClause().
    # ------------------------------------------------------------------
    Log.Write(f"DeltaLoadMriTenant: Ensuring sink Delta table exists: {Entity._TableName}")
    Entity.Create()
    Log.Write(f"DeltaLoadMriTenant: Sink Delta table confirmed: {Entity._TableName}")

    # ------------------------------------------------------------------
    # STEP 3 - Add SCD Type 2 tracking columns
    # Added after catalogue column arrangement so they appear at the end.
    # Idempotency guard: only add if not already present in the dataframe.
    #
    # EffectiveStartDate: when this version of the record became active.
    #                     Uses InsertedDateTime if present, else now().
    # EffectiveEndDate:   when this version was superseded. NULL = active.
    # IsActive:           True = current active version of the record.
    #
    # NOTE: These three columns must also be defined in
    # Catalogue.Schema.Attributes of the sink .properties file
    # so that Entity.Create() includes them in the Delta table DDL.
    # ------------------------------------------------------------------
    existing_lower = [c.lower() for c in dfCleansed.columns]

    if "effectivestartdate" not in existing_lower:
        dfCleansed = dfCleansed.withColumn(
            "EffectiveStartDate",
            F.coalesce(
                col("InsertedDateTime").cast(TimestampType()),
                current_timestamp()
            )
        )
        Log.Write("DeltaLoadMriTenant: EffectiveStartDate added")

    if "effectiveenddate" not in existing_lower:
        dfCleansed = dfCleansed.withColumn(
            "EffectiveEndDate",
            lit(None).cast(TimestampType())  # NULL = currently active
        )
        Log.Write("DeltaLoadMriTenant: EffectiveEndDate added")

    if "isactive" not in existing_lower:
        dfCleansed = dfCleansed.withColumn(
            "IsActive",
            lit(True).cast(BooleanType())
        )
        Log.Write("DeltaLoadMriTenant: IsActive added")

    # Register cleansed dataframe as temp view for SQL MERGE statements
    dfCleansed.createOrReplaceTempView("StagingData")

    # ------------------------------------------------------------------
    # STEP 4 - Build SCD2 merge clause components
    # Uses ClassDeltaTable helper methods which read directly from
    # Catalogue.Schema in the sink properties file — no hardcoding.
    #
    # OnClause:         JOIN condition on NaturalKey columns
    #                   e.g. "sink.TenantReference = source.TenantReference"
    #
    # AndMatchedClause: AND (...) condition detecting changed non-key columns
    #                   e.g. "AND (sink.TradeName <> source.TradeName OR ...)"
    #                   Excludes audit/SCD2 cols from change detection.
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

    # Build insert column and values lists from actual dataframe columns
    InsertColumnList = ", ".join(dfCleansed.columns)
    InsertValuesList = ", ".join([f"source.{c.strip()}" for c in dfCleansed.columns])

    Log.Write(f"DeltaLoadMriTenant: OnClause:         {OnClause}")
    Log.Write(f"DeltaLoadMriTenant: AndMatchedClause: {AndMatchedClause}")

    # ------------------------------------------------------------------
    # STEP 5 - SCD2 PASS 1: Expire changed records
    #
    # Target: rows in the sink that ARE active AND match on NaturalKey
    #         AND have at least one non-key column that has changed.
    #
    # Action: UPDATE those rows ->
    #           IsActive         = false  (no longer the current version)
    #           EffectiveEndDate = now()  (closed at this point in time)
    #           UpdatedDateTime  = now()  (audit trail)
    #
    # Result: the old version is preserved in history, marked inactive.
    # ------------------------------------------------------------------
    ExpireSql = f"""
        MERGE INTO {Entity._TableName} sink
        USING StagingData source
        ON {OnClause}
        AND sink.IsActive = true
        WHEN MATCHED {AndMatchedClause}
        THEN UPDATE SET
            sink.IsActive         = false,
            sink.EffectiveEndDate = current_timestamp(),
            sink.UpdatedDateTime  = current_timestamp()
    """

    Log.Write("DeltaLoadMriTenant: Executing Pass 1 - Expire changed records")
    if Debug == 1:
        Log.Write(f"DeltaLoadMriTenant: ExpireSql:\n{ExpireSql}")

    spark.sql(ExpireSql)
    Log.Write("DeltaLoadMriTenant: Pass 1 complete")

    # ------------------------------------------------------------------
    # STEP 6 - SCD2 PASS 2: Insert new active versions
    #
    # Target: staging rows that have NO active matching row in the sink.
    #         This covers two cases:
    #         a) Changed records: old version was expired in Pass 1,
    #            so no active row exists -> insert new version
    #         b) Brand new records: never existed in sink -> insert
    #
    # Unchanged records: still have an active row in sink ->
    #         WHEN NOT MATCHED is False -> NOT inserted (no duplicates)
    #
    # Action: INSERT staging row as new active version with:
    #           IsActive         = true   (set in Step 3)
    #           EffectiveEndDate = NULL   (set in Step 3, still active)
    #           EffectiveStartDate = InsertedDateTime or now() (Step 3)
    # ------------------------------------------------------------------
    InsertSql = f"""
        MERGE INTO {Entity._TableName} sink
        USING StagingData source
        ON {OnClause}
        AND sink.IsActive = true
        WHEN NOT MATCHED
        THEN INSERT ({InsertColumnList})
        VALUES ({InsertValuesList})
    """

    Log.Write("DeltaLoadMriTenant: Executing Pass 2 - Insert new active versions")
    if Debug == 1:
        Log.Write(f"DeltaLoadMriTenant: InsertSql:\n{InsertSql}")

    dfResult = spark.sql(InsertSql)
    Log.Write("DeltaLoadMriTenant: Pass 2 complete")

    # Capture rows affected for HWM metafile
    try:
        rows_affected = dfResult.collect()[0][0]
    except Exception:
        rows_affected = row_count

    Log.Write(f"DeltaLoadMriTenant: Rows affected: {rows_affected}")

    # ------------------------------------------------------------------
    # STEP 7 - Write new High Water Mark
    # NewHWM was set on Entity by main.PerformTransformation as
    # SourceCurrentHWM = SourceEntity.GetCurrentHWM() = now().
    # WriteNewHWM persists it to the sink metafile so the next run
    # knows where to start reading from.
    # Because DataSourceType="multitable", main does NOT call _WriteHWM,
    # so we handle it here.
    # ------------------------------------------------------------------
    Entity.WriteNewHWM(RowsAffected = rows_affected)
    Log.Write(f"DeltaLoadMriTenant: New HWM written: {Entity.GetNewHWM()}")

    # ------------------------------------------------------------------
    # STEP 8 - Housekeeping (optional)
    # Removes expired SCD2 records beyond the allowed retention window.
    # Driven by System.AllowedRetention and System.IsHousekeepingEnabled
    # in the SOURCE properties file.
    # Non-fatal: logged and skipped on any failure.
    # ------------------------------------------------------------------
    try:
        IsHousekeepingEnabled  = Entity.oProperties.System.IsHousekeepingEnabled
        SourceAllowedRetention = Entity.oProperties.System.AllowedRetention
    except Exception:
        IsHousekeepingEnabled  = False
        SourceAllowedRetention = None

    if str(IsHousekeepingEnabled).lower() == "true" and SourceAllowedRetention:
        try:
            retention_days  = int(SourceAllowedRetention)
            HousekeepingSql = f"""
                DELETE FROM {Entity._TableName}
                WHERE IsActive = false
                AND EffectiveEndDate < date_sub(current_date(), {retention_days})
            """
            spark.sql(HousekeepingSql)
            Log.Write(f"DeltaLoadMriTenant: Housekeeping done. Removed inactive records older than {retention_days} days.")
        except Exception as e:
            Log.Write(f"DeltaLoadMriTenant: Housekeeping skipped: {str(e)}")
    else:
        Log.Write("DeltaLoadMriTenant: Housekeeping not enabled - skipping")

    Log.Write(f"** DeltaLoadMriTenant - Completed | Rows staged: {row_count} | Rows affected: {rows_affected} | NewHWM: {Entity.GetNewHWM()}")

    return dfResult
