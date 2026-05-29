# Databricks notebook source
# MAGIC %md
# MAGIC ## CastAttributes - Cleansing Transformation Function
# MAGIC
# MAGIC Add this function to the Cleansing notebook.
# MAGIC
# MAGIC **Two changes needed in the Cleansing notebook:**
# MAGIC 1. Add `CastAttributes` to the `TransformationFunctions` dict in `__init__`
# MAGIC 2. Add the `CastAttributes` method to the `Cleansing` class
# MAGIC
# MAGIC **Usage in .properties file CleansingTransformations:**
# MAGIC ```json
# MAGIC "CleansingTransformations": [
# MAGIC     {"RenameAttributes": "TNNT_REF:TenantReference, USAGE_CD:UsageCode"},
# MAGIC     {"CastAttributes": "path/to/sink.properties"},
# MAGIC     {"CalculateAttribute": "InsertedDateTime:current_timestamp()"}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **Important:** `CastAttributes` should always be placed AFTER `RenameAttributes`
# MAGIC in the CleansingTransformations list, so columns have already been renamed
# MAGIC to their target names before casting is attempted.

# COMMAND ----------

# =============================================================================
# CHANGE 1: In Cleansing.__init__, add this line to TransformationFunctions dict
# =============================================================================
#
#   self.TransformationFunctions = {
#       'DropDuplicates':                    self.DropDuplicates,
#       'FlattenAttribute':                  self.FlattenAttribute,
#       'RenameAttributes':                  self.RenameAttributes,
#       'RemoveAttributes':                  self.RemoveAttributes,
#       'CalculateAttribute':                self.CalculateAttribute,
#       'ConvertStringToStructAttribute':    self.ConvertStringToStructAttribute,
#       'MaskAttributes':                    self.MaskAttributes,
#       'NullAttributes':                    self.NullAttributes,
#       'ExtractValueFromOptionalAttribute': self.ExtractValueFromOptionalAttribute,
#       'ExtractValueFromJsonAttributeOfXmlSource': self.ExtractValueFromJsonAttributeOfXmlSource,
#       'DropDuplicatesByKey':               self.DropDuplicatesByKey,
#       'CastAttributes':                    self.CastAttributes,   # <-- ADD THIS LINE
#   }

# =============================================================================
# CHANGE 2: Add this method to the Cleansing class
# =============================================================================

def CastAttributes(self, i, propertiesFilePath):
    """
    CastAttributes: Casts dataframe columns to the datatypes defined in the
    Catalogue.Schema.Attributes section of the sink .properties file.

    It uses the RenameAttributes entries already applied to understand the
    source->target column mapping, and looks up the target datatype from
    the Catalogue. This avoids the need to manually write CalculateAttribute
    cast expressions for every column.

    Parameters
    ----------
    i : pyspark.sql.DataFrame
        The input dataframe (columns should already be renamed to target names
        via RenameAttributes before calling this function)
    propertiesFilePath : str
        The DBFS path to the sink entity .properties file
        e.g. "/mnt/metadata/definition/properties/adlsg2/cleansed/MriTenant.properties"

    Usage in CleansingTransformations (properties file):
        {"CastAttributes": "/mnt/metadata/definition/properties/adlsg2/cleansed/MriTenant.properties"}

    Notes
    -----
    - Must be called AFTER RenameAttributes so column names match the catalogue
    - Columns present in the dataframe but NOT in the catalogue are left as-is
    - Columns in the catalogue but NOT in the dataframe are silently skipped
    - Type mapping mirrors _GetSchemaString in ClassProperties/ClassDeltaTable:
        string, nvarchar, varchar, char -> StringType
        bit, boolean, bool             -> BooleanType
        date                           -> DateType
        datetime, datetimeoffset,
        datetime2                      -> TimestampType
        decimal, numeric               -> DecimalType(precision, scale)
        long, bigint                   -> LongType
        double                         -> DoubleType
        int, integer                   -> IntegerType
        float                          -> FloatType
        smallint                       -> ShortType
        tinyint                        -> ByteType
    """
    import json
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        StringType, BooleanType, DateType, TimestampType,
        DecimalType, LongType, DoubleType, IntegerType,
        FloatType, ShortType, ByteType
    )

    if Debug == 1:
        Log.Write("** Cleansing.CastAttributes - Started")

    # ------------------------------------------------------------------
    # Step 1: Load the properties file JSON
    # ------------------------------------------------------------------
    try:
        with open('/dbfs/' + propertiesFilePath.lstrip('/mnt').lstrip('/dbfs'), 'r') as f:
            propertiesJson = json.load(f)
    except Exception as e:
        # Try the path as-is (may already be a /dbfs/ path)
        try:
            with open(propertiesFilePath if propertiesFilePath.startswith('/dbfs/') 
                      else '/dbfs' + propertiesFilePath, 'r') as f:
                propertiesJson = json.load(f)
        except Exception as e2:
            Log.Write(f"CastAttributes: Could not load properties file {propertiesFilePath}: {str(e2)}")
            raise Exception(f"CastAttributes: Could not load properties file {propertiesFilePath}: {str(e2)}")

    # ------------------------------------------------------------------
    # Step 2: Build a dict of { TargetColumnName -> Type }
    #         from Catalogue.Schema.Attributes
    # ------------------------------------------------------------------
    try:
        catalogueAttributes = propertiesJson['Catalogue']['Schema']['Attributes']
    except KeyError as e:
        raise Exception(f"CastAttributes: Could not find Catalogue.Schema.Attributes in {propertiesFilePath}: {str(e)}")

    # Build lookup: column name (lower) -> attribute definition
    catalogueTypeMap = {}
    for attr in catalogueAttributes:
        colName  = str(attr.get('Name', '')).strip()
        colType  = str(attr.get('Type', '')).strip().lower()
        precision = attr.get('Precision') or attr.get('NumericPrecision')
        scale     = attr.get('Scale')     or attr.get('NumericScale')

        if colName:
            catalogueTypeMap[colName.lower()] = {
                'Name':      colName,
                'Type':      colType,
                'Precision': precision,
                'Scale':     scale
            }

    if Debug == 1:
        Log.Write(f"CastAttributes: catalogueTypeMap built with {len(catalogueTypeMap)} entries")

    # ------------------------------------------------------------------
    # Step 3: Map catalogue type strings to PySpark types
    #         (mirrors the logic in _GetSchemaString)
    # ------------------------------------------------------------------
    def resolveSparkType(colType, precision, scale):
        if colType in ('string', 'nvarchar', 'varchar', 'char'):
            return StringType()
        elif colType in ('bit', 'boolean', 'bool'):
            return BooleanType()
        elif colType == 'date':
            return DateType()
        elif colType in ('datetime', 'datetimeoffset', 'datetime2'):
            return TimestampType()
        elif colType in ('decimal', 'numeric'):
            try:
                return DecimalType(int(precision), int(scale))
            except Exception:
                return DecimalType(18, 6)  # safe default
        elif colType in ('long', 'bigint'):
            return LongType()
        elif colType == 'double':
            return DoubleType()
        elif colType in ('int', 'integer'):
            return IntegerType()
        elif colType == 'float':
            return FloatType()
        elif colType == 'smallint':
            return ShortType()
        elif colType == 'tinyint':
            return ByteType()
        else:
            # Unknown type — leave as string, log a warning
            if Debug == 1:
                Log.Write(f"CastAttributes: Unknown type '{colType}' — leaving as StringType")
            return StringType()

    # ------------------------------------------------------------------
    # Step 4: Cast each dataframe column that exists in the catalogue
    # ------------------------------------------------------------------
    dfColumns = [c.lower() for c in i.columns]

    for dfColLower, attrDef in catalogueTypeMap.items():

        # Only process columns that actually exist in the dataframe
        if dfColLower not in dfColumns:
            if Debug == 1:
                Log.Write(f"CastAttributes: Column '{attrDef['Name']}' in catalogue but not in dataframe — skipping")
            continue

        # Get the original case column name from the dataframe
        originalColName = next(c for c in i.columns if c.lower() == dfColLower)

        sparkType = resolveSparkType(
            attrDef['Type'],
            attrDef['Precision'],
            attrDef['Scale']
        )

        if Debug == 1:
            Log.Write(f"CastAttributes: Casting '{originalColName}' from string to {sparkType}")

        try:
            i = i.withColumn(originalColName, col(originalColName).cast(sparkType))
        except Exception as e:
            Log.Write(f"CastAttributes: Failed to cast '{originalColName}' to {sparkType}: {str(e)}")
            raise

    if Debug == 1:
        Log.Write("** Cleansing.CastAttributes - Completed")

    return i
