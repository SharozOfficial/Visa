# =============================================================================
# CastAttributes - Add to Cleansing notebook
# =============================================================================
#
# CHANGE 1: In Cleansing.__init__, add one line to TransformationFunctions dict:
#
#   self.TransformationFunctions = {
#       'DropDuplicates':                           self.DropDuplicates,
#       'FlattenAttribute':                         self.FlattenAttribute,
#       'RenameAttributes':                         self.RenameAttributes,
#       'RemoveAttributes':                         self.RemoveAttributes,
#       'CalculateAttribute':                       self.CalculateAttribute,
#       'ConvertStringToStructAttribute':           self.ConvertStringToStructAttribute,
#       'MaskAttributes':                           self.MaskAttributes,
#       'NullAttributes':                           self.NullAttributes,
#       'ExtractValueFromOptionalAttribute':        self.ExtractValueFromOptionalAttribute,
#       'ExtractValueFromJsonAttributeOfXmlSource': self.ExtractValueFromJsonAttributeOfXmlSource,
#       'DropDuplicatesByKey':                      self.DropDuplicatesByKey,
#       'CastAttributes':                           self.CastAttributes,  # <-- ADD THIS
#   }
#
# CHANGE 2: Add the method below to the Cleansing class.
#
# =============================================================================
# USAGE in .properties file CleansingTransformations:
#
#   "CleansingTransformations": [
#       {"RenameAttributes": "TNT_REF:TenantReference, USAGE_CD:UsageCode"},
#       {"CastAttributes": "/mnt/metadata/definition/properties/adlsg2/cleansed/MriTenant.properties"},
#       {"CalculateAttribute": "hashedKey:sha2(concat_ws('||', TenantReference), 256)"}
#   ]
#
# ORDER MATTERS:
#   1. RenameAttributes   — rename source columns to target names
#   2. CastAttributes     — cast all columns + timezone conversions
#   3. CalculateAttribute — remaining custom expressions e.g. hashes
#
# Eliminates the need to write individual CalculateAttribute cast entries
# and from_utc_timestamp entries for every column.
#
# =============================================================================
# CATALOGUE ATTRIBUTE FIELDS USED:
#
#   Name     : column name (required)
#   Type     : target datatype (required)
#              string | nvarchar | varchar | char
#              bit | boolean | bool
#              date
#              datetime | datetimeoffset | datetime2
#              decimal | numeric
#              long | bigint | int | integer
#              float | double | smallint | tinyint
#
#   Format   : optional — Spark SQL date/datetime format string
#              For date:      e.g. "yyyy-MM-dd", "dd/MM/yyyy"
#              For datetime:  e.g. "yyyy-MM-dd HH:mm:ss"
#              If absent: uses cast() — safe for ISO standard formats
#              If present: uses to_date() or to_timestamp() with format
#
#   Timezone : optional — IANA timezone string
#              ONLY applied to datetime/timestamp columns, NOT date
#              e.g. "Europe/London", "UTC", "America/New_York"
#              If present: from_utc_timestamp(col, timezone) applied
#                          after timestamp cast (assumes source is UTC)
#              If absent:  no timezone conversion
#
# EXAMPLE CATALOGUE ATTRIBUTES:
#
#   Simple cast:
#   { "Name": "TenantReference", "Type": "string" }
#   -> cast(TenantReference as string)
#
#   Date with format:
#   { "Name": "fileDate", "Type": "date", "Format": "yyyy-MM-dd" }
#   -> to_date(fileDate, 'yyyy-MM-dd')
#
#   Timestamp with format and timezone:
#   { "Name": "createdTime", "Type": "datetime",
#     "Format": "yyyy-MM-dd HH:mm:ss", "Timezone": "Europe/London" }
#   -> to_timestamp(createdTime, 'yyyy-MM-dd HH:mm:ss')
#   -> from_utc_timestamp(createdTime, 'Europe/London')
#
#   Timestamp without format, with timezone:
#   { "Name": "btpTimeIn", "Type": "datetime", "Timezone": "Europe/London" }
#   -> cast(btpTimeIn as timestamp)
#   -> from_utc_timestamp(btpTimeIn, 'Europe/London')
# =============================================================================

def CastAttributes(self, i, propertiesFilePath):
    """
    CastAttributes: Automatically casts dataframe columns to the datatypes
    defined in Catalogue.Schema.Attributes of the sink .properties file.

    Handles:
      - Simple type casting    string -> int, boolean, bigint etc.
      - Date casting           string -> date, with optional Format
      - Timestamp casting      string -> timestamp, with optional Format
      - Timezone conversion    UTC -> target timezone via from_utc_timestamp
                               for datetime/timestamp columns only,
                               when Timezone field defined in catalogue

    Only casts when current dtype differs from catalogue dtype — idempotent.
    Reuses self.CalculateAttribute (expr() pathway) for all expressions —
    fully consistent with the existing Cleansing framework.

    Parameters
    ----------
    i                  : pyspark.sql.DataFrame
                         Columns must already be renamed to target names.
                         RenameAttributes must run before CastAttributes.
    propertiesFilePath : str
                         Path to the sink .properties file.
                         Handles both /mnt/ and /dbfs/ prefixes.

    Returns
    -------
    i : pyspark.sql.DataFrame
        Dataframe with all columns cast to catalogue-defined types.
    """
    import json

    if Debug == 1:
        Log.Write("** Cleansing.CastAttributes - Started")

    # ------------------------------------------------------------------
    # Step 1: Load properties file JSON
    # open() requires /dbfs/ prefix — normalise from /mnt/ if needed
    # ------------------------------------------------------------------
    try:
        if propertiesFilePath.startswith('/dbfs/'):
            fullPath = propertiesFilePath
        elif propertiesFilePath.startswith('/mnt/'):
            fullPath = '/dbfs' + propertiesFilePath
        else:
            fullPath = '/dbfs/' + propertiesFilePath.lstrip('/')

        with open(fullPath, 'r') as f:
            propertiesJson = json.load(f)

        if Debug == 1:
            Log.Write(f"CastAttributes: Loaded properties from {fullPath}")

    except Exception as e:
        raise Exception(
            f"CastAttributes: Could not load properties file "
            f"'{propertiesFilePath}': {str(e)}"
        )

    # ------------------------------------------------------------------
    # Step 2: Read Catalogue.Schema.Attributes
    # ------------------------------------------------------------------
    try:
        catalogueAttributes = propertiesJson['Catalogue']['Schema']['Attributes']
    except KeyError as e:
        raise Exception(
            f"CastAttributes: Catalogue.Schema.Attributes not found: {str(e)}"
        )

    # ------------------------------------------------------------------
    # Step 3: Define helpers — once, outside the loop
    #
    # resolveSqlType: catalogue Type string -> SQL cast type string
    #   mirrors ClassDeltaTable._GetSchemaString for consistency
    #
    # normaliseType:  normalise Spark dtype strings for comparison
    #   prevents unnecessary re-casts when types are equivalent
    #   e.g. Spark reports 'integer', catalogue says 'int' — same type
    # ------------------------------------------------------------------
    def resolveSqlType(colType, precision, scale):
        t = str(colType).lower().strip()
        if t in ('string', 'nvarchar', 'varchar', 'char'):
            return 'string'
        elif t in ('bit', 'boolean', 'bool'):
            return 'boolean'
        elif t == 'date':
            return 'date'
        elif t in ('datetime', 'datetimeoffset', 'datetime2'):
            return 'timestamp'
        elif t in ('decimal', 'numeric'):
            try:
                return f"decimal({int(precision)},{int(scale)})"
            except Exception:
                return 'decimal(18,6)'
        elif t in ('long', 'bigint'):
            return 'bigint'
        elif t == 'double':
            return 'double'
        elif t in ('int', 'integer'):
            return 'int'
        elif t == 'float':
            return 'float'
        elif t == 'smallint':
            return 'smallint'
        elif t == 'tinyint':
            return 'tinyint'
        else:
            if Debug == 1:
                Log.Write(f"CastAttributes: Unknown type '{t}' — defaulting to string")
            return 'string'

    def normaliseType(t):
        t = str(t).lower().strip()
        if t.startswith('decimal'):                          return 'decimal'
        if t in ('int', 'integer'):                          return 'integer'
        if t in ('bool', 'boolean'):                         return 'boolean'
        if t in ('long', 'bigint'):                          return 'bigint'
        if t in ('datetime', 'datetimeoffset',
                 'datetime2', 'timestamp'):                  return 'timestamp'
        if t in ('string', 'nvarchar', 'varchar', 'char'):  return 'string'
        return t

    # ------------------------------------------------------------------
    # Step 4: Build catalogue type map
    # { column_name_lower -> { Name, SqlType, Format, Timezone, IsTimestamp } }
    # ------------------------------------------------------------------
    catalogueTypeMap = {}
    for attr in catalogueAttributes:
        colName   = str(attr.get('Name',  '')).strip()
        colType   = str(attr.get('Type',  '')).strip()
        precision = attr.get('Precision') or attr.get('NumericPrecision')
        scale     = attr.get('Scale')     or attr.get('NumericScale')
        fmt       = attr.get('Format')      # optional — date/datetime format
        timezone  = attr.get('Timezone')    # optional — IANA timezone string

        if colName and colType:
            sqlType      = resolveSqlType(colType, precision, scale)
            isTimestamp  = (sqlType == 'timestamp')  # timezone only for timestamp

            catalogueTypeMap[colName.lower()] = {
                'Name':        colName,
                'SqlType':     sqlType,
                'Format':      fmt,
                'Timezone':    timezone,
                'IsTimestamp': isTimestamp
            }

    if Debug == 1:
        Log.Write(f"CastAttributes: {len(catalogueTypeMap)} catalogue attributes loaded")

    # ------------------------------------------------------------------
    # Step 5: Build dataframe dtype lookup
    # { column_name_lower -> current_spark_type_string }
    # ------------------------------------------------------------------
    dfTypeMap = {c.lower(): t for c, t in i.dtypes}

    # ------------------------------------------------------------------
    # Step 6: Compare and cast
    #
    # For each catalogue column present in the dataframe:
    #
    # A) SIMPLE TYPES (string, boolean, int, bigint, float, decimal etc.)
    #    cast(col as type)
    #
    # B) DATE
    #    Format absent  -> cast(col as date)        ISO standard YYYY-MM-DD
    #    Format present -> to_date(col, 'format')   non-standard formats
    #    No timezone applied to date columns.
    #
    # C) TIMESTAMP / DATETIME
    #    Step 1 — string to timestamp:
    #      Format absent  -> cast(col as timestamp)
    #      Format present -> to_timestamp(col, 'format')
    #    Step 2 — timezone conversion (only if Timezone defined):
    #      from_utc_timestamp(col, 'Europe/London')
    #      Assumes source data arrives in UTC.
    #      If Timezone absent: no conversion applied.
    #
    # All expressions passed to self.CalculateAttribute (expr() pathway)
    # keeping everything consistent with the existing framework.
    # ------------------------------------------------------------------
    castCount    = 0
    skipCount    = 0
    missingCount = 0

    for colLower, catalogueDef in catalogueTypeMap.items():

        # Column in catalogue but not in dataframe — skip silently
        if colLower not in dfTypeMap:
            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{catalogueDef['Name']}' "
                    f"not in dataframe — skipping"
                )
            missingCount += 1
            continue

        currentType  = dfTypeMap[colLower]
        expectedType = catalogueDef['SqlType']
        colName      = catalogueDef['Name']
        fmt          = catalogueDef['Format']
        timezone     = catalogueDef['Timezone']
        isTimestamp  = catalogueDef['IsTimestamp']

        typesMatch = (normaliseType(currentType) == normaliseType(expectedType))

        # If types already match AND no timezone needed — nothing to do
        if typesMatch and not (isTimestamp and timezone):
            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{colName}' already "
                    f"'{currentType}' — skipping"
                )
            skipCount += 1
            continue

        # If already a timestamp but timezone still needed — skip cast,
        # apply timezone only. Avoids double-casting.
        if typesMatch and isTimestamp and timezone:
            tzExpr = f"{colName}:from_utc_timestamp({colName}, '{timezone}')"
            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{colName}' already timestamp, "
                    f"applying timezone '{timezone}'"
                )
            i = self.CalculateAttribute(i, tzExpr)
            castCount += 1
            continue

        # -----------------------------------------------
        # A) Simple types
        # -----------------------------------------------
        if expectedType not in ('date', 'timestamp'):
            expr = f"{colName}:cast({colName} as {expectedType})"
            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{colName}' "
                    f"'{currentType}' -> '{expectedType}' | {expr}"
                )
            i = self.CalculateAttribute(i, expr)
            castCount += 1

        # -----------------------------------------------
        # B) Date
        # -----------------------------------------------
        elif expectedType == 'date':
            if fmt:
                expr = f"{colName}:to_date({colName}, '{fmt}')"
            else:
                expr = f"{colName}:cast({colName} as date)"

            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{colName}' "
                    f"'{currentType}' -> date | {expr}"
                )
            i = self.CalculateAttribute(i, expr)
            castCount += 1

        # -----------------------------------------------
        # C) Timestamp — two steps
        # -----------------------------------------------
        elif expectedType == 'timestamp':

            # Step 1: string -> timestamp
            if fmt:
                expr = f"{colName}:to_timestamp({colName}, '{fmt}')"
            else:
                expr = f"{colName}:cast({colName} as timestamp)"

            if Debug == 1:
                Log.Write(
                    f"CastAttributes: '{colName}' "
                    f"'{currentType}' -> timestamp | {expr}"
                )
            i = self.CalculateAttribute(i, expr)
            castCount += 1

            # Step 2: UTC -> target timezone (datetime/timestamp only)
            if timezone:
                tzExpr = (
                    f"{colName}:"
                    f"from_utc_timestamp({colName}, '{timezone}')"
                )
                if Debug == 1:
                    Log.Write(
                        f"CastAttributes: '{colName}' "
                        f"timezone -> '{timezone}' | {tzExpr}"
                    )
                i = self.CalculateAttribute(i, tzExpr)
                castCount += 1

    Log.Write(
        f"CastAttributes: Complete — "
        f"{castCount} expressions applied, "
        f"{skipCount} already correct, "
        f"{missingCount} not in dataframe"
    )

    if Debug == 1:
        Log.Write("** Cleansing.CastAttributes - Completed")

    return i
