# =============================================================================
# CastAttributes — add to Cleansing notebook
# =============================================================================
# CHANGE 1: Add to TransformationFunctions dict in Cleansing.__init__:
#   'CastAttributes': self.CastAttributes
#
# CHANGE 2: Add method below to the Cleansing class.
#
# USAGE in .properties CleansingTransformations:
#
#   Without default timezone (column-level Timezone in catalogue only):
#   {"CastAttributes": "adlsg2/adlsg2analytics/cleansed/MriTenant"}
#
#   With default timezone (applied to all datetime/timestamp columns
#   that do not have their own Timezone defined in catalogue):
#   {"CastAttributes": "adlsg2/adlsg2analytics/cleansed/MriTenant|Europe/London"}
#
# The relative path matches what is stored in metadata.entities.EntityPath.
# Full path is built internally using mountpointMetadata (set by DBFSMountPoints,
# guaranteed in scope before PerformTransformation runs).
#
# ORDER: RenameAttributes -> CastAttributes -> CalculateAttribute (hashes etc.)
#
# CATALOGUE FIELDS:
#   Name     : column name (required)
#   Type     : string|boolean|date|datetime|decimal|int|bigint|float etc. (required)
#   Format   : Spark date/time format e.g. "yyyy-MM-dd" (optional)
#              If absent: cast() used — safe for ISO standard formats
#              If present: to_date() or to_timestamp() used with format
#   Timezone : IANA timezone e.g. "Europe/London" (optional, datetime only)
#              Overrides the default timezone parameter for that column.
#              If absent: default timezone from parameter is used (if provided)
#              If no default either: no timezone conversion applied
# =============================================================================

def CastAttributes(self, i, value):
    """
    Automatically casts dataframe columns to types defined in
    Catalogue.Schema.Attributes of the sink .properties file.

    value : str
        Relative entity path matching metadata.entities.EntityPath,
        optionally with a default timezone separated by |:
        "adlsg2/adlsg2analytics/cleansed/MriTenant"
        "adlsg2/adlsg2analytics/cleansed/MriTenant|Europe/London"

    Full path built as:
        /dbfs{mountpointMetadata}/definition/properties/{relativePath}.properties

    Handles:
      - Simple type casting    string -> int, boolean, bigint, decimal etc.
      - Date casting           with optional Format for non-standard formats
      - Timestamp casting      with optional Format
      - Timezone conversion    UTC -> target timezone via from_utc_timestamp
                               for datetime/timestamp columns only

    Only casts when current dtype differs from catalogue type — idempotent.
    Reuses self.CalculateAttribute (expr() pathway) — consistent with framework.
    """
    import json

    if Debug == 1: Log.Write("** Cleansing.CastAttributes - Started")

    # ------------------------------------------------------------------
    # Step 1: Parse value — relative path + optional default timezone
    # ------------------------------------------------------------------
    parts        = value.split('|')
    relativePath = parts[0].strip()
    defaultTZ    = parts[1].strip() if len(parts) > 1 else None

    # ------------------------------------------------------------------
    # Step 2: Build full path and load properties file
    # mountpointMetadata is set by DBFSMountPoints (%run in Initialisation)
    # and is in scope as a global variable.
    # open() requires /dbfs/ prefix to access mounted ADLS paths.
    # ------------------------------------------------------------------
    fullPath = f"/dbfs{mountpointMetadata}/definition/properties/{relativePath}.properties"

    if Debug == 1: Log.Write(f"CastAttributes: Loading properties from {fullPath}")

    try:
        with open(fullPath, 'r') as f:
            catalogueAttributes = json.load(f)['Catalogue']['Schema']['Attributes']
    except Exception as e:
        raise Exception(f"CastAttributes: Could not load '{fullPath}': {str(e)}")

    if Debug == 1: Log.Write(f"CastAttributes: {len(catalogueAttributes)} catalogue attributes loaded")

    # ------------------------------------------------------------------
    # Step 3: Define helpers — once, outside the loop
    #
    # resolveSqlType : catalogue Type string -> SQL type string for cast()
    #                  mirrors ClassDeltaTable._GetSchemaString
    # normaliseType  : normalise Spark-reported types for comparison
    #                  prevents unnecessary re-casts
    #                  e.g. Spark reports 'integer', catalogue says 'int'
    # ------------------------------------------------------------------
    def resolveSqlType(t, precision, scale):
        t = str(t).lower().strip()
        if t in ('string', 'nvarchar', 'varchar', 'char'):   return 'string'
        if t in ('bit', 'boolean', 'bool'):                   return 'boolean'
        if t == 'date':                                        return 'date'
        if t in ('datetime', 'datetimeoffset', 'datetime2'):  return 'timestamp'
        if t in ('decimal', 'numeric'):
            try:    return f"decimal({int(precision)},{int(scale)})"
            except: return 'decimal(18,6)'
        if t in ('long', 'bigint'):  return 'bigint'
        if t in ('int', 'integer'):  return 'int'
        if t == 'double':            return 'double'
        if t == 'float':             return 'float'
        if t == 'smallint':          return 'smallint'
        if t == 'tinyint':           return 'tinyint'
        return 'string'

    def normaliseType(t):
        t = str(t).lower().strip()
        if t.startswith('decimal'):                        return 'decimal'
        if t in ('int', 'integer'):                        return 'integer'
        if t in ('bool', 'boolean'):                       return 'boolean'
        if t in ('long', 'bigint'):                        return 'bigint'
        if t in ('datetime','datetimeoffset','datetime2',
                 'timestamp'):                             return 'timestamp'
        if t in ('string','nvarchar','varchar','char'):    return 'string'
        return t

    # ------------------------------------------------------------------
    # Step 4: Current dataframe dtype lookup
    # { column_name_lower -> spark_type_string }
    # ------------------------------------------------------------------
    dfTypeMap = {c.lower(): t for c, t in i.dtypes}

    castCount = skipCount = missingCount = 0

    # ------------------------------------------------------------------
    # Step 5: Iterate catalogue attributes and cast where needed
    #
    # For each attribute in the catalogue:
    #
    # A) Not in dataframe         -> skip silently
    # B) Types already match
    #    + no timezone needed     -> skip
    #    + timezone needed        -> apply from_utc_timestamp only
    # C) Simple types             -> cast(col as type)
    # D) Date
    #    Format absent            -> cast(col as date)
    #    Format present           -> to_date(col, 'format')
    # E) Timestamp
    #    Step 1 — cast to timestamp:
    #      Format absent          -> cast(col as timestamp)
    #      Format present         -> to_timestamp(col, 'format')
    #    Step 2 — timezone (if Timezone or defaultTZ defined):
    #      from_utc_timestamp(col, 'timezone')
    #      Column-level Timezone overrides defaultTZ
    #
    # All expressions passed to self.CalculateAttribute (expr() pathway)
    # ------------------------------------------------------------------
    for attr in catalogueAttributes:
        colName = str(attr.get('Name', '')).strip()
        colType = str(attr.get('Type', '')).strip()
        if not colName or not colType:
            continue

        colLower = colName.lower()

        # A) Not in dataframe
        if colLower not in dfTypeMap:
            if Debug == 1: Log.Write(f"CastAttributes: '{colName}' not in dataframe — skipping")
            missingCount += 1
            continue

        sqlType     = resolveSqlType(colType,
                                     attr.get('Precision') or attr.get('NumericPrecision'),
                                     attr.get('Scale')     or attr.get('NumericScale'))
        fmt         = attr.get('Format')
        timezone    = attr.get('Timezone') or (defaultTZ if sqlType == 'timestamp' else None)
        isTimestamp = (sqlType == 'timestamp')
        typesMatch  = (normaliseType(dfTypeMap[colLower]) == normaliseType(sqlType))

        # B) Types match — skip or timezone only
        if typesMatch:
            if not (isTimestamp and timezone):
                if Debug == 1: Log.Write(f"CastAttributes: '{colName}' already '{sqlType}' — skipping")
                skipCount += 1
                continue
            # Already timestamp but timezone needed
            i = self.CalculateAttribute(i, f"{colName}:from_utc_timestamp({colName}, '{timezone}')")
            if Debug == 1: Log.Write(f"CastAttributes: '{colName}' timezone only -> '{timezone}'")
            castCount += 1
            continue

        # C) Simple types
        if sqlType not in ('date', 'timestamp'):
            expr = f"{colName}:cast({colName} as {sqlType})"

        # D) Date
        elif sqlType == 'date':
            expr = f"{colName}:to_date({colName}, '{fmt}')" if fmt \
              else f"{colName}:cast({colName} as date)"

        # E) Timestamp — step 1: cast
        else:
            expr = f"{colName}:to_timestamp({colName}, '{fmt}')" if fmt \
              else f"{colName}:cast({colName} as timestamp)"

        if Debug == 1: Log.Write(f"CastAttributes: '{colName}' '{dfTypeMap[colLower]}' -> '{sqlType}' | {expr}")
        i = self.CalculateAttribute(i, expr)
        castCount += 1

        # E) Timestamp — step 2: timezone
        if isTimestamp and timezone:
            i = self.CalculateAttribute(i, f"{colName}:from_utc_timestamp({colName}, '{timezone}')")
            if Debug == 1: Log.Write(f"CastAttributes: '{colName}' timezone -> '{timezone}'")
            castCount += 1

    Log.Write(f"CastAttributes: Complete — {castCount} cast, {skipCount} skipped, {missingCount} not in dataframe")
    if Debug == 1: Log.Write("** Cleansing.CastAttributes - Completed")

    return i
