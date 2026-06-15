# =============================================================================
# CastAttributes — add to Cleansing notebook (v2)
# =============================================================================
# CHANGE 1: Add to TransformationFunctions dict in Cleansing.__init__:
#   'CastAttributes': self.CastAttributes
#
# CHANGE 2: Add method below to the Cleansing class.
#
# USAGE in .properties CleansingTransformations:
#
#   {"CastAttributes": "cleansed.MriHorizon.places.Tenant"}
#
#   The value is the SINK entity's own EntityFullyQualifiedName — i.e. the
#   exact same EFQN passed into Utility.GetEntity(EntityFullyQualifiedName=...,
#   Role="Sink") in the Wrapper notebook for this entity.
#
#   It serves two purposes:
#     1. Satisfies PerformCleansingTransformations' requirement that the
#        value be non-empty for the transform to fire at all.
#     2. Acts as a defensive self-check — confirms the SinkEntity global
#        (relied upon below) actually refers to THIS entity before any
#        casting takes place. If it doesn't match, fails loudly and early
#        with a clear message rather than producing wrong casts or a
#        bare NameError.
#
# NO FILE I/O — catalogue data comes from SinkEntity.oProperties, which was
# already fully loaded by ClassProperties when Utility.GetEntity() ran in
# the Wrapper, BEFORE main() was called.
#
# RELIES ON: SinkEntity — a global set in the Wrapper notebook's top-level
# scope and shared into this notebook's namespace via %run. Same category
# of shared global as Debug, Log, Utility — consistent with existing
# framework conventions.
#
# ORDER: RenameAttributes -> CastAttributes -> CalculateAttribute (hashes etc.)
#
# CATALOGUE FIELDS (Catalogue.Schema.Attributes):
#   Name     : column name (required)
#   Type     : string|boolean|date|datetime|decimal|int|bigint|float etc. (required)
#   Format   : Spark date/time format e.g. "yyyy-MM-dd" (optional)
#              If absent: cast() used — safe for ISO standard formats
#              If present: to_date() or to_timestamp() used with format
#   Timezone : IANA timezone e.g. "UTC" (optional, datetime/timestamp only)
#              If present: overrides the function's default timezone for
#                          this column only.
#              If absent:  the default timezone (Europe/London, see
#                          DEFAULT_TIMEZONE below) is applied automatically.
#                          This is the expected behaviour for the majority
#                          of timestamp columns — explicit Timezone is only
#                          needed for the exceptions.
# =============================================================================

def CastAttributes(self, i, value):
    """
    Automatically casts dataframe columns to types defined in
    SinkEntity.oProperties.Catalogue.Schema.Attributes — no file read.

    value : str
        The sink entity's own EntityFullyQualifiedName, e.g.
        "cleansed.MriHorizon.places.Tenant"
        Used as a defensive cross-check against the SinkEntity global
        and to satisfy the non-empty value requirement.

    Handles:
      - Simple type casting    string -> int, boolean, bigint, decimal etc.
      - Date casting           with optional Format for non-standard formats
      - Timestamp casting      with optional Format
      - Timezone conversion    UTC -> target timezone via from_utc_timestamp
                               for datetime/timestamp columns only.
                               Uses per-column catalogue 'Timezone' if present,
                               otherwise DEFAULT_TIMEZONE.

    Only casts when current dtype differs from catalogue type — idempotent.
    Reuses self.CalculateAttribute (expr() pathway) — consistent with framework.
    """

    if Debug == 1: Log.Write("** Cleansing.CastAttributes - Started")

    # ------------------------------------------------------------------
    # Default timezone applied to any timestamp/datetime column that does
    # NOT have an explicit 'Timezone' in its catalogue attribute definition.
    # Per-column 'Timezone' in the catalogue overrides this.
    # ------------------------------------------------------------------
    DEFAULT_TIMEZONE = "Europe/London"

    # ------------------------------------------------------------------
    # Step 1: Defensive self-check.
    #
    # 'value' is this entity's own EFQN. SinkEntity is a global set in the
    # Wrapper notebook and shared into this namespace via %run. Confirm it
    # actually refers to THIS entity before trusting SinkEntity.oProperties
    # for the catalogue below.
    # ------------------------------------------------------------------
    expectedEFQN = value.strip()
    actualEFQN   = SinkEntity.oProperties.EntityFullyQualifiedName

    if actualEFQN.lower() != expectedEFQN.lower():
        raise Exception(
            f"CastAttributes: properties file declares EntityFullyQualifiedName "
            f"'{expectedEFQN}' but the SinkEntity in scope is '{actualEFQN}'. "
            f"CastAttributes relies on the global SinkEntity matching the "
            f"entity being cleansed — check the Wrapper notebook."
        )

    # ------------------------------------------------------------------
    # Step 2: Catalogue attributes — already in memory, no file read.
    # ------------------------------------------------------------------
    catalogueAttributes = SinkEntity.oProperties.Catalogue.Schema.Attributes

    if Debug == 1: Log.Write(f"CastAttributes: {len(catalogueAttributes)} catalogue attributes for {actualEFQN}")

    # ------------------------------------------------------------------
    # Step 3: Helper functions — defined once, outside the loop.
    #
    # resolveSqlType : catalogue Type string -> SQL type string for cast()
    #                  mirrors ClassDeltaTable._GetSchemaString
    # normaliseType  : normalise Spark-reported types for comparison,
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
    # Step 4: Current dataframe dtype lookup.
    # { column_name_lower -> spark_type_string }
    # ------------------------------------------------------------------
    dfTypeMap = {c.lower(): t for c, t in i.dtypes}

    castCount = skipCount = missingCount = 0

    # ------------------------------------------------------------------
    # Step 5: Iterate catalogue attributes and cast where needed.
    #
    # A) Not in dataframe         -> skip silently
    # B) Types already match
    #    + no timezone needed     -> skip
    #    + timezone needed        -> apply from_utc_timestamp only
    # C) Simple types             -> cast(col as type)
    # D) Date
    #    Format absent            -> cast(col as date)
    #    Format present            -> to_date(col, 'format')
    # E) Timestamp
    #    Step 1 — cast to timestamp:
    #      Format absent          -> cast(col as timestamp)
    #      Format present         -> to_timestamp(col, 'format')
    #    Step 2 — timezone (always applied for timestamp columns):
    #      Catalogue 'Timezone' if present, else DEFAULT_TIMEZONE
    #      from_utc_timestamp(col, 'timezone')
    #
    # All expressions passed to self.CalculateAttribute (expr() pathway).
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
        isTimestamp = (sqlType == 'timestamp')

        # Timezone: per-column catalogue value overrides DEFAULT_TIMEZONE.
        # Only relevant for timestamp columns.
        timezone    = (attr.get('Timezone') or DEFAULT_TIMEZONE) if isTimestamp else None

        typesMatch  = (normaliseType(dfTypeMap[colLower]) == normaliseType(sqlType))

        # B) Types match — skip, or apply timezone only
        if typesMatch:
            if not (isTimestamp and timezone):
                if Debug == 1: Log.Write(f"CastAttributes: '{colName}' already '{sqlType}' — skipping")
                skipCount += 1
                continue
            # Already timestamp — apply timezone conversion only
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

        # E) Timestamp — step 2: timezone (always applied)
        if isTimestamp and timezone:
            i = self.CalculateAttribute(i, f"{colName}:from_utc_timestamp({colName}, '{timezone}')")
            if Debug == 1: Log.Write(f"CastAttributes: '{colName}' timezone -> '{timezone}'")
            castCount += 1

    Log.Write(f"CastAttributes: Complete — {castCount} cast, {skipCount} skipped, {missingCount} not in dataframe")
    if Debug == 1: Log.Write("** Cleansing.CastAttributes - Completed")

    return i
