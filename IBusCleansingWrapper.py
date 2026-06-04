# Databricks notebook source
# MAGIC %md
# MAGIC # IBusCleansingWrapper
# MAGIC
# MAGIC Entry point for the iBus Cleansing framework.
# MAGIC Triggered by ADF pipeline passing EntityFullyQualifiedName, Action, BatchId.
# MAGIC
# MAGIC Flow:
# MAGIC   ADF Pipeline
# MAGIC     -> IBusCleansingWrapper
# MAGIC         -> %run Initialisation  (loads all classes)
# MAGIC         -> Utility.GetEntity()  (instantiates ClassDeltaTable for sink)
# MAGIC         -> main(Entity, Action) (drives full pipeline)
# MAGIC              -> PerformTransformation:
# MAGIC                   reads SourceEntities[].EFQN from properties
# MAGIC                   Utility.GetEntity(EFQN, Role="Source") -> ClassDelimitedFile
# MAGIC                   ClassDelimitedFile.Read(HighWaterMark=SinkCurrentHWM)
# MAGIC                   Cleansing().PerformCleansingTransformations()
# MAGIC                   Entity.SetChangeData()
# MAGIC              -> DataSourceType="multitable":
# MAGIC                   main does NOT call _CommitData or _WriteHWM
# MAGIC                   action function handles SCD2 merge + HWM itself

# COMMAND ----------
# Cell 1
print("Loading IBusCleansingWrapper")

# COMMAND ----------
# Cell 2 - Initialise all framework notebooks
# Loads: Cleansing, ClassProperties, ClassDeltaTable, ClassDelimitedFile,
#        Utility, main, Log, Config, ClassMetaFile etc.
%run "/Shared/Initialisation"

# COMMAND ----------
# Cell 3 - Widgets
dbutils.widgets.text("EntityFullyQualifiedName", "[EntityFullyQualifiedNameNotProvided]", "EntityFullyQualifiedName")
dbutils.widgets.text("Action",                   "[ActionNotProvided]",                   "Action")
dbutils.widgets.text("BatchId",                  "-1",                                    "BatchId")

EntityFullyQualifiedName = dbutils.widgets.get("EntityFullyQualifiedName")
Action                   = dbutils.widgets.get("Action")
BatchId                  = dbutils.widgets.get("BatchId")

# Debug is set here as a global — available in all %run notebooks
Debug = Config.Debug

Log.Write(f"####################### IBusCleansingWrapper for {EntityFullyQualifiedName} - {Action} - Starting")
Log.Write(f"AnalyticsProduct:         {AnalyticsProduct}")
Log.Write(f"StorageAccount:           {StorageAccount}")
Log.Write(f"EntityFullyQualifiedName: {EntityFullyQualifiedName}")
Log.Write(f"Action:                   {Action}")
Log.Write(f"BatchId:                  {BatchId}")
Log.Write(f"Debug:                    {Debug}")

# COMMAND ----------
# Cell 4 - Validate widgets
if EntityFullyQualifiedName == "[EntityFullyQualifiedNameNotProvided]":
    raise Exception("EntityFullyQualifiedName widget has not been set.")

if Action == "[ActionNotProvided]":
    raise Exception("Action widget has not been set.")

# COMMAND ----------
# Cell 5 - Instantiate sink entity
# Utility.GetEntity:
#   1. ClassProperties(EFQN) -> looks up EntityPath from metadata.entities
#      -> loads .properties JSON -> mounts ADLS container
#   2. GetEntityType -> reads System.FileFormat.Extension -> "delta"
#   3. ClassDeltaTable(**kwargs) instantiated:
#      -> _TableName, _Schema, _TargetFormat set
#      -> CurrentHWM read from metafile (Role="Sink")
#      -> MountPoint resolved
SinkEntity = Utility.GetEntity(
    EntityFullyQualifiedName = EntityFullyQualifiedName,
    Role                     = "Sink",
    Action                   = Action
)

Log.Write(f"SinkEntity instantiated:  {EntityFullyQualifiedName}")
Log.Write(f"SinkEntity._TableName:    {SinkEntity._TableName}")
Log.Write(f"SinkEntity.CurrentHWM:    {SinkEntity.CurrentHWM}")

# COMMAND ----------
# Cell 6 - Load action function notebook(s)
# ActionFunctions defined in Extended.Cleanse.ActionFunctions of properties file
# e.g. ["DeltaLoadMriTenant"]
# Each notebook is %run to bring its function into scope.
# The function is then called by main.PerformTransformation via eval().

try:
    ActionFunctions = SinkEntity.oProperties.Extended.Cleanse.ActionFunctions
except Exception as e:
    raise Exception(
        f"ActionFunctions not found in Extended.Cleanse of {EntityFullyQualifiedName}. "
        f"Define ActionFunctions in the properties file. Error: {str(e)}"
    )

Log.Write(f"ActionFunctions: {ActionFunctions}")

for af in ActionFunctions:
    NotebookPath = f"/Shared/IBusCleansing/Actions/{af}"
    Log.Write(f"Loading action function notebook: {NotebookPath}")
    %run $NotebookPath

# COMMAND ----------
# Cell 7 - Call main
# main drives the full pipeline:
#
# For cleanse with DataSourceType="multitable":
#   GetActionFunctions()
#     -> reads Extended.Cleanse.CleansingTransformations
#     -> auto-appends InsertedDateTime/UpdatedDateTime if not defined
#
#   PerformTransformation() for "cleanse":
#     -> reads SourceEntities[0].EFQN from properties
#     -> Utility.GetEntity(EFQN, Role="Source") -> ClassDelimitedFile
#        ClassDelimitedFile.__init__:
#          FilePath = MountPoint + EntityPath (resolved via ClassProperties)
#          FileHeader, Delimiter etc. from System.FileFormat.*
#     -> SinkCurrentHWM  = SinkEntity.GetCurrentHWM() (from metafile)
#     -> SourceCurrentHWM = SourceEntity.GetCurrentHWM() (= now())
#     -> SinkEntity.SetNewHWM(NewHWM = SourceCurrentHWM)
#     -> Source_file_filter from Extended.Cleanse.SourceFilePathFilter (optional)
#     -> dfChangeData = SourceEntity.Read(
#            HighWaterMark  = SinkCurrentHWM,
#            FilePathFilter = Source_file_filter
#        )
#        Read() filters files by file_modification_time > SinkCurrentHWM
#     -> Cleansing().PerformCleansingTransformations(dfChangeData, each_transform)
#        Applies: RenameAttributes -> CastAttributes -> CalculateAttribute
#     -> arranges columns in catalogue order via GetColumnList
#     -> SinkEntity.SetChangeData(ChangeData = dfCleansed)
#
# DataSourceType="multitable":
#   main does NOT call _CommitData or _WriteHWM
#   -> eval("DeltaLoadMriTenant")(SinkEntity, Action) is called
#   -> DeltaLoadMriTenant handles SCD2 merge + WriteNewHWM itself

Main = main(
    Entity = SinkEntity,
    Action = Action
)

Log.Write(f"####################### IBusCleansingWrapper for {EntityFullyQualifiedName} - {Action} - Completed")
