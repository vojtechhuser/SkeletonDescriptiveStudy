library(SkeletonDescriptiveStudy)

# Optional: specify where the temporary files (used by the ff package) will be created:
options(fftempdir = "c:/b/FFtemp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "c:/b/a"

#load env variables
source('~/secret/conn.R')
schema='onek'
# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = Sys.getenv("DBMS"),
                                                                server = Sys.getenv("DBSERVER"),
                                                                user = Sys.getenv("USER"),
                                                                password = Sys.getenv("PW"),
                                                                port = Sys.getenv("DBPORT")
                                                                ,schema=schema)

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- schema

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "gpc_results"
cohortTable <- "huserv_desciptive3"

# Some meta-information that will be used by the export function:
databaseId <- "dbid"
databaseName <- "mydb"
databaseDescription <- "my db description"

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL


#end of params

#draft of execute function
packageName='SkeletonDescriptiveStudy'
pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = packageName)
cohortsToCreate <- read.csv(pathToCsv)
cohortsToCreate


conn <- DatabaseConnector::connect(connectionDetails)

SkeletonDescriptiveStudy:::.populateCohorts(packageName='SkeletonDescriptiveStudy',
         connection = conn,
               cdmDatabaseSchema = cdmDatabaseSchema,
               cohortDatabaseSchema = cohortDatabaseSchema,
               cohortTable = cohortTable,
               oracleTempSchema = oracleTempSchema,
               outputFolder = outputFolder)

disconnect(conn)



#
#
# createCohorts(connectionDetails = connectionDetails,
#               cdmDatabaseSchema = cdmDatabaseSchema,
#               cohortDatabaseSchema = cohortDatabaseSchema,
#               cohortTable = cohortTable,
#               oracleTempSchema = oracleTempSchema,
#               outputFolder = outputFolder)
#
#
#
#
# execute(connectionDetails = connectionDetails,
#         cdmDatabaseSchema = cdmDatabaseSchema,
#         cohortDatabaseSchema = cohortDatabaseSchema,
#         cohortTable = cohortTable,
#         oracleTempSchema = oracleTempSchema,
#         outputFolder = outputFolder,
#         databaseId = databaseId,
#         databaseName = databaseName,
#         databaseDescription = databaseDescription,
#         createCohorts = TRUE,
#         synthesizePositiveControls = TRUE,
#         runAnalyses = TRUE,
#         runDiagnostics = TRUE,
#         packageResults = TRUE,
#         maxCores = maxCores)
#
# resultsZipFile <- file.path(outputFolder, "export", paste0("Results", databaseId, ".zip"))
# dataFolder <- file.path(outputFolder, "shinyData")
#
# prepareForEvidenceExplorer(resultsZipFile = resultsZipFile, dataFolder = dataFolder)
#
# launchEvidenceExplorer(dataFolder = dataFolder, blind = TRUE, launch.browser = FALSE)
