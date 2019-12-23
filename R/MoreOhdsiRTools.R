
#creates cohortTable, if it does not exist.
#uses short hand functions from DatabaseConnector package (not SqlRender!)
#relies on parameters convention in Atlas

.createCohortTable<- function(connection,cohortTable='cohort',cohortDatabaseSchema){

  csql<-"IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
  DROP TABLE @cohort_database_schema.@cohort_table;

  CREATE TABLE @cohort_database_schema.@cohort_table (
    cohort_definition_id INT,
    subject_id BIGINT,
    cohort_start_date DATE,
    cohort_end_date DATE
  );"
  result <- DatabaseConnector::renderTranslateExecuteSql(connection,
                                                         csql,
                                                         cohort_database_schema = cohortDatabaseSchema,
                                                         cohort_table = cohortTable)

  return(result)
}




# fetch sql code for a cohort by definitionId and outputs it
# adopted from OhdsiRTools and function there (insertCohortIntoPackage)
# OhdsiRTools::insertCohortDefinitionSetInPackage (but omits writing it into R package files)

.fetchDefinition<-function (definitionId,  baseUrl,name = NULL) {
  # if (!.checkBaseUrl(baseUrl)) {
  #   stop("Base URL not valid, should be like http://server.org:80/WebAPI")
  # }
  json <- OhdsiRTools::getCohortDefinitionExpression(definitionId = definitionId,
                                                     baseUrl = baseUrl)
  if (is.null(name)) {
    name <- stringr::str_replace(json$name,'\\s|\\:','-')
    #name <- stringr::str_replace_all('bad naem:','\\s|\\:','.')
    name
  }
  # if (!file.exists("inst/cohorts")) {
  #   dir.create("inst/cohorts", recursive = TRUE)
  # }
  # fileConn <- file(file.path("inst/cohorts", paste(name, "json",
  #                                                  sep = ".")))
  # writeLines(json$expression, fileConn)
  # close(fileConn)
  parsedExpression <- RJSONIO::fromJSON(json$expression)

  # if (generateStats) {
  #   jsonBody <- RJSONIO::toJSON(list(expression = parsedExpression,
  #                                    options = list(generateStats = TRUE)), digits = 23)
  # }
  # else {


  jsonBody <- RJSONIO::toJSON(list(expression = parsedExpression),digits = 23)
  # }
  httpheader <- c(Accept = "application/json; charset=UTF-8",
                  `Content-Type` = "application/json")
  url <- paste(baseUrl, "cohortdefinition", "sql", sep = "/")
  cohortSqlJson <- httr::POST(url, body = jsonBody, config = httr::add_headers(httpheader))
  cohortSqlJson <- httr::content(cohortSqlJson)
  sql <- cohortSqlJson$templateSql
  #cat(sql)
  # if (!file.exists("inst/sql/sql_server")) {
  #   dir.create("inst/sql/sql_server", recursive = TRUE)
  # }
  # fileConn <- file(file.path("inst/sql/sql_server", paste(name,
  #                                                         "sql", sep = ".")), open = "wb")
  # writeLines(sql, fileConn)
  # close(fileConn)
  return(sql)
}








# given external server ID and baseURL, execute on your data the definition and populate a specified cohort table
# Example
# connection <-connect(connectionDetails)
# res<-.executeExternalCohort(definitionId,connection,cdmDatabaseSchema,cohortDatabaseSchema,vocabularyDatabaseSchema
#                             ,cohortTable,baseUrl,createCohortTable = FALSE )
# DatabaseConnector::disconnect(connection)


.executeExternalCohort<-function(definitionId=1
                                 ,connection
                                 ,cdmDatabaseSchema='cdm'
                                 ,cohortDatabaseSchema=cdmDatabaseSchema
                                 ,vocabularyDatabaseSchema=cdmDatabaseSchema
                                 ,cohortTable='cohort'
                                 ,baseUrl
                                 ,createCohortTable=FALSE){



    if (createCohortTable) .createCohortTable(connection,cohortTable,cohortDatabaseSchema)

#fetch from inet

    sql<-.fetchDefinition(definitionId,baseUrl=baseUrl)

#cat(sql)


#translate

# renderTranslateExecuteSql() Render, translate, execute SQL code
# renderTranslateQuerySql.ffdf() Render, translate, and query to ffdf
# renderTranslateQuerySql() Render, translate, and query to data.frame
# SqlRender::loadRenderTranslateSql
#borrowing from .populateCohorts in skeleton

#sql <- "SELECT cohort_definition_id, COUNT(*) AS count FROM @cohort_database_schema.@cohort_table GROUP BY cohort_definition_id"
    #connection <-connect(connectionDetails)
    #on.exit(DatabaseConnector::disconnect(connection))
    result <- DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                     sql=sql,
                                                     #snakeCaseToCamelCase = TRUE,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     vocabulary_database_schema = vocabularyDatabaseSchema,
                                                     target_database_schema = cohortDatabaseSchema,
                                                     target_cohort_table = cohortTable,
                                                     target_cohort_id = definitionId
                                                     )

    return(list(query=sql,result=result))
}


# Fetch cohort counts:
.fetchCohortCounts <-function(connection,connectionDetails2){
  sql <- "SELECT cohort_definition_id, COUNT(*) AS count FROM @cohort_database_schema.@cohort_table GROUP BY cohort_definition_id"
  counts <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                       sql,
                                                       cohort_database_schema = connectionDetails2$cohortDatabaseSchema,
                                                       cohort_table = connectionDetails2$cohortTable,
                                                       snakeCaseToCamelCase = TRUE)
  return(counts)
}




#' helper object with more connection details
#' @param cdmDatabaseSchema schema
#' @param resultsDatabaseSchema result schema
#' @param oracleTempSchema oracle specific
#' @param cdmVersion version
#' @param cohortTable cohort table name
#' @param workFolder where to work

#' @export
.createConnectionDetails2<-function (cdmDatabaseSchema
                                     ,resultsDatabaseSchema=NULL  #rename later to resultDatabaseSchema (Achiles uses results but other packages use just result :-( )
                                     ,oracleTempSchema=NULL
                                     ,cdmVersion="5"
                                     ,cohortDatabaseSchema
                                     ,cohortTable='cohort'
                                     ,workFolder='c:/temp'
                                     ,outputFolder='c:/temp')#resolve with workFolder later
  {

  result <- list()
  for (name in names(formals(.createConnectionDetails2))) {
    result[[name]] <- get(name)
  }
  values <- lapply(as.list(match.call())[-1], function(x) eval(x,
                                                               envir = sys.frame(-3)))
  for (name in names(values)) {
    if (name %in% names(result))
      result[[name]] <- values[[name]]
  }
  class(result) <- "connectionDetails2"
  return(result)
}








#' Run AdditionalAnalysis package
#'
#' @details
#' Run the AdditionalAnalysis package, which implements additional analysis.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder where the results were generated; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#'
#' @export
.runCohortCharacterization <- function(connectionDetails,
                                      cdmDatabaseSchema,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      oracleTempSchema,
                                      cohortId,
                                      outputFolder
                                      #,cohortsToCreate
                                      #cohortCounts,
                                      #minCellCount
                                      ){

  #index <- grep(cohortId, cohortCounts$cohortDefinitionId)
  # index=30
  # if (length(index)==0) {
  #
  #   #ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " becasue of no count."))
  #   #    stop(paste0("ERROR: Trying to characterize a cohort that was not created! CohortID --> ", cohortsToCreate$cohortId[i], " Cohort Name --> ", cohortsToCreate$name[i]))
  #
  # } else if (cohortCounts$personCount[index] < minCellCount) {
  #
  #   #ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " low cell count."))
  #
  # } else {



    covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
    covariateSettings$DemographicsAge <- TRUE # Need to Age (Median, IQR)
    covariateSettings$DemographicsPostObservationTime <- TRUE # Need to calculate Person-Year Observation post index date (Median, IQR)

    covariateData2 <- FeatureExtraction::getDbCovariateData(connectionDetails = connectionDetails,
                                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                                            cohortTable = cohortTable,
                                                            cohortId = cohortId,
                                                            covariateSettings = covariateSettings,
                                                            aggregated = TRUE)
    summary(covariateData2)
    result <- FeatureExtraction::createTable1(covariateData2, specifications = .getCustomizeTable1Specs(), output = "one column"  )
    #  FeatureExtraction::saveCovariateData(covariateData2, file.path(outputFolder,paste0(cohortId,"_covariates")))
    print(result, row.names = FALSE, right = FALSE)
    #TODO check if this exist, harmonize with other functions
    analysisFolder <- file.path(outputFolder,'export')#, additionalAnalysisFolder)
    if (!file.exists(analysisFolder)) {
      dir.create(analysisFolder, recursive = TRUE)
    }
    write.csv(result, file.path(analysisFolder, paste0(cohortId,"_table1.csv")), row.names = FALSE)


    #}#end else
}





#' better table 1
#' @export
.getCustomizeTable1Specs <- function() {
  s <- FeatureExtraction::getDefaultTable1Specifications()
  appendedTable1Spec <- rbind(s, c("Age", 2,"")) # Add Age as a continuous variable to table1
  appendedTable1Spec <- rbind(appendedTable1Spec, c("PriorObservationTime", 8,"")) # Add Observation prior index date
  appendedTable1Spec <- rbind(appendedTable1Spec, c("PostObservationTime", 9,"")) # Add Observation post index date
  return(appendedTable1Spec)
}



# all functions from karthik - you can  delete it later
#'
#'
#' additionalAnalysisFolder <- "additional_analysis"
#'
#' #' Run AdditionalAnalysis package
#' #'
#' #' @details
#' #' Run the AdditionalAnalysis package, which implements additional analysis for the IUD Study.
#' #'
#' #' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#' #'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#' #'                             DatabaseConnector package.
#' #' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#' #'                             Note that for SQL Server, this should include both the database and
#' #'                             schema name, for example 'cdm_data.dbo'.
#' #' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#' #'                             write priviliges in this schema. Note that for SQL Server, this should
#' #'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' #' @param cohortTable          The name of the table that will be created in the work database schema.
#' #'                             This table will hold the exposure and outcome cohorts used in this
#' #'                             study.
#' #' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#' #'                             priviliges for storing temporary tables.
#' #' @param outputFolder         Name of local folder where the results were generated; make sure to use forward slashes
#' #'                             (/). Do not use a folder on a network drive since this greatly impacts
#' #'                             performance.
#' #'
#' #' @export
#' runCohortCharacterization <- function(connectionDetails,
#'                                       cdmDatabaseSchema,
#'                                       cohortDatabaseSchema,
#'                                       cohortTable,
#'                                       oracleTempSchema,
#'                                       cohortId,
#'                                       outputFolder,
#'                                       cohortsToCreate,
#'                                       cohortCounts,
#'                                       minCellCount) {
#'
#'   index <- grep(cohortId, cohortCounts$cohortDefinitionId)
#'   if (length(index)==0) {
#'
#'     ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " becasue of no count."))
#'     #    stop(paste0("ERROR: Trying to characterize a cohort that was not created! CohortID --> ", cohortsToCreate$cohortId[i], " Cohort Name --> ", cohortsToCreate$name[i]))
#'
#'   } else if (cohortCounts$personCount[index] < minCellCount) {
#'
#'     ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " low cell count."))
#'
#'   } else {
#'
#'     covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
#'     covariateSettings$DemographicsAge <- TRUE # Need to Age (Median, IQR)
#'     covariateSettings$DemographicsPostObservationTime <- TRUE # Need to calculate Person-Year Observation post index date (Median, IQR)
#'
#'     covariateData2 <- FeatureExtraction::getDbCovariateData(connectionDetails = connectionDetails,
#'                                                             cdmDatabaseSchema = cdmDatabaseSchema,
#'                                                             cohortDatabaseSchema = cohortDatabaseSchema,
#'                                                             cohortTable = cohortTable,
#'                                                             cohortId = cohortId,
#'                                                             covariateSettings = covariateSettings,
#'                                                             aggregated = TRUE)
#'     summary(covariateData2)
#'     result <- FeatureExtraction::createTable1(covariateData2, specifications = getCustomizeTable1Specs(), output = "one column"  )
#'     #  FeatureExtraction::saveCovariateData(covariateData2, file.path(outputFolder,paste0(cohortId,"_covariates")))
#'     print(result, row.names = FALSE, right = FALSE)
#'     analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
#'     if (!file.exists(analysisFolder)) {
#'       dir.create(analysisFolder, recursive = TRUE)
#'     }
#'     write.csv(result, file.path(outputFolder, additionalAnalysisFolder, paste0(cohortId,"_table1.csv")), row.names = FALSE)
#'
#'   }
#' }
#'
#' # Moves all table1, cumulative incidence, filtered cohortCounts, and graphs from diagnostic folder to the export folder
#' copyAdditionalFilesToExportFolder <- function(outputFolder,
#'                                               cohortCounts,
#'                                               minCellCount) {
#'   #copy table1, cumlative incidence, and cohort counts per year files
#'   filesToCopy <- list.files(path=file.path(outputFolder, additionalAnalysisFolder), full.names = TRUE, pattern="_table1|cumlativeIncidence|per_year|Kaplan")
#'
#'   # copy the files to export folder
#'   exportFolder <- file.path(outputFolder, "export")
#'   if (!file.exists(exportFolder)) {
#'     dir.create(exportFolder, recursive = TRUE)
#'   }
#'   file.copy(filesToCopy, file.path(outputFolder, "export"))
#'
#'   #filter the cohort counts for counts greater than minCellCount
#'   for (row in 1:nrow(cohortCounts)) {
#'     pc <- cohortCounts[row, "personCount"]
#'
#'     if(pc < minCellCount) {
#'       print(paste("Cohort count is less than ", minCellCount ," --> ", pc))
#'       cohortCounts[row, "personCount"] <- paste0("<", minCellCount)
#'       cohortCounts[row, "cohortCount"] <- paste0("<", minCellCount)
#'     }
#'   }
#'   analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
#'   if (!file.exists(analysisFolder)) {
#'     #dir.create(analysisFolder, recursive = TRUE)
#'     ParallelLogger::logInfo("Cannot copy files b/c additional analysese were not done...")
#'   }
#'   write.csv(cohortCounts, file.path(exportFolder, "filtered_cohort_counts.csv"), row.names = FALSE)
#'
#'   #copy the graphs from the diagnostic folder
#'   filesToCopy <- list.files(path=file.path(outputFolder, "diagnostics"), full.names = TRUE, pattern=".png")
#'   file.copy(filesToCopy, file.path(outputFolder, "export"))
#'
#' }
#'
#' createKMGraphs <- function(outputFolder, cohortsToCreate) {
#'   x <- list.files(path=paste0(outputFolder,"/cmOutput"), pattern="StratPop", full.names = TRUE)
#'   cohortsToCreate$stringsAsFactors=FALSE
#'   for (i in 1:length(x)) {
#'     studyPop <- readRDS(x[i])
#'     r <- extractParametersFromName(x[i])
#'     CohortMethod::plotKaplanMeier(studyPop,
#'                                   targetLabel= gettext(cohortsToCreate$atlasName[ which(cohortsToCreate$cohortId == r$target)]),
#'                                   comparatorLabel = gettext(cohortsToCreate$atlasName[ which(cohortsToCreate$cohortId == r$comparator)]),
#'                                   title = r$title,
#'                                   fileName = file.path(outputFolder,additionalAnalysisFolder,paste0("Kaplan Meier Plot ",r$title,".png")))
#'   }
#' }
#'
#' extractParametersFromName <- function(fileName) {
#'   result <- list("target" = "", "outcome" = "", "comparator" = "", title="No Title")
#'   f <- unlist(strsplit(fileName, "_"))
#'   cnt <- 0 #count to determine title
#'   for (i in 1:length(f)) {
#'     if (startsWith(f[i],"c1")) {
#'       result$comparator <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
#'     } else if (startsWith(f[i],"t1")) {
#'       result$target <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
#'     } else if (startsWith(f[i],"o1")) {
#'       result$outcome <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
#'     } else if (f[i]=='s1') {
#'       cnt <- cnt+1
#'     } else if (f[i]=='s2') {
#'       cnt <- cnt+2
#'     } else if (f[i]=='s3') {
#'       cnt <- cnt+4 # crude analysis
#'     }
#'   }
#'
#'   if (cnt == 2) {
#'     result$title <- "Subgroup Analysis"
#'   } else if (cnt == 3) {
#'     result$title <- "Matched Analysis"
#'   } else if (cnt == 4) {
#'     result$title <- "Stratification Analysis"
#'   } else if (cnt == 5) {
#'     result$title <- "Crude Analysis"
#'   }
#'
#'   return(result)
#' }
#' getCustomizeTable1Specs <- function() {
#'   s <- FeatureExtraction::getDefaultTable1Specifications()
#'   appendedTable1Spec <- rbind(s, c("Age", 2,"")) # Add Age as a continuous variable to table1
#'   appendedTable1Spec <- rbind(appendedTable1Spec, c("PriorObservationTime", 8,"")) # Add Observation prior index date
#'   appendedTable1Spec <- rbind(appendedTable1Spec, c("PostObservationTime", 9,"")) # Add Observation post index date
#'   return(appendedTable1Spec)
#' }
#'
#'
#'
#' calculateCumulativeIncidence <- function(connectionDetails,
#'                                          cohortDatabaseSchema,
#'                                          cdmDatabaseSchema,
#'                                          cohortTable,
#'                                          oracleTempSchema,
#'                                          targetCohortId,
#'                                          outcomeCohortId,
#'                                          outputFolder) {
#'
#'   conn <- DatabaseConnector::connect(connectionDetails)
#'   sql <- SqlRender::loadRenderTranslateSql("CumulativeIncidence.sql",
#'                                            "IUDCLW",
#'                                            dbms = connectionDetails$dbms,
#'                                            target_database_schema = cohortDatabaseSchema,
#'                                            cdm_database_schema = cdmDatabaseSchema,
#'                                            study_cohort_table = cohortTable,
#'                                            outcome_cohort = outcomeCohortId,
#'                                            target_cohort = targetCohortId,
#'                                            oracleTempSchema = oracleTempSchema)
#'   cumlativeIncidence <- DatabaseConnector::querySql(conn, sql)
#'   analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
#'   if (!file.exists(analysisFolder)) {
#'     dir.create(analysisFolder, recursive = TRUE)
#'   }
#'   output <- file.path(outputFolder, additionalAnalysisFolder, paste0(targetCohortId, "_", outcomeCohortId,"_cumlativeIncidence.csv"))
#'   write.table(cumlativeIncidence, file=output, sep = ",", row.names=FALSE, col.names = TRUE, append=FALSE)
#' }
#'
#' #Retrieves and writes yearly inclusion counts for all cohorts
#' calculatePerYearCohortInclusion <- function(connectionDetails,
#'                                             cohortDatabaseSchema,
#'                                             cohortTable,
#'                                             oracleTempSchema,
#'                                             outputFolder,
#'                                             minCellCount) {
#'
#'   sql <- SqlRender::loadRenderTranslateSql("GetCountsPerYear.sql",
#'                                            "IUDCLW",
#'                                            dbms = connectionDetails$dbms,
#'                                            target_database_schema = cohortDatabaseSchema,
#'                                            study_cohort_table = cohortTable,
#'                                            oracleTempSchema = oracleTempSchema)
#'   conn <- DatabaseConnector::connect(connectionDetails)
#'   counts <- DatabaseConnector::querySql(conn, sql)
#'   filtered_counts <- counts[counts["PERSON_COUNT"]>minCellCount,]
#'
#'   analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
#'   if (!file.exists(analysisFolder)) {
#'     dir.create(analysisFolder, recursive = TRUE)
#'   }
#'   output <- file.path(outputFolder, additionalAnalysisFolder, "cohort_counts_per_year.csv")
#'   write.table(filtered_counts, file=output, sep = ",", row.names=FALSE, col.names = TRUE)
#'
#' }
