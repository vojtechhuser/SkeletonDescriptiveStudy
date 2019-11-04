
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

    return(result)
}


