# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of SkeletonComparativeEffectStudy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Format and check code ---------------------------------------------------
OhdsiRTools::formatRFolder()
#OhdsiRTools::checkUsagePackage("SkeletonComparativeEffectStudy")
OhdsiRTools::updateCopyrightYearFolder()

# Create manual -----------------------------------------------------------
shell("rm extras/SkeletonComparativeEffectStudy.pdf")
shell("R CMD Rd2pdf ./ --output=extras/SkeletonComparativeEffectStudy.pdf")

# Create vignettes ---------------------------------------------------------
rmarkdown::render("vignettes/UsingSkeletonPackage.Rmd",
                  output_file = "../inst/doc/UsingSkeletonPackage.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

rmarkdown::render("vignettes/DataModel.Rmd",
                  output_file = "../inst/doc/DataModel.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

# Insert cohort definitions from ATLAS into package -----------------------

#create cohorts on Atlas
#create a file with cohort IDS and write it to inst/settings/cohortsToCreate.csv
#enf had to be set, but works with Martijn code

#declare packages that you will utilize
library(devtools)
use_package('DatabaseConnector')
use_package('SqlRender')
use_package('SqlRender')
use_package('ParallelLogger')
use_package('RJSONIO')
use_package('httr')
use_package('stringr')
use_package('OhdsiRTools')
use_package('FeatureExtraction')

#fetch sql code for cohorts

Sys.setenv(baseUrl='http://18.213.176.21:80/WebAPI')
OhdsiRTools::insertCohortDefinitionSetInPackage(fileName = "CohortsToCreate.csv",
                                                baseUrl = Sys.getenv("baseUrl"),
                                                insertTableSql = TRUE,
                                                insertCohortCreationR = TRUE,
                                                generateStats = FALSE,
                                                packageName = "SkeletonDescriptiveStudy")



# Create analysis details -------------------------------------------------
# source('extras/CreateStudyAnalysisDetails.R') createAnalysesDetails('inst/settings/')
# createPositiveControlSynthesisArgs('inst/settings/')

# Store environment in which the study was executed ----------------------- library(devtools)
# options(devtools.install.args = '--no-multiarch') install_github('OHDSI/OhdsiRTools')
# install_github('OHDSI/OhdsiSharing')

# OhdsiRTools::insertEnvironmentSnapshotInPackage('SkeletonDescriptiveStudy')



#generate RMarkdown as appendix
#will be replaced by better code once OHDSI API has better export feature
cohortsToCreate <- read.csv(file.path("inst/settings", 'CohortsToCreate.csv'))
urlPrefix='http://atlas-demo.ohdsi.org/#/cohortdefinition/'
tempf<-tempfile(pattern = 'temp', fileext = '.Rmd')
sink(tempf)
cat('# Cohorts  \n  \n')
for (i in 1:nrow(cohortsToCreate)) {
  cat(paste0("#### COHORT: ",as.character(cohortsToCreate$name[i]),'   \n\n'))
  cat(paste0(urlPrefix,cohortsToCreate$atlasId[i],'  \n  \n'))
}
sink()

rmarkdown::render(tempf,output_file = 'c:/temp/Appendix_Cohorts.htm'
                  ,rmarkdown::html_document(toc = F, fig_caption = TRUE)
                  )


#writeLines('---\ntitle: "Rules"\n---\n
#           ```{r, echo=FALSE}\n rules<-read.csv(system.file("csv","achilles_rule.csv",package="Achilles"),as.is=T);knitr::kable(rules)\n
#            ```',tempf)
# writeLines('# Results',tempf)

# writeLines('---\ntitle: "Rules"\n---\n```{r, echo=FALSE}\n rules<-read.csv(system.file("csv","achilles_rule.csv",package="Achilles"),as.is=T);knitr::kable(rules)\n```',tempf)
# rmarkdown::render(tempf,output_file = 'c:/temp/Heel-Rules.html',rmarkdown::html_document(toc = F, fig_caption = TRUE))

