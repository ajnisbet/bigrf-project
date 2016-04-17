# Tidy up R environment
rm(list=ls())
cat('\014')

# Load libraries
library(randomForest)
library(parallel)
library(pryr)
library(data.table)

# Import data
classes <- c(
  'Month' = 'factor',
  'DayOfWeek' = 'factor',
  'UniqueCarrier' = 'factor',
  'Dest' = 'factor',
  'IsDepDelay' = 'factor',
  'IsArrDelay' = 'factor',
  'DepHourLocal' = 'factor',
  'OriginSkyCoverage' = 'factor',
  'DestSkyCoverage' = 'factor'
)

# Load data
df.train <- fread('../data/train-10000.csv', header=TRUE, colClasses=classes, data.table=FALSE)
df.validation <- fread('../data/validation-10000.csv', header=TRUE, colClasses=classes, data.table=FALSE)

# Remove large category column
columns.to.drop <- c('Dest')
df.train <- df.train[, !(names(df.train) %in% columns.to.drop)]
df.validation <- df.validation[, !(names(df.validation) %in% columns.to.drop)]

# Fill missing values (there aren't many)
df.train <- na.roughfix(df.train)
df.validation <- na.roughfix(df.validation)

# Fix levels
for(attr in colnames(df.train)) {
  if (is.factor(df.train[[attr]])) {
    new.levels <- setdiff(levels(df.train[[attr]]), levels(df.validation[[attr]]))
    if ( length(new.levels) != 0 ) {
      levels(df.validation[[attr]]) <- union(levels(df.validation[[attr]]), levels(df.train[[attr]]))
    }
    new.levels <- setdiff(levels(df.validation[[attr]]), levels(df.train[[attr]]))
    if ( length(new.levels) != 0 ) {
      levels(df.train[[attr]]) <- union(levels(df.train[[attr]]), levels(df.validation[[attr]]))
    }
  }
}


# Do RF
t <- system.time({
  forest <- randomForest(IsArrDelay ~ ., data=df.train)
})

# Validate
validation.predictions <- predict(forest, newdata=df.validation)
error.rate <- sum(validation.predictions != df.validation$IsArrDelay) / nrow(df.validation)
base.error.rate <- sum(df.validation$IsArrDelay == 'True') / nrow(df.validation)

# Show results
print(c('Validation error', error.rate))
print(c('Elapsed time', t[['elapsed']]))
print(c('Base rate', base.error.rate))

# Tidy up so RStudio is still usable after run
rm(forest)
rm(df.train)
rm(df.validation)
gc()
