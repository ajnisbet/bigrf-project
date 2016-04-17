# Tidy up R environment
rm(list=ls())
cat('\014')


# Load libraries
library(doParallel)
library(randomForest)  # Requited for na handling
library(data.table)

registerDoParallel(cores=detectCores(all.tests=TRUE))
# registerDoParallel(cores=1)
library(bigrf)

# Clear temp files, crashes otherwise
tmp.dir.forest <- '/tmp/Rforest'
tmp.dir.predict <- '/tmp/Rpredict'
unlink(tmp.dir.forest, recursive = TRUE)
unlink(tmp.dir.predict, recursive = TRUE)


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

# Split into x and y
df.train.y <- df.train$IsArrDelay
df.validation.y <- df.validation$IsArrDelay
df.train$IsArrDelay <- NULL
df.validation$IsArrDelay <- NULL


# Do RF
t <- system.time({
  forest <- bigrfc(df.train, df.train.y, cachepath=tmp.dir.forest, trace=1)
})

# Validate
validation.predictions <- predict(forest, df.validation, cachepath=tmp.dir.predict)
error.rate <- sum(validation.predictions != as.numeric(df.validation.y)) / nrow(df.validation)

print(c('Validation error', error.rate))
print(c('Elapsed time', t[['elapsed']]))

rm(df.train)
rm(df.train.y)
rm(forest)
gc()
