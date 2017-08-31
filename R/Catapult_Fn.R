#' Imports the file from CSV and converts it to the correct format
#' @param A CSV file
#' @export

Catapult_CSV_ImportAndConvert <- function(sFileName){

  library(readr)
  library(data.table)
  library(lubridate)
  library(dplyr)
  library(RODBC)

  tryCatch({

    ctr <- data.table(read_csv(sFileName, skip = 9, col_types = cols(.default = "c")))

    #Fix the the HMS columns.
    a <- colnames(ctr) #Get Col Names
    l <- c("Total Duration", "Average Duration", "Bench Time", "Field Time") #Sets the righ Col Name Types
    b <- grepl(paste(l, collapse = "|"), a) #Create List of the correct columns, its boolean
    a <- a[b] #Select Correct Columns
    a <- a[grep("%", a, invert = TRUE)] # Remove Columns that have % Symbol
    #a #List of Field names that need to be formated.

    #For Each Value in the selected columns convert to MM
    for (j in a) set(ctr, i = NULL, j = j, value = hour(hms(ctr[[j]]))*60 + minute(hms(ctr[[j]])) + second(hms(ctr[[j]]))/60)
    rm(a,b,j,l)

    #Remove Duplicate Columns
    ctr <- ctr[,unique(names(ctr)),with = FALSE]

    #Remove Spaces From Col Names
    names(ctr) <- gsub(x = names(ctr), pattern = " ", replacement = "_")
    names(ctr) <- gsub(x = names(ctr), pattern = "%", replacement = "Pct")
    names(ctr) <- gsub(x = names(ctr), pattern = "#", replacement = "Num")
    names(ctr) <- gsub(x = names(ctr), pattern = "Velocity", replacement = "Vel")
    names(ctr) <- gsub(x = names(ctr), pattern = "Band", replacement = "Bnd")
    names(ctr) <- gsub(x = names(ctr), pattern = "Duration", replacement = "Dur")
    names(ctr) <- gsub(x = names(ctr), pattern = "Total", replacement = "Ttl")
    names(ctr) <- gsub(x = names(ctr), pattern = "Distance", replacement = "Dist")
    names(ctr) <- gsub(x = names(ctr), pattern = "Count", replacement = "Cnt")
    names(ctr) <- gsub(x = names(ctr), pattern = "Maximum", replacement = "Max")
    names(ctr) <- gsub(x = names(ctr), pattern = "Minimum", replacement = "Min")
    names(ctr) <- gsub(x = names(ctr), pattern = "Efforts", replacement = "Eff")
    names(ctr) <- gsub(x = names(ctr), pattern = "Effort", replacement = "Eff")
    names(ctr) <- gsub(x = names(ctr), pattern = "Acceleration", replacement = "Acc")
    names(ctr) <- gsub(x = names(ctr), pattern = "Heart_Rate", replacement = "HR")
    names(ctr) <- gsub(x = names(ctr), pattern = "Player_Load", replacement = "PL")
    names(ctr) <- gsub(x = names(ctr), pattern = "Metabolic_Power", replacement = "MP")
    names(ctr) <- gsub(x = names(ctr), pattern = "Average", replacement = "Avg")
    names(ctr) <- gsub(x = names(ctr), pattern = "Session", replacement = "Sess")
    names(ctr) <- gsub(x = names(ctr), pattern = "Session", replacement = "Sess")
    names(ctr) <- gsub(x = names(ctr), pattern = "O'Clock", replacement = "OClock")
    names(ctr) <- gsub(x = names(ctr), pattern = "1.0", replacement = "1")
    names(ctr) <- gsub(x = names(ctr), pattern = "Medium", replacement = "Med")
    names(ctr) <- gsub(x = names(ctr), pattern = "Minute", replacement = "Min")
    names(ctr) <- gsub(x = names(ctr), pattern = "/", replacement = "")
    names(ctr) <- gsub(x = names(ctr), pattern = "_-_", replacement = "_")
    names(ctr) <- gsub(x = names(ctr), pattern = "Running", replacement = "Run")
    names(ctr) <- gsub(x = names(ctr), pattern = "Recovery", replacement = "Rec")
    names(ctr) <- gsub(x = names(ctr), pattern = "Meterage", replacement = "Meter")
    names(ctr) <- gsub(x = names(ctr), pattern = "[(]", replacement = "")
    names(ctr) <- gsub(x = names(ctr), pattern = ")", replacement = "")
    names(ctr) <- gsub(x = names(ctr), pattern = "[-]", replacement = "")
    names(ctr) <- gsub(x = names(ctr), pattern = "[']", replacement = "")



    #Create connection
    conn <- odbcDriverConnect('driver={SQL Server};server=ndperfsci.c3uooucvyesd.us-east-2.rds.amazonaws.com,1433;database=PerfSci;uid=NDPerfSci;pwd=GoUND2017!')



    #Check the number of variables to match the master list
    all_vars <- sqlColumns(conn, "ctplt")

    #Replace NameID and DateID with Athlete and Test_Date.
    remove <- c ("Name_FirstLast", "DateID")
    all_vars <- all_vars$COLUMN_NAME[! all_vars$COLUMN_NAME %in% remove]
    all_vars <- c(all_vars, "Player_Name", "Date")

    #Check whether the tmp file is missing any variables.
    #If so, create a vector containing the missing variables.
    #Loop through this vector and append the variables to the tbl, filling the contents with NA's.
    diff <- setdiff(all_vars, colnames(ctr))
    if(length(diff)!= 0)
    {
      for(i in 1:length(diff)){
        ctr[,diff[i]] <- NA
      }
    }

    delete_names <- setdiff(colnames(ctr), all_vars)

    for(i in 1:length(delete_names)){
      ctr[,delete_names[i]] <- NULL
    }



    #Convert Date
    ctr$Date <- mdy(ctr$Date)

    #Set Character Columns
    ctr$Day_Name <- as.character(ctr$Day_Name)
    ctr$Month_Name <- as.character(ctr$Month_Name)
    ctr$Period_Name <- as.character(ctr$Period_Name)
    ctr$Player_Name <- as.character(ctr$Player_Name)
    ctr$Position_Name <-  as.character(ctr$Position_Name)
    ctr$Team_Name <- as.character(ctr$Team_Name)
    ctr$Jersey <- as.character(ctr$Jersey)

    #Rearrange
    ctr <- ctr %>% select(Date, Month_Name, Period_Name,
                          Player_Name, Position_Name,
                          Team_Name, Jersey, Day_Name,
                          Start_Time, End_Time, everything())

    #Convert Remaining Colums to Numeric
    cols <- names(ctr)
    cols <- cols[11:length(cols)]
    ctr <- ctr[,(cols) := lapply(.SD,as.numeric),.SDcols = cols]



    #Replace $ with pct
    return(ctr)
  }, error = function(e){
    # cat("ERROR :",conditionMessage(e), "\n")
    # #return(conditionMessage(e))
    # return(e)
    return(0)
  })
}

#This Function Appends the converted csv from catapult_csv_ImportAndConvert
Catapult_DB_Append <- function(ctr, table, runProc) {

  ## SQL Connetion
  library(RODBC)

  tryCatch({

    #Open Connection
    conn <- odbc_conn()


    if ("ctplt_tmp" %in% sqlTables(conn)$TABLE_NAME) {
      sqlDrop(conn, "ctplt_tmp")
    }

    #Save to Database // Get Result from this for output
    varTypes <- c(Date = "date")
    sqlSave(conn, ctr, tablename = "ctplt_tmp", append = TRUE, rownames = FALSE, colnames = FALSE, verbose = FALSE, safer = TRUE, varTypes = varTypes, addPK = FALSE, fast = FALSE, test = FALSE, nastring = NULL)

    #Run Procedure to move files
    if (runProc) {
      # Proc State Returns 0 if successful. 1 if Append Fails and 2 if the table doesn't exsists.
      ProcState <- sqlQuery(conn, "DECLARE	@return_value int, @rowcnt int, @rowcnt_tmp int EXEC	@return_value = [dbo].[ctplt_append] @rowcnt = @rowcnt OUTPUT, @rowcnt_tmp = @rowcnt_tmp OUTPUT SELECT @rowcnt as RowCnt, @rowcnt_tmp as RowCnt_tmp,	@return_value as 'State', @@ERROR as 'Error';")
    } else if (!runProc) {
      ProcState$State <- -1
      ProcState$Error <- "RunProc = FALSE"
    }

    #Close all conncetions
    odbcClose(conn)

    return(ProcState)
  }, error = function(e){
    return(0)
  }
  )

}

#Query Maker Data Edit
ctplt_qry_str <- function(DateLow, DateHigh, PeriodName, TeamName, option){

  SelectTeam <- paste("SELECT ID, Date, Period_Name, Start_Time FROM ctplt.ctplt_prds WHERE Team_Name = '" ,TeamName,"'", sep = "")

  if (option == "All") {
    sql <- SelectTeam
  } else if (option == "Date") {
    sql <- paste(SelectTeam, " AND Date >= '", DateLow, "' AND Date <= '", DateHigh, "'",  sep = "")
  } else if (option == "Period_Name") {
    sql <- paste(SelectTeam, " AND Period_Name = '", PeriodName,"'", sep = "")
  } else
    sql <- SelectTeam


  return(sql)

  #sqlQuery(odbc_conn(), ctplt_qry_str('2016-01-01', '2016-02-01', "Session", "Notre Dame Mens Soccer", "Date"))

  #ctplt_qry_str('2016-01-01', '2016-02-01', "Session", "Notre Dame Mens Soccer", "Date")

}
