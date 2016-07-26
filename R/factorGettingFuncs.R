#' get factor functions
#' @name getfactor
#' @param TS a \bold{TS} object
#' @return a \bold{TSF} object
NULL



# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# =============   get factors through local data base    ===============
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
#' @rdname getfactor
#' @export
gf.mkt_cap <- function(TS){
  re <- TS.getTech(TS,variables="mkt_cap")
  re <- renameCol(re,"mkt_cap","factorscore")
  return(re)
}
#' @rdname getfactor
#' @export
gf.float_cap <- function(TS){
  re <- TS.getTech(TS,variables="float_cap")
  re <- renameCol(re,"float_cap","factorscore")
  return(re)
}

#' @rdname getfactor
#' @export
#' @param is1q logic. if TRUE(the default), return the single quarter data, else a cummuliated data.
gf.NP_YOY <- function(TS,is1q=TRUE,filt=10000000){
  check.TS(TS)  
  PeriodMark <- ifelse(is1q,2,3)    
  TS$date <- rdate2int(TS$date)
  qr <- paste(
    "select b.date, b.stockID, InfoPublDate,EndDate,NP_YOY as 'factorscore', src, NP_LYCP 
    from LC_PerformanceGrowth a, yrf_tmp b
    where a.id=(
    select id from LC_PerformanceGrowth
    where stockID=b.stockID and InfoPublDate<=b.date 
    and PeriodMark=",PeriodMark,"
    order by InfoPublDate desc, EndDate DESC
    limit 1);
    "      
  )  
  con <- db.local()
  dbWriteTable(con,name="yrf_tmp",value=TS[,c("date","stockID")],row.names = FALSE,overwrite = TRUE)
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)  
  re <- merge.x(TS,re,by=c("date","stockID"))  
  re <- transform(re, date=intdate2r(date))   
  
  # -- filtering
  re[!is.na(re$NP_LYCP) & abs(re$NP_LYCP)<filt, "factorscore"] <- NA  
  return(re)
}





# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# =============   get technical factors through tinysoft ===============
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============

# -- by TS.getTech_ts
# gf.totalshares <- function(TS){  
#   TS.getTech_ts(TS,"StockTotalShares3")
# }

# gf.pct_chg_per <- function(TS,N){
#   TS.getTech_ts(TS,"StockZf2",N)
# }


# -- by TS.getTech_ts
#' @rdname getfactor
#' @export
gf.pct_chg_per <- function(TS,N){
  funchar <- paste("StockZf2(",N,")",sep="")
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.totalshares <- function(TS){ 
  funchar <- "StockTotalShares3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.totalmarketvalue <- function(TS){
  funchar <- "StockTotalValue3()"
  TS.getTech_ts(TS,funchar)
}

gf.BBIBOLL <- function(TS,p1,p2){
  funchar <- paste('BBIBOLL_v(',p1,',',p2,')',sep='')
  TS.getTech_ts(TS,funchar)
}

gf.avgTurnover <- function(TS,N){
  funchar <- paste('StockAveHsl2(',N,')',sep='')
  TS.getTech_ts(TS,funchar)
}

gf.avgTurnover_1M3M <- function(TS){
  funchar <- "StockAveHsl2(20)/StockAveHsl2(60)"
  TS.getTech_ts(TS,funchar)
}

gf.sumAmount <- function(TS,N){
  funchar <- paste('StockAmountSum2(',N,')',sep='')
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.floatMarketValue <- function(TS){
  funchar <- "StockMarketValue3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PE_lyr <- function(TS){
  funchar <- "StockPE3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PE_ttm <- function(TS){
  funchar <- "StockPE3_V()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PS_lyr <- function(TS){
  funchar <- "StockPMI3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PS_ttm <- function(TS){
  funchar <- "StockPMI3_V()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PCF_lyr <- function(TS){
  funchar <- "StockPCF3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PCF_ttm <- function(TS){
  funchar <- "StockPCF3_V()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PB_lyr <- function(TS){
  funchar <- "StockPNA3()"
  TS.getTech_ts(TS,funchar)
}
#' @rdname getfactor
#' @export
gf.PB_mrq <- function(TS){
  funchar <- "StockPNA3_II()"
  TS.getTech_ts(TS,funchar)
}


gf.temp111 <- function (TS,p1,p2) {
  funchar <- paste('BBIBOLL_v(',p1,',',p2,')',sep='')
  TSF <- TS.getTech_ts(TS,funchar)
}

# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# ===================== get financial factors through tinysoft =========
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============

# -- via TS.getFin_ts

gf.tempxxxyyy <- function(TS){
  funchar <- "ReportOfAll(9900416,Rdate)"
  TS.getFin_ts(TS,funchar)
}
gf.tempxxxyyyzz <- function(TS){
  funchar <- "StockAveHsl2(20)+reportofall(9900003,Rdate)"
  TS.getFin_ts(TS,funchar)
}

gf.G_NP_Q_xx <- function(TS){  
  funchar <- "LastQuarterData(Rdate,9900604,0)"
  re <- TS.getFin_ts(TS,funchar)  
  return(re)
}





# -- via TS.getFin_rptTS

# -- GrossProfitMargin(MLL)
#' @rdname getfactor
#' @export
gf.G_MLL_Q <- function(TS){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900103,0)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}



# -- OCF
#' @rdname getfactor
#' @export
gf.G_OCF_Q <- function(TS, filt=0){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,48018,1),
  "OCF_t0",RefReportValue(@LastQuarterData(DefaultRepID(),48018,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$OCF_t0) & abs(re$OCF_t0)<filt, "factorscore"] <- NA    
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_OCF <- function(TS, filt=0){
  funchar <-  '"factorscore",Last12MTBGrowRatio(Rdate,48018,1),
  "OCF_t0",RefReportValue(@Last12MData(DefaultRepID(),48018),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$OCF_t0) & abs(re$OCF_t0)<filt, "factorscore"] <- NA    
  return(re)
}


# -- SCF
#' @rdname getfactor
#' @export
gf.G_SCF_Q <- function(TS, filt=0){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,48002,1),
  "SCF_t0",RefReportValue(@LastQuarterData(DefaultRepID(),48002,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$SCF_t0) & abs(re$SCF_t0)<filt, "factorscore"] <- NA    
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_SCF <- function(TS, filt=0){
  funchar <-  '"factorscore",last12MTBGrowRatio(Rdate,48002,1),
  "SCF_t0",RefReportValue(@Last12MData(DefaultRepID(),48002),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$SCF_t0) & abs(re$SCF_t0)<filt, "factorscore"] <- NA    
  return(re)
}


# -- OCF2OR
#' @rdname getfactor
#' @export
# gf.G_OCF2OR_Q <- function(TS){
#   funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900701,0)'
#   re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
#   return(re)
# }



# -- SCF2OR
#' @rdname getfactor
#' @export
# gf.G_SCF2OR_Q <- function(TS){
#   funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900700,0)'
#   re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
#   return(re)
# }


# -- ROE
#' @rdname getfactor
#' @export
gf.G_ROE_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900100,0),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA   
  return(re)
}


# -- EPS
#' @rdname getfactor
#' @export
gf.G_EPS_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900000,1),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA   
  return(re)
}



# -- scissor
#' @rdname getfactor
#' @export
gf.G_scissor_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",LastQuarterData(Rdate,9900604,0)-LastQuarterData(Rdate,9900600,0),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA    
  return(re)
}



# --- NP --
#' @rdname getfactor
#' @export
gf.G_NP_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",LastQuarterData(Rdate,9900604,0),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA    
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_NPcut_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,42017,1),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),42017,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA    
  return(re)
}
#' @rdname getfactor
#' @export
gf.GG_NP_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900604,0),
  "NP_t1",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1),
  "NP_t2",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,2)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[(!is.na(re$NP_t1) & abs(re$NP_t1)<filt) | (!is.na(re$NP_t2) & abs(re$NP_t2)<filt), "factorscore"] <- NA 
  return(re)
}


# --- OR --
#' @rdname getfactor
#' @export
gf.G_OR_Q <- function(TS, filt=100000000){
  funchar <-  '"factorscore",LastQuarterData(Rdate,9900600,0),
  "OR_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46002,0),Rdate,1)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[!is.na(re$OR_t0) & abs(re$OR_t0)<filt, "factorscore"] <- NA    
  return(re)
}

#' @rdname getfactor
#' @export
gf.GG_OR_Q <- function(TS, filt=100000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900600,0),
  "OR_t1",RefReportValue(@LastQuarterData(DefaultRepID(),46002,0),Rdate,1),
  "OR_t2",RefReportValue(@LastQuarterData(DefaultRepID(),46002,0),Rdate,2)'
  re <- TS.getFin_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering  
  re[(!is.na(re$OR_t1) & abs(re$OR_t1)<filt) | (!is.na(re$OR_t2) & abs(re$OR_t2)<filt), "factorscore"] <- NA   
  return(re)
}






# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# ===================== get factors through database ===================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============

#' TS.getFactor.db
#'
#' get factors throug a SQL qurey from a database
#' @param TS a \bold{TS} object
#' @param subfun a function object,which get the factors of a subset of TS from the database.
#' @param ... other parametres of subfun
#' @note Note that the subfun must contain subset of TS as the first argument. and the subset must be a subset of TS on a certain "date".
#' @return A \bold{TSF} object
#' @author Ruifei.Yin
#' @examples 
#' TS <- getTS(getRebDates(as.Date('2011-03-17'),as.Date('2012-04-17')),'EI000300')
#' subfun <- function(subTS,con_type){ # con_type indicate the consensus type
#'   dt <- subTS[1,"date"]
#'   qr_char <- paste("SELECT stock_code,con_date,con_type,c80 
#'       FROM CON_FORECAST_STK a
#'         where a.con_date=",QT(dt),"and year(a.con_date)=a.rpt_date and a.stock_type=1 and con_type=",con_type,"and a.rpt_type=4")    
#'   tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)
#'   subTS$stock_code <- stockID2tradeCode(subTS$stockID)
#'   re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
#'   re <- re[,c(names(TS),"c80")]
#'   re <- renameCol(re,"c80","factorscore")
#'   return(re)
#' }
#'  TSF <- TS.getFactor.db(TS,subfun,con_type=1)
TS.getFactor.db <- function(TS, subfun, ...){ 
  check.TS(TS) 
  cat("Function TS.getFactor.db Working...\n")
  re <- ddply(TS, "date", .progress = "text", subfun,...)
  return(re)  
}


# ----- some examples -------------
#' @rdname getfactor
#' @export
gf.F_NP_chg <- function(TS,span,con_type="1"){
  # span: "w1","w4","w13","w26","w52"
  # con_type: one or more of 1,2,3,4
  subfun <- function(subTS,span,con_type){
    dt <- subTS[1,"date"]
    var <- switch(span,
                  w1="c80",
                  w4="c81",
                  w13="c82",
                  w26="c83",
                  w52="c84")
    qr_char <- paste("SELECT stock_code,con_date,con_type,",var,"
      FROM CON_FORECAST_STK a
        where a.con_date=",QT(dt),"and year(a.con_date)=a.rpt_date and a.stock_type=1 and con_type in (",con_type,")and a.rpt_type=4")    
  tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
    re <- re[,c(names(TS),var)]
    re <- renameCol(re,var,"factorscore")
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,span=span,con_type=con_type)
  return(re)
}



#' @rdname getfactor
#' @export
gf.F_PE <- function(TS,con_type="1"){
  # con_type: one or more of 1,2,3,4
  subfun <- function(subTS,span,con_type){
    dt <- subTS[1,"date"]
    qr_char <- paste("SELECT stock_code,con_date,con_type,C5 as factorscore
      FROM CON_FORECAST_STK a
        where a.con_date=",QT(dt),"and year(a.con_date)=a.rpt_date and a.stock_type=1 and con_type in (",con_type,")and a.rpt_type=4")    
    tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,span=span,con_type=con_type)
  return(re)
}


#' @rdname getfactor
#' @export
gf.F_ROE <- function(TS,con_type="1"){
  # con_type: one or more of 1,2,3,4
  subfun <- function(subTS,span,con_type){
    dt <- subTS[1,"date"]
    qr_char <- paste("SELECT stock_code,con_date,con_type,C12 as factorscore
      FROM CON_FORECAST_STK a
        where a.con_date=",QT(dt),"and year(a.con_date)=a.rpt_date and a.stock_type=1 and con_type in (",con_type,")and a.rpt_type=4")    
    tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,span=span,con_type=con_type)
  return(re)
}



#' @rdname getfactor
#' @export
gf.F_rank_chg <- function(TS,lag,con_type="1"){
  # lag: integer,ginving the number of lag tradingdays
  # con_type: one or more of 1,2,3,4
  subfun <- function(subTS,lag,con_type){
    dt <- subTS[1,"date"]
    dt_lag <- trday.nearby(dt,by=lag)
    qr_char <- paste(
      "SELECT a.stock_code,a.con_date,a.score,a.score_type,b.score as score_ref,b.score_type as score_type_ref,
              a.score/(case when b.score=0 then NULL else b.score end)-1 as factorscore
        FROM CON_FORECAST_SCHEDULE a, CON_FORECAST_SCHEDULE b
        where a.con_date=",QT(dt),"and a.score_type in (",con_type,") 
                     and b.con_date=",QT(dt_lag),"and b.stock_code=a.stock_code"
    )   
    tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)    
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
    #     re <- re[,c(names(TS),"factorscore")]
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,lag=lag,con_type=con_type)
  return(re)
}


#' @rdname getfactor
#' @export
gf.F_target_rtn <- function(TS,con_type="1"){
  # con_type: one or more of 1,2,3,4
  subfun <- function(subTS,con_type){
    dt <- subTS[1,"date"]
    dt_int <- as.integer(as.character(dt,format="%Y%m%d"))
    qr_char <- paste(
      "SELECT a.stock_code,a.con_date,a.target_price,a.target_price_type,b.TCLOSE,
              a.target_price/(case when b.TCLOSE=0 then NULL else b.TCLOSE end)-1 as factorscore
        FROM CON_FORECAST_SCHEDULE a, GG_CHDQUOTE b
        where a.con_date=",QT(dt),"and a.target_price_type in (",con_type,") 
                     and b.TDATE=",dt_int,"and b.SYMBOL=a.stock_code"
                      )   
    tmpdat <- read.db.odbc(db.cs(),qr_char,as.is=1)    
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
#     re <- re[,c(names(TS),"factorscore")]
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,con_type=con_type)
  return(re)
}