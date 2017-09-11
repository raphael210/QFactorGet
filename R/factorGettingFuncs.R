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
  re <- getTech(TS,variables="mkt_cap")
  re <- renameCol(re,"mkt_cap","factorscore")
  return(re)
}
#' @rdname getfactor
#' @export
gf.float_cap <- function(TS){
  re <- getTech(TS,variables="float_cap")
  re <- renameCol(re,"float_cap","factorscore")
  return(re)
}
#' @rdname getfactor
#' @export
gf.ln_mkt_cap <- function(TS){
  re <- gf.mkt_cap(TS)
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
  return(re)
}

#' @rdname getfactor
#' @export
gf.ln_float_cap <- function(TS){
  re <- gf.float_cap(TS)
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
  return(re)
}

#' @rdname getfactor
#' @export
#' @param is1q logic. if TRUE(the default), return the single quarter data, else a cummuliated data.
gf.NP_YOY <- function(TS,is1q=TRUE,filt=10000000,rm_neg=FALSE,src=c("all","fin")){
  src <- match.arg(src)
  if(src=="fin") {
    src_filt <- "src='fin'"
  } else {
    src_filt <- "1>0"
  }
  check.TS(TS)
  PeriodMark <- ifelse(is1q,2,3)
  TS$date <- rdate2int(TS$date)
  qr <- paste(
    "select b.date, b.stockID, InfoPublDate,EndDate,NP_YOY as 'factorscore', src, NP_LYCP
    from LC_PerformanceGrowth a, yrf_tmp b
    where a.id=(
    select id from LC_PerformanceGrowth
    where stockID=b.stockID and InfoPublDate<=b.date and",src_filt," 
    and PeriodMark=",PeriodMark,"
    order by InfoPublDate desc, EndDate DESC
    limit 1);
    "
  )
  con <- db.local()
  dbWriteTable(con,name="yrf_tmp",value=TS[,c("date","stockID")],row.names = FALSE,overwrite = TRUE)
  re <- DBI::dbGetQuery(con,qr)
  DBI::dbDisconnect(con)
  re <- merge.x(TS,re,by=c("date","stockID"))
  re <- transform(re, date=intdate2r(date))

  # -- filtering
  if(rm_neg){
    re[!is.na(re$NP_LYCP) & re$NP_LYCP<filt, "factorscore"] <- NA
  } else{
    re[!is.na(re$NP_LYCP) & abs(re$NP_LYCP)<filt, "factorscore"] <- NA
  }
  return(re)
}





# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# =============   get technical factors through tinysoft ===============
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============




# -- by getTech_ts
#' @rdname getfactor
#' @export
gf.pct_chg_per <- function(TS,N){
  funchar <- paste("StockZf2(",N,")",sep="")
  getTech_ts(TS,funchar,varname="factorscore")
}
#' @rdname getfactor
#' @export
gf.totalshares <- function(TS){
  funchar <- "StockTotalShares3()"
  getTech_ts(TS,funchar,varname="factorscore")
}
#' @rdname getfactor
#' @export
gf.totalmarketvalue <- function(TS){
  funchar <- "StockTotalValue3()"
  getTech_ts(TS,funchar,varname="factorscore")
}

gf.BBIBOLL <- function(TS,p1,p2){
  funchar <- paste('BBIBOLL_v(',p1,',',p2,')',sep='')
  getTech_ts(TS,funchar,varname="factorscore")
}

gf.avgTurnover <- function(TS,N){
  funchar <- paste('StockAveHsl2(',N,')',sep='')
  getTech_ts(TS,funchar,varname="factorscore")
}

gf.avgTurnover_1M3M <- function(TS){
  funchar <- "StockAveHsl2(20)/StockAveHsl2(60)"
  getTech_ts(TS,funchar,varname="factorscore")
}

gf.sumAmount <- function(TS,N){
  funchar <- paste('StockAmountSum2(',N,')',sep='')
  getTech_ts(TS,funchar,varname="factorscore")
}
#' @rdname getfactor
#' @export
gf.floatMarketValue <- function(TS){
  funchar <- "StockMarketValue3()"
  getTech_ts(TS,funchar,varname="factorscore")
}
#' @rdname getfactor
#' @export
gf.PE_lyr <- function(TS,fillna=TRUE){
  funchar <- "StockPE3()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PE_ttm <- function(TS,fillna=TRUE){
  funchar <- "StockPE3_V()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.ln_PE_ttm <- function(TS){
  re <- gf.PE_ttm(TS)
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
  return(re)
}


#' @rdname getfactor
#' @export
gf.PS_lyr <- function(TS,fillna=TRUE){
  funchar <- "StockPMI3()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PS_ttm <- function(TS,fillna=TRUE){
  funchar <- "StockPMI3_V()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PCF_lyr <- function(TS,fillna=TRUE){
  funchar <- "StockPCF3()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PCF_ttm <- function(TS,fillna=TRUE){
  funchar <- "StockPCF3_V()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PB_lyr <- function(TS,fillna=TRUE){
  funchar <- "StockPNA3()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.PB_mrq <- function(TS,fillna=TRUE){
  funchar <- "StockPNA3_II()"
  re <- TS.getTech_ts(TS,funchar,varname="factorscore")
  if(fillna){
    re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  }
  return(re)
}
#' @rdname getfactor
#' @export
gf.ln_PB_mrq <- function(TS){
  re <- gf.PB_mrq(TS)
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
  return(re)
}


gf.temp111 <- function (TS,p1,p2) {
  funchar <- paste('BBIBOLL_v(',p1,',',p2,')',sep='')
  TSF <- getTech_ts(TS,funchar,varname="factorscore")
}

# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# ===================== get financial factors through tinysoft =========
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============

# -- via getFin_ts -------

gf.tempxxxyyy <- function(TS){
  funchar <- "ReportOfAll(9900416,Rdate)"
  getFin_ts(TS,funchar,varname="factorscore")
}
gf.tempxxxyyyzz <- function(TS){
  funchar <- "StockAveHsl2(20)+reportofall(9900003,Rdate)"
  getFin_ts(TS,funchar,varname="factorscore")
}

gf.G_NP_Q_xx <- function(TS){
  funchar <- "LastQuarterData(Rdate,9900604,0)"
  re <- getFin_ts(TS,funchar,varname="factorscore")  
  return(re)
}





# -- via TS.getFin_by_rptTS -------





#' @export
gf.ROE <- function(TS){
  funchar <-  '"factorscore",reportofall(9900100,Rdate)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}
#' @export
gf.ROE_Q <- function(TS){
  funchar <-  '"factorscore",LastQuarterData(Rdate,9900100,0)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}
#' @export
gf.ROE_ttm <- function(TS){
  funchar <-  '"factorscore",Last12MData(Rdate,9900100)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}



# -- GrossProfitMargin(MLL)
#' @rdname getfactor
#' @export
gf.G_MLL_Q <- function(TS){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900103,0)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  return(re)
}



# -- OCF
#' @rdname getfactor
#' @export
gf.G_OCF_Q <- function(TS, filt=0){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,48018,1),
  "OCF_t0",RefReportValue(@LastQuarterData(DefaultRepID(),48018,0),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering
  re[!is.na(re$OCF_t0) & abs(re$OCF_t0)<filt, "factorscore"] <- NA
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_OCF <- function(TS, filt=0){
  funchar <-  '"factorscore",Last12MTBGrowRatio(Rdate,48018,1),
  "OCF_t0",RefReportValue(@Last12MData(DefaultRepID(),48018),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering
  re[!is.na(re$SCF_t0) & abs(re$SCF_t0)<filt, "factorscore"] <- NA
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_SCF <- function(TS, filt=0){
  funchar <-  '"factorscore",last12MTBGrowRatio(Rdate,48002,1),
  "SCF_t0",RefReportValue(@Last12MData(DefaultRepID(),48002),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering
  re[!is.na(re$SCF_t0) & abs(re$SCF_t0)<filt, "factorscore"] <- NA
  return(re)
}


# -- OCF2OR
#' @rdname getfactor
#' @export
# gf.G_OCF2OR_Q <- function(TS){
#   funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900701,0)'
#   re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
#   return(re)
# }



# -- SCF2OR
#' @rdname getfactor
#' @export
# gf.G_SCF2OR_Q <- function(TS){
#   funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900700,0)'
#   re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
#   return(re)
# }


# -- ROE
#' @rdname getfactor
#' @export
gf.G_ROE_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,9900100,0),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),46078,0),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_NP_ttm <- function(TS, filt=10000000){
  funchar <-  '"factorscore",Last12MData(Rdate,9900604),
  "NP_t0",RefReportValue(@Last12MData(DefaultRepID(),46078),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
  # -- filtering
  re[!is.na(re$NP_t0) & abs(re$NP_t0)<filt, "factorscore"] <- NA
  return(re)
}
#' @rdname getfactor
#' @export
gf.G_NPcut_Q <- function(TS, filt=10000000){
  funchar <-  '"factorscore",QuarterTBGrowRatio(Rdate,42017,1),
  "NP_t0",RefReportValue(@LastQuarterData(DefaultRepID(),42017,0),Rdate,1)'
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
  re <- TS.getFin_by_rptTS(TS,fun="rptTS.getFin_ts",funchar= funchar)
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
#'   tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
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
  re <- plyr::ddply(TS, "date", .progress = "text", subfun,...)
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
  tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
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
    tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,span=span,con_type=con_type)
  re$factorscore <- ifelse(re$factorscore<=0,NA,re$factorscore)
  return(re)
}
#' @rdname getfactor
#' @export
gf.ln_F_PE <- function(TS,con_type="1"){
  re <- gf.F_PE(TS,con_type = con_type)
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
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
    tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
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
    dt_lag <- trday.nearby(dt,by=-lag)
    qr_char <- paste(
      "SELECT a.stock_code,a.con_date,a.score,a.score_type,b.score as score_ref,b.score_type as score_type_ref,
              a.score/(case when b.score=0 then NULL else b.score end)-1 as factorscore
        FROM CON_FORECAST_SCHEDULE a, CON_FORECAST_SCHEDULE b
        where a.con_date=",QT(dt),"and a.score_type in (",con_type,")
                     and b.con_date=",QT(dt_lag),"and b.stock_code=a.stock_code"
    )
    tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
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
    tmpdat <- queryAndClose.odbc(db.cs(),qr_char,as.is=1)
    subTS$stock_code <- stockID2tradeCode(subTS$stockID)
    re <- merge(subTS,tmpdat,by="stock_code",all.x=TRUE)
#     re <- re[,c(names(TS),"factorscore")]
    return(re)
  }
  re <- TS.getFactor.db(TS,subfun,con_type=con_type)
  return(re)
}




# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# ===================== Andrew  ===================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============

#inner function
expandTS2TSF <- function(TS,nwin,rawdata){
  TS_ <- data.frame(date=unique(TS$date))
  TS_ <- transform(TS_,begT=trday.nearby(date,-nwin))
  TS_ <- TS_ %>% dplyr::rowwise() %>%
    dplyr::do(data.frame(date=.$date,TradingDay=trday.get(.$begT, .$date)))
  TS_ <- dplyr::full_join(TS_,TS,by='date')
  result <- dplyr::left_join(TS_,rawdata,by=c('stockID','TradingDay'))
  result <- na.omit(result)
  result <- dplyr::arrange(result,date,stockID,TradingDay)
  return(result)
}


#' @rdname getfactor
#' @export
gf.liquidity <- function(TS,nwin=22){
  check.TS(TS)
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)
  
  conn <- db.local()
  qr <- paste("select t.TradingDay ,t.ID 'stockID',t.TurnoverVolume/10000 'TurnoverVolume',t.NonRestrictedShares
              from QT_DailyQuote2 t where ID in",brkQT(unique(TS$stockID)), 
              " and t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<",rdate2int(endT))
  rawdata <- RSQLite::dbGetQuery(conn,qr)
  RSQLite::dbDisconnect(conn)
  rawdata <- dplyr::filter(rawdata,TurnoverVolume>=0,NonRestrictedShares>0)
  rawdata <- transform(rawdata,TradingDay=intdate2r(TradingDay),
                       TurnoverRate=TurnoverVolume/NonRestrictedShares)
  rawdata <- rawdata[,c("TradingDay","stockID","TurnoverRate")]
  re <- expandTS2TSF(TS,nwin,rawdata)
  
  tmp.TSF <- re %>% dplyr::group_by(date, stockID) %>% 
    dplyr::summarise(factorscore=sum(TurnoverRate,na.rm = T)) %>% dplyr::ungroup()
  tmp.TSF <- dplyr::filter(tmp.TSF,factorscore>0)
  tmp.TSF <- transform(tmp.TSF,factorscore=log(factorscore))
  
  TSF <- dplyr::left_join(TS,tmp.TSF,by=c('date','stockID'))
  return(TSF)
}


#' @rdname getfactor
#' @export
gf.ILLIQ <- function(TS,nwin=22){
  check.TS(TS)
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)
  
  conn <- db.local()
  qr <- paste("select t.TradingDay,t.ID 'stockID',t.DailyReturn,
              t.TurnoverValue from QT_DailyQuote2 t 
              where ID in",brkQT(unique(TS$stockID)), 
              " and t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<",rdate2int(endT))
  rawdata <- RSQLite::dbGetQuery(conn,qr)
  RSQLite::dbDisconnect(conn)
  rawdata <- dplyr::filter(rawdata,TurnoverValue>0)
  rawdata <- transform(rawdata,TradingDay=intdate2r(TradingDay),
                       ILLIQ=abs(DailyReturn)/(TurnoverValue/100000000))
  rawdata <- rawdata[,c("TradingDay","stockID","ILLIQ")]
  re <- expandTS2TSF(TS,nwin,rawdata)
  
  tmp.TSF <- re %>% dplyr::group_by(date, stockID) %>% 
    dplyr::summarise(factorscore=mean(ILLIQ,na.rm = T)) %>% dplyr::ungroup()
  tmp.TSF <- dplyr::filter(tmp.TSF,factorscore>0)
  tmp.TSF <- dplyr::mutate(tmp.TSF,factorscore=log(factorscore)) # log it, for normalize.
  tmp.TSF <- na.omit(tmp.TSF)
  TSF <- dplyr::left_join(TS,tmp.TSF,by = c("date", "stockID"))
  return(TSF)
}




#' @rdname getfactor
#' @export
gf.beta <- function(TS,nwin=250){
  check.TS(TS)
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)
  qr <- paste("select TradingDay,ID 'stockID',DailyReturn 'stockRtn'
              from QT_DailyQuote2
              where ID in",brkQT(unique(TS$stockID)), 
              " and TradingDay>=",rdate2int(begT)," and TradingDay<",rdate2int(endT))
  conn <- db.local()
  rawdata <- RSQLite::dbGetQuery(conn,qr)
  DBI::dbDisconnect(conn)
  rawdata <- transform(rawdata,TradingDay=intdate2r(TradingDay))
  
  index <- getIndexQuote("EI801003",begT,endT,'pct_chg',datasrc = 'jy')
  index <- dplyr::rename(index,TradingDay=date)
  rawdata <- dplyr::left_join(rawdata,index[,c('TradingDay','pct_chg')],by='TradingDay')
  
  pb <- txtProgressBar(style = 3)
  dates <- sort(unique(TS$date))
  tmp.TSF <- data.frame()
  for(i in dates){
    i <- as.Date(i,origin='1970-01-01')
    begT <- trday.nearby(i,-nwin)
    tmp.TS <- TS[TS$date==i,]
    tmp <- rawdata %>% dplyr::filter(TradingDay<i,TradingDay>=begT,stockID %in% tmp.TS$stockID) %>% 
      dplyr::group_by(stockID)  %>%  
      dplyr::filter(n()>=nwin/2) %>% 
      dplyr::do(factorscore = lm(stockRtn ~ pct_chg, data = .)$coef[[2]]) %>% dplyr::ungroup()
    tmp <- transform(tmp,factorscore=as.numeric(factorscore))
    tmp$date <- i
    tmp.TSF <- rbind(tmp.TSF,tmp)
    setTxtProgressBar(pb,  findInterval(i,dates)/length(dates))
  }
  close(pb)
  TSF <- dplyr::left_join(TS,tmp.TSF,by = c("date", "stockID"))
  return(TSF)
}




#' @rdname getfactor
#' @export
gf.IVR <- function(TS,nwin=22){
  check.TS(TS)
  
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)
  indexs <- c('EI801003','EI801811','EI801813','EI801831','EI801833')
  FF3 <- getIndexQuote(indexs,begT,endT,"pct_chg",datasrc = 'jy')
  FF3 <- reshape2::dcast(FF3,date~stockID,value.var = 'pct_chg')
  FF3 <- transform(FF3,SMB=EI801813-EI801811,HML=EI801833-EI801831,market=EI801003)
  FF3 <- FF3[,c('date','SMB','HML','market')]
  FF3 <- dplyr::rename(FF3,TradingDay=date)
  
  conn <- db.local()
  qr <- paste("select t.TradingDay,t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote2 t where ID in",brkQT(unique(TS$stockID)),
              " and t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<",rdate2int(endT))
  rawdata <- RSQLite::dbGetQuery(conn,qr)
  RSQLite::dbDisconnect(conn)
  rawdata <- transform(rawdata,TradingDay=intdate2r(TradingDay))
  rawdata <- dplyr::left_join(rawdata,FF3,by='TradingDay')
  re <- expandTS2TSF(TS,nwin,rawdata)
  
  pb <- txtProgressBar(style = 3)
  dates <- sort(unique(TS$date))
  tmp.TSF <- data.frame()
  for(i in dates){
    i <- as.Date(i,origin='1970-01-01')
    tmp <- re %>% dplyr::filter(date==i) %>% dplyr::group_by(stockID) %>% dplyr::filter(n()>=nwin/2) %>% 
      dplyr::do(factorscore = 1-summary(lm(stockRtn ~ SMB+HML+market,data = .))$r.squared) %>% dplyr::ungroup()
    tmp <- transform(tmp,factorscore=as.numeric(factorscore))
    tmp <- na.omit(tmp)
    tmp$date <- i
    tmp.TSF <- rbind(tmp.TSF,tmp)
    setTxtProgressBar(pb,  findInterval(i,dates)/length(dates))
  }
  close(pb)
  TSF <- dplyr::left_join(TS,tmp.TSF,by = c("date", "stockID"))
  return(TSF)
}




#' @rdname getfactor
#' @export
gf.volatility <- function(TS,nwin=60){
  check.TS(TS)
  funchar <- paste("StockStdev2(",nwin,")",sep='')
  TSF <- TS.getTech_ts(TS,funchar)
  colnames(TSF) <- c('date','stockID','factorscore')
  return(TSF)
}






#' @rdname getfactor
#' @export
gf.disposition <- function(TS,nwin=66){
  check.TS(TS)
  
  begT <- trday.nearby(min(TS$date),-nwin)
  endT <- max(TS$date)
  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',
              t.RRClosePrice,t.TurnoverVolume/10000 'TurnoverVolume',t.NonRestrictedShares
              from QT_DailyQuote2 t where ID in",brkQT(unique(TS$stockID)), 
              " and t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<",rdate2int(endT))
  conn <- db.local()
  rawdata <- dbGetQuery(conn,qr)
  dbDisconnect(conn)
  rawdata <- dplyr::filter(rawdata,NonRestrictedShares>0)
  rawdata <- transform(rawdata,date=intdate2r(date),
                       turnover=abs(TurnoverVolume)/NonRestrictedShares)
  rawdata <- rawdata[,c("date","stockID","RRClosePrice","turnover")]
  
  pb <- txtProgressBar(style = 3)
  dates <- sort(unique(TS$date))
  tmp.TSF <- data.frame()
  for(i in dates){
    i <- as.Date(i,origin='1970-01-01')
    begT <- trday.nearby(i,-nwin)
    
    re <- rawdata %>% dplyr::filter(stockID %in% TS[TS$date==i,'stockID'],date>=begT,date<i) %>% 
      dplyr::arrange(stockID,date)
    
    tmpprice <- re %>% dplyr::group_by(stockID) %>% 
      dplyr::summarise(P = last(RRClosePrice)) %>% dplyr::ungroup()
    
    re <- dplyr::left_join(re,tmpprice,by='stockID')
    re <- transform(re,gain=ifelse(RRClosePrice<P,1-RRClosePrice/P,0),
                    loss=ifelse(RRClosePrice>P,(1-RRClosePrice/P),0),
                    turnovervise=1-re$turnover)
    
    tmpprice <- re %>% dplyr::group_by(stockID) %>% 
      dplyr::summarise(zero=all(turnover==0),n=n()) %>% 
      dplyr::filter(n==nwin,zero==FALSE) %>% dplyr::ungroup()
    re <- re %>% dplyr::filter(stockID %in% tmpprice$stockID) %>% 
      dplyr::arrange(stockID,dplyr::desc(date))
    
    tmpdf <- re %>% dplyr::group_by(stockID) %>% 
      dplyr::do(tmp = c(1,cumprod(.$turnovervise[1:(nwin-1)])))
    tmpdf <- tmpdf %>% dplyr::do(data.frame(tmp = .$tmp))
    re <- cbind(re,tmpdf)
    
    tmpdf <- re %>% dplyr::group_by(stockID) %>% 
      dplyr::do(wgt = .$turnover*.$tmp)
    tmpdf <- tmpdf %>% dplyr::do(data.frame(wgt = .$wgt,tot=sum(.$wgt)))
    tmpdf$wgt <- tmpdf$wgt/tmpdf$tot
    tmpdf$tot <- NULL
    re <- cbind(re,tmpdf)
    
    tmp <- re %>% dplyr::group_by(stockID) %>% 
      dplyr::summarise(factorscore=as.numeric(gain %*% wgt+loss %*% wgt)) %>% dplyr::ungroup()
    tmp$date <- i
    tmp.TSF <- rbind(tmp.TSF,tmp)
    setTxtProgressBar(pb,  findInterval(i,dates)/length(dates))
  }
  close(pb)
  TSF <- dplyr::left_join(TS,tmp.TSF,by = c("date", "stockID"))
  return(TSF)
}




# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
# ===================== to be tested  ===================
# ===================== xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ==============
#' @export
gf.rotation_s <- function(TS){
  PB <- gf.PB_mrq(TS)$factorscore
  roe <- gf.ROE_ttm(TS)$factorscore/100
  rotation <- PB/ROE
  return(data.frame(TS,factorscore=rotation,PB=PB,roe=roe))
}

#' @export
gf.rotation <- function(TS){
  PB <- gf.PB_mrq(TS)$factorscore
  roe <- gf.ROE_ttm(TS)$factorscore/100
  rotation <- log(2*PB,(1+roe))
  return(data.frame(TS,factorscore=rotation,PB=PB,roe=roe))
}



