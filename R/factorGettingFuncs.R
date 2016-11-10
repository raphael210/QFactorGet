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
gf.ln_mkt_cap <- function(TS){
  re <- TS.getTech(TS,variables="mkt_cap")
  re <- renameCol(re,"mkt_cap","factorscore")
  re$factorscore <- ifelse(is.na(re$factorscore),NA,log(re$factorscore))
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
  re <- DBI::dbGetQuery(con,qr)
  DBI::dbDisconnect(con)  
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
    dt_lag <- trday.nearby(dt,by=lag)
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


#' get liquidity factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @param nwin is time window.
#' @param datasrc is data source parameter,default value is "local".
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.liquidity(TS)
#' TSF <- gf.liquidity(TS,datasrc = 'quant')
#' @export
gf.liquidity <- function(TS,nwin=21,datasrc = defaultDataSRC()){
  check.TS(TS)
  
  begT <- trday.nearby(min(TS$date),nwin)
  endT <- max(TS$date)
  if(datasrc=='local'){
    conn <- db.local()
    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.TurnoverVolume,t.NonRestrictedShares
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    re <- RSQLite::dbGetQuery(conn,qr)
    RSQLite::dbDisconnect(conn)
  }else if(datasrc=='quant'){
    conn <- db.quant()
    tmp <- unique(TS$stockID)
    if(length(tmp)<500){
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.TurnoverVolume,t.NonRestrictedShares
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT),
                  " and t.ID in",paste("(",paste(QT(tmp),collapse=","),")"))
    }else{
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.TurnoverVolume,t.NonRestrictedShares
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT))
    }
    re <- RODBC::sqlQuery(conn,qr)
    RODBC::odbcClose(conn)
  }
  
  re <- re[re$stockID %in% c(unique(TS$stockID)),]
  re$TurnoverRate <- abs(re$TurnoverVolume/(re$NonRestrictedShares*10000))
  re <- re[,c("date","stockID","TurnoverRate")]
  tmp <- as.data.frame(table(re$stockID))
  tmp <- tmp[tmp$Freq>=nwin,]
  re <- re[re$stockID %in% tmp$Var1,]
  re <- plyr::arrange(re,stockID,date)
  
  re <- plyr::ddply(re,"stockID",plyr::here(plyr::mutate),factorscore=zoo::rollapply(TurnoverRate,nwin,sum,fill=NA,align = 'right'))
  re <- subset(re,!is.na(re$factorscore))
  re <- subset(re,factorscore>=0.000001)
  re$factorscore <- log(re$factorscore)
  re <- re[,c("date","stockID","factorscore")]
  re$date <- intdate2r(re$date)
  re <- re[re$date %in% c(unique(TS$date)),]
  
  TSF <- merge.x(TS,re)
  return(TSF)
}



#' get beta factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @param nwin  time window.
#' @param datasrc is data source parameter,default value is "local".
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.beta(TS)
#' TSF <- gf.beta(TS,datasrc = 'quant')
#' @export
gf.beta <- function(TS,nwin=250,datasrc = defaultDataSRC()){
  check.TS(TS)
  
  begT <- trday.nearby(min(TS$date),nwin)
  endT <- max(TS$date)
  
  if(datasrc=='local'){
    conn <- db.local()
    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    re <- RSQLite::dbGetQuery(conn,qr)
    RSQLite::dbDisconnect(conn)
  }else if(datasrc=='quant'){
    conn <- db.quant()
    tmp <- unique(TS$stockID)
    if(length(tmp)<500){
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT),
                  " and t.ID in",paste("(",paste(QT(tmp),collapse=","),")"))
    }else{
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT))
    }
    re <- RODBC::sqlQuery(conn,qr)
    RODBC::odbcClose(conn)
  }
  re <- re[re$stockID %in% unique(TS$stockID),]
  re <- plyr::arrange(re,stockID,date)
  
  
  qr <- paste("SELECT convert(varchar(8),q.[TradingDay],112) 'date',
              q.ClosePrice/q.PrevClosePrice-1 'indexRtn'
              FROM QT_IndexQuote q,SecuMain s
              where q.InnerCode=s.InnerCode AND s.SecuCode='801003'
              and q.TradingDay>=",QT(begT),
              " and q.TradingDay<=",QT(endT))
  con <- db.jy()
  index <- RODBC::sqlQuery(con,qr)
  RODBC::odbcClose(con)
  
  re <- merge.x(re,index)
  re <- re[!is.na(re$indexRtn),]
  tmp <- as.data.frame(table(re$stockID))
  tmp <- tmp[tmp$Freq>=nwin,]
  re <- re[re$stockID %in% tmp$Var1,]
  re <- plyr::arrange(re,stockID,date)
  
  stocks <- unique(re$stockID)
  pb <- txtProgressBar(style = 3)
  for(j in 1:length(stocks)){
    tmp <- re[re$stockID==stocks[j],]
    beta.tmp <- zoo::rollapply(tmp[,c('indexRtn','stockRtn')], width = nwin,
                               function(x) coef(lm(stockRtn ~ indexRtn, data = as.data.frame(x)))[2],
                               by.column = FALSE, align = "right")
    beta.tmp <- data.frame(date=tmp$date[nwin:nrow(tmp)],
                           stockID=stocks[j],
                           factorscore=beta.tmp)
    if(j==1){
      beta <- beta.tmp
    }else{
      beta <- rbind(beta,beta.tmp)
    }
    setTxtProgressBar(pb, j/length(stocks))
  }
  close(pb)
  beta$date <- intdate2r(beta$date)
  beta <- beta[beta$date %in% unique(TS$date),]
  TSF <- merge.x(TS,beta)
  
  return(TSF)
}



#' get IVR factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @param nwin time window.
#' @param datasrc is data source parameter,default value is "local".
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.IVR(TS)
#' TSF <- gf.IVR(TS,datasrc = 'quant')
#' @export
gf.IVR <- function(TS,nwin=22,datasrc = defaultDataSRC()){
  check.TS(TS)
  
  begT <- trday.nearby(min(TS$date),nwin)
  endT <- max(TS$date)
  indexs <- c('EI801811','EI801813','EI801831','EI801833')
  FF3 <- getIndexQuote(indexs,begT,endT,"pct_chg",datasrc = 'jy')
  FF3 <- reshape2::dcast(FF3,date~stockID,value.var = 'pct_chg')
  FF3 <- transform(FF3,SMB=EI801813-EI801811,HML=EI801833-EI801831)
  FF3 <- FF3[FF3$date>=begT & FF3$date<=endT,c('date','SMB','HML')]
  
  tmp <- getIndexQuote('EI801003',begT,endT,variables = 'pct_chg',datasrc = 'jy')
  tmp <- tmp[,c('date','pct_chg')]
  colnames(tmp) <- c('date','market')
  FF3 <- merge(FF3,tmp,by='date')
  
  
  if(datasrc=='local'){
    conn <- db.local()
    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    stockrtn <- RSQLite::dbGetQuery(conn,qr)
    RSQLite::dbDisconnect(conn)
  }else if(datasrc=='quant'){
    conn <- db.quant()
    tmp <- unique(TS$stockID)
    if(length(tmp)<500){
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT),
                  " and t.ID in",paste("(",paste(QT(tmp),collapse=","),")"))
    }else{
      qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                  from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                  " and t.TradingDay<=",rdate2int(endT))
    }
    stockrtn <- RODBC::sqlQuery(conn,qr)
    RODBC::odbcClose(conn)
  }
  
  stockrtn <- stockrtn[stockrtn$stockID %in% unique(TS$stockID),]
  stockrtn <- plyr::arrange(stockrtn,stockID,date)
  stockrtn$date <- intdate2r(stockrtn$date)
  
  tmp.stock <- unique(stockrtn$stockID)
  IVR <- data.frame()
  pb <- txtProgressBar(style = 3)
  for(i in 1:length(tmp.stock)){
    tmp.rtn <- stockrtn[stockrtn$stockID==tmp.stock[i],]
    tmp.FF3 <- merge(FF3,tmp.rtn[,c('date','stockRtn')],by='date',all.x=T)
    tmp.FF3 <- na.omit(tmp.FF3)
    if(nrow(tmp.FF3)<nwin) next
    tmp <- zoo::rollapply(tmp.FF3[,c("stockRtn","market","SMB","HML")], width =nwin,
                          function(x){
                            x <- as.data.frame(x)
                            if(sum(x$stockRtn==0)>=10){
                              result <- NaN
                            }else{
                              tmp.lm <- lm(stockRtn~market+SMB+HML, data = x)
                              result <- 1-summary(tmp.lm)$r.squared
                            }
                            return(result)},by.column = FALSE, align = "right")
    IVR.tmp <- data.frame(date=tmp.FF3$date[nwin:nrow(tmp.FF3)],
                          stockID=as.character(tmp.stock[i]),
                          IVRValue=tmp)
    IVR <- rbind(IVR,IVR.tmp)
    setTxtProgressBar(pb, i/length(tmp.stock))
  }
  close(pb)
  IVR <- IVR[!is.nan(IVR$IVRValue),]
  colnames(IVR) <- c('date','stockID','factorscore')
  
  TSF <- merge.x(TS,IVR,by=c('date','stockID'))
  return(TSF)
}


#' @rdname getfactor
#' @export
gf.free_float_shares <- function(TS){
  
  qr <- "select b.date, b.stockID,a.freeShares as 'factorscore'
  from QT_FreeShares a, yrf_tmp b
  where a.rowid=(
  select rowid from QT_FreeShares
  where stockID=b.stockID and date<=b.date
  order by date desc limit 1)"
  
  con <- db.local()
  TS$date <- rdate2int(TS$date)
  dbWriteTable(con,name="yrf_tmp",value=TS,row.names = FALSE,overwrite = TRUE)
  re <- dbGetQuery(con,qr)
  re <- merge.x(TS,re,by=c("date","stockID"))
  re <- transform(re, date=intdate2r(date))
  dbDisconnect(con)
  return(re)
}


#' @rdname getfactor
#' @export
gf.free_float_sharesMV <- function(TS){
  ffs <- gf.free_float_shares(TS)
  close <- TS.getTech(TS,variables='close')
  re <- merge(ffs,close,by=c('date','stockID'))
  re$factorscore <- re$factorscore*re$close
  re <- re[,c("date","stockID","factorscore")]
  return(re)
}






