libname BB0419IN 'G:\Data\Projects\Bettting Against Beta\SASData\2018-04-19\Intl'; run;

%let wrds=wrds-cloud.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;        
signon username=_prompt_;               
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname fx '/wrds/frb/sasdata'; run; *Refers to the Federal Reserve Bank;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname naa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname FF ' /wrds/ff/sasdata'; run;
libname xpf '/wrds/comp/sasdata/d_global'; run; *refers to Xpressfeed;
libname NK '/scratch/isb/NK'; run; *temporary directory on WRDS;
endrsubmit;



rsubmit;


/********************************************************************/
/*** STEP-1: ESTIMATING EX-ANTE BETAS USING CRSP STOCK-LEVEL DATA ***/
/********************************************************************/


/* Step 1.1. Select common stocks (tpci = 0) from CRSP */
proc sql;
  create table Stocks as
  select distinct gvkey, upcase(loc) as loc
  from xpf.g_secd
  where tpci='0';
quit;
*56,972 gvkeys;


/* Step 1.2. Subsetting countries of interest */
*County codes are listed from Xpressfeed documentation?;
data CountryList;
	input code $ country $13.;
	datalines;
	AUS AUSTRALIA
	AUT AUSTRIA
	BEL BELGIUM
	CAN CANADA
	DNK DENMARK
	FIN FINLAND
	FRA FRANCE
	DEU GERMANY
	HKG HONGKONG
	ITA ITALY
	JPN JAPAN
	NLD NETHERLANDS
	NOR NORWAY
	NZL NEWZEALAND
	SWE SWEDEN
	SGP SINGAPORE
	ESP SPAIN
	CHE SWITZERLAND
	GBR UNITEDKINGDOM
	GRC GREECE
	IRL IRELAND
	ISR ISRAEL
	PRT PORTUGAL
	BRA BRAZIL
	CHL CHILE
	MEX MEXICO
	IND INDIA
	TWN TAIWAN
	THA THAILAND
	MYS MALAYSIA
	ZAF SOUTHAFRICA
	;
*31 countries: 22 msci developed and 9 msci emerging countries;


*Ncountries contains total number of countries;
proc sql noprint;
  select distinct count(*) into :Ncountries from CountryList;  
quit;
%put &Ncountries; *31 countries;


*Number codes for countries;
data CountryList;
  set CountryList;
  CountryID = _N_; 
run;
data NK.CountryList; set CountryList; run;


/* Step 1.3. Creating unique gvkey dataset */
*Select stocks from the countries in CountryList;
proc sql;
  create table StocksList as
  select distinct a.*, b.*
  from Stocks as a, CountryList as b
  where a.loc = b.code;
quit;
*38,906 gvkeys;


proc sort data=StocksList nodupkey; by gvkey; run;
*38906 gvkeys, 0 obs deleted;


proc sort data=StocksList nodupkey; by gvkey code country; run;
data StocksList;
  format gvkey;
  set StocksList;
  gvkeyid = _N_; 
run;
data NK.StocksList; set StocksList; run;
*38906 obs;


/* Step 1.4. Last trading day by month and year */
proc sql;
  create table LastTradingDay as
  select distinct year(datadate) as year, month(datadate) as month, upcase(loc) as loc, day(datadate) as LastTradingDay
  from xpf.g_secd
  group by year(datadate), month(datadate), upcase(loc)
  having datadate=max(datadate);
quit;
data NK.LastTradingDay; set LastTradingDay; run;



/* Step 1.5. Create foreign exchange tables */
**Foreign exchange data (for all countries except Netherlands, Chile and Israel) after 1994 is obtained from Thomson Reuters 
and before 1994 is obtained from WRDS;
**Foreign exchange data for Netherlands is obtained only from WRDS;
**Foreign exchange data for Chile and Israel is obtained only from Thomson Reuters;

**Step 1.5.1. Foreign exchange data (for all countries except Netherlands, Chile and Israel) before 1994;
data exchange;
set fx.fx_daily;
 rename exalus	=	AUSTRALIA
; rename exauus	=	AUSTRIA
; rename exbeus	=	BELGIUM
; rename exbzus	=	BRAZIL
; rename excaus	=	CANADA
; rename exdnus	=	DENMARK
; rename exeuus	=	EURO
; rename exfnus	=	FINLAND
; rename exfrus	=	FRANCE
; rename exgeus	=	GERMANY
; rename exgrus	=	GREECE
; rename exhkus	=	HONGKONG
; rename exinus	=	INDIA
; rename exirus	=	IRELAND
; rename exitus	=	ITALY
; rename exjpus	=	JAPAN
; rename exmaus	=	MALAYSIA
; rename exmxus	=	MEXICO
; rename exnous	=	NORWAY
; rename exnzus	=	NEWZEALAND
; rename expous	=	PORTUGAL
; rename exsdus	=	SWEDEN
; rename exsfus	=	SOUTHAFRICA
; rename exsius	=	SINGAPORE
; rename exspus	=	SPAIN
; rename exszus	=	SWITZERLAND
; rename extaus	=	TAIWAN
; rename exthus	=	THAILAND
; rename exukus	=	UNITEDKINGDOM
; rename exvzus	=	VENEZUELA;				
if year(date)<=1993;
run;

data exchange;
	set exchange;
	keep DATE AUSTRALIA AUSTRIA BELGIUM CANADA DENMARK FINLAND FRANCE
  GERMANY HONGKONG ITALY JAPAN NORWAY NEWZEALAND SWEDEN SINGAPORE
  SPAIN SWITZERLAND UNITEDKINGDOM PORTUGAL IRELAND GREECE BRAZIL MEXICO INDIA TAIWAN THAILAND MALAYSIA SOUTHAFRICA;
run;

*Converting local currency to Euro;
data exchange;
	set exchange;
	IF YEAR(DATE)>1998 THEN  AUSTRIA=EURO;
	IF YEAR(DATE)>1998 THEN  BELGIUM=EURO;
	IF YEAR(DATE)>1998 THEN  SPAIN=EURO;
	IF YEAR(DATE)>1998 THEN  FINLAND=EURO;
	IF YEAR(DATE)>1998 THEN  FRANCE=EURO;
	IF YEAR(DATE)>1998 THEN  GERMANY=EURO;
	IF YEAR(DATE)>1998 THEN  ITALY=EURO;
	IF YEAR(DATE)>1998 THEN  PORTUGAL=EURO;
	IF YEAR(DATE)>1998 THEN  GREECE=EURO;
run;

/*converting wide to long form */
proc transpose data=exchange out=exchange;
  by date notsorted;
  var AUSTRALIA AUSTRIA BELGIUM CANADA DENMARK FINLAND FRANCE
  GERMANY HONGKONG ITALY JAPAN NORWAY NEWZEALAND SWEDEN SINGAPORE
  SPAIN SWITZERLAND UNITEDKINGDOM PORTUGAL IRELAND GREECE BRAZIL MEXICO INDIA TAIWAN THAILAND MALAYSIA SOUTHAFRICA
;
run;

data exchange;
	set exchange;
	_NAME_=upcase(country);
	rename _NAME_=country;
	rename col1=rate;
	drop _label_;
run;

**Step 1.5.2. Foreign exchange data for Netherlands (Taken from WRDS Since it's not available on Reuters/Bloomberg);
data exchange1;
	set fx.fx_daily;
	keep date exneus exeuus;
	rename exneus=NETHERLANDS;
	rename exeuus=EURO;
run;

data exchange1;
	set exchange1;
	if year(date)>1998 then  NETHERLANDS=EURO;
	drop Euro;
run;

/*converting wide to long form */
proc transpose data=exchange1 out=exchange1;
  by date notsorted;
  var NETHERLANDS;
run;

data exchange1;
	set exchange1;
	rename _NAME_=country;
	rename col1=rate;
	drop _label_;
run;

**Step 1.5.3. Merging Netherlands data with the data of other countries; 
proc append data=exchange1 base=exchange; run;

**Step 1.5.4. Merging with foreign exchange data (ash.forex) after 1994, obtained from Thomson Reuters (except for Netherlands);
**Put forex file from dropbox in the scratch folder on WRDS;
proc append data=ash.forex base=exchange; run;

data exchange;
	set exchange;
	COUNTRY=UPCASE(COUNTRY);
run;

***Include country code to exchange;
proc sql;
	create table ash.exchange as
	select distinct a.*,upcase(b.code) as loc
	from exchange as a, ash.country as b
	where a.country=b.country;
quit;






/* Step 1.6. Calculate stock-level ex-ante vols and correlations */
**Put returns file from dropbox in the scratch folder on WRDS;

rsubmit;
data country;set ash.country;run;
data Stocks;set ash.Stocks;run;
data exchange;set ash.exchange;run;
data returns;set ash.returns;run;
data LastTradingDay ;set ash.LastTradingDay ;run;

data returns;
	set returns;
	country=upcase(country);
run;

proc sql noprint;
  select distinct count(*) into :country from country;  
quit;

*totpermno contains total number of permnos;
proc sql noprint;
  select distinct count(*) into :totpermno from Stocks;  
quit;

options mprint symbolgen;
%macro TSBeta;
%do i=1 %to &totpermno;

    ***Select one gvkey from perms by i counter;
    proc sql;
	  create table Permno_Temp as
	  select distinct *
	  from Stocks
	  where permnoid=&i;
	quit;
    
	proc sql noprint;
      select gvkey into :perm from Permno_Temp;
    quit;

	proc sql noprint;
      select country into :loc from Permno_Temp;
    quit;

	proc sql;
	  create table DailyData_Temp as
	  select distinct datadate format=date9., gvkey, upcase(loc) as loc, prccd, TRFD
	  from xpf.g_secd
	  where input(gvkey,best12.)=&perm and missing(prccd)=0;
	quit;

    ***Include foreign exchange rates;
	proc sql;
	  create table DailyData_Temp as
	  select distinct a.*, b.rate as forex_rate,b.country
	  from DailyData_Temp as a, exchange as b
	  where a.datadate=b.date and a.loc=b.loc;
	quit;

	*Filling missing values with previous value;
/*	data DailyData_Temp;*/
/*		set DailyData_Temp;*/
/*		retain _forex_rate;*/
/*		if not missing(forex_rate) then _forex_rate=forex_rate;*/
/*		else forex_rate=_forex_rate;*/
/*		drop _forex_rate;*/
/*	run;*/

	***Calculating USD returns;
	data DailyData_Temp;
		set DailyData_Temp;
		by gvkey;

		*A)Local currency returns;
		if first.gvkey=1 then ret=.;
		else ret=log(abs(prccd)/abs(lag(prccd)))*TRFD;

		*B)USD returns: Method-1;
/*		if first.gvkey=1 then ret_usd=.;*/
/*		else ret_usd=(1+(log(abs(prccd)/abs(lag(prccd)))*TRFD))*(1+log((1/forex_rate)/(1/lag(forex_rate))))-1;*/

		*C)USD returns: Method-2;
		if first.gvkey=1 then ret_usd=.;
		else ret_usd=log(abs(prccd*forex_rate)/(abs(lag(prccd)*lag(forex_rate))*TRFD));
	run;
	
	*Include market returns;
	proc sql;
  	  create table DailyData_Temp as
  	  select distinct a.*, b.RET
      from DailyData_Temp as a, returns as b
      where a.datadate=b.date and a.country=b.country;
    quit;
    
    *Estimating three day returns;
    proc sort data=DailyData_Temp; by gvkey datadate; run;
    data DailyData_Temp;
      set DailyData_Temp;
      by gvkey datadate;
      lag1ret = lag(ret_usd);
      lag1vwretd = lag(RET);
      if first.gvkey=1 then lag1ret=.;
      if first.gvkey=1 then lag1vwretd=.;
    run;

    data DailyData_Temp;
      set DailyData_Temp;
      by gvkey datadate;
      lag2ret = lag(lag1ret);
      lag2vwretd = lag(lag1vwretd);
      if first.permno=1 then lag2ret=.;
      if first.permno=1 then lag2vwretd=.;
    run;
    
    data DailyData_Temp;
      format date gvkey lret lvwretd lret3d lvwretd3d;
      set DailyData_Temp;
      if missing(lag1ret)=1 or missing(lag2ret)=1 then delete;

      lret3d = sum(log(1+ret_usd), log(1+lag1ret), log(1+lag2ret));
      lvwretd3d = sum(log(1+RET), log(1+lag1vwretd), log(1+lag2vwretd));

      *Log daily returns;
      lret = log(1+ret_usd);
      lvwretd = log(1+RET);

      *Keep relevant variables;
      keep datadate gvkey lret lvwretd lret3d lvwretd3d loc country;
    run;


	***Subsetting month end data;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct datadate,gvkey,country,loc
	  from DailyData_Temp
	  group by month(datadate),year(datadate)
	  having datadate=max(datadate);
	quit;
	
     
	*Last observsation may not be month end date, for instance, if the data ends on 15Aug2017, then the last row will be 15Aug2017. Remove such observsations;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct a.*
	  from MonthEnd_Temp as a, LastTradingDay as b
	  where year(a.datadate)=b.year and month(a.datadate)=b.month and day(a.datadate)=b.LastTradingDay and a.loc=b.loc;
	quit;
    
	***Create past 1-yr and 5-yr dates;
	data MonthEnd_Temp;
	  set MonthEnd_Temp;
	  vol_date=intnx('day',datadate,-365); *past 1-yr date;
	  corr_date=intnx('day',datadate,-1825); *past 5-yr date;
	  format vol_date date9. corr_date date9.;
	run;
    
	***Subset data for volatility;
	proc sql;
	  create table VolData_Temp as
	  select distinct a.datadate, a.gvkey, a.vol_date, b.datadate as date1, b.lret, b.lvwretd
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.gvkey=b.gvkey and a.vol_date<b.datadate<=a.datadate
	  group by a.datadate, a.gvkey
	  having count(distinct b.datadate)>=120; *Require good return for 120 trading days;
	quit;

	***Subset data for correlation;
	proc sql;
      create table CorrData_Temp as
	  select distinct a.datadate, a.gvkey, a.corr_date, b.datadate as date1, b.lret3d, b.lvwretd3d
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.gvkey=b.gvkey and a.corr_date<b.datadate<=a.datadate
	  group by a.datadate, a.gvkey
	  having count(distinct b.datadate)>=750; *Require good return for 120 trading days;
	quit;
    
    ***Estimate trailing stock and market volatilities;
	proc sql;
      create table Vol_&i as
	  select distinct datadate, gvkey, std(lret) as stockvol, std(lvwretd) as mktvol
      from VolData_Temp
	  group by datadate, gvkey;
	quit;
 
    ***Estimate trailing trailing 3day return correlation;
	proc corr data=CorrData_Temp noprint out=Corr_&i;
	  by datadate gvkey;
	  var lret3d lvwretd3d;
	run;
	proc sql;
	  create table Corr_&i as
	  select distinct datadate, gvkey, lret3d as corr
	  from Corr_&i
	  where _TYPE_='CORR' and _NAME_='lvwretd3d';
	quit;

   
	***Merge data;
	proc sql;
	  create table TSBeta_&i as
	  select distinct a.datadate, a.gvkey, b.stockvol, b.mktvol, c.corr
	  from MonthEnd_Temp as a, Vol_&i as b, Corr_&i as c
	  where a.datadate=b.datadate=c.datadate and a.gvkey=b.gvkey=c.gvkey and missing(b.stockvol)=0 and missing(c.corr)=0;
	quit;
     
	***Copy TSBeta_&i to Ash;
	data ASH1.TSBeta_&i; set TSBeta_&i; run;
    
	***Drop temporary data;
	proc sql;
	  drop table CorrData_Temp, Corr_&i, DailyData_Temp, MonthEnd_Temp, Permno_Temp, TSBeta_&i, VolData_Temp, Vol_&i;
	quit;

%end;
%mend TSBeta;

*Call marco;
%TSBeta;

*Restore log output in log window;
proc printto; run;

***House cleaning;
proc sql;
  drop table ASH.CRSPStocks, ASH.LastTradingDay;
quit;



/* Step 1.5. Create a consolidated dataset with ex-ante vols and correlations */

*Run a PROC CONTENTS to create a SAS data set with the names of the SAS data sets in the SAS data  library;
proc contents data=ASH._all_ out=ASH.NKcont(keep=memname) noprint; run;


*Eliminate any duplicate names of the SAS data set names stored in the SAS data set;
proc sort data=ASH.NKcont nodupkey; by memname; run;
*24,698 obs;


*Run a DATA _NULL_ step to create 2 macro variables: one with the names of each SAS data set and 
the other with the final count of the number of SAS data sets;
data _null_;
  set ASH.NKcont end=last;
  by memname;
  i+1;
  call symputx('name'||trim(left(put(i,8.))),memname);
  if last then call symputx('count',i);
run;

* Direct log of macro to a different directory;
proc printto log="D:\BAB_ADARSH\Prof Nitin\BAB1.log"; run;

*Macro containing the PROC APPEND that executes for each SAS data set you want to concatenate together to create 1 SAS data set;
%macro combdsets;
  %do i=1 %to &count;
    proc append base=TSBeta data=ASH.&&name&i force; run;
  %end;
%mend combdsets;

*Call macro;
%combdsets;

*Restore log output in log window;
proc printto; run;
  



/* Step 1.6. Calculate time-series betas */

data TSBeta;
  set TSBeta;
  TSbeta = corr * (stockvol / mktvol);
run;

*Sanity check;
proc sql;
  create table DistinctPermno as
  select distinct gvkey
  from TSBeta;
quit;
*19,115 obs. These are less than 24,698 obs. There are some firms with zero obs with no valid beta;


*Copy to NK and download;
data NK.TSBeta; set TSBeta; run;

*Manually transfer NK.TSBeta to BB0419US;
endrsubmit;





/*****************************************************/
/* Step 2. Shrink betas towards cross-sectional mean */
/*****************************************************/

data Beta;
  set BB0419US.TSBeta;
  beta = (0.6 * TSBeta) + (0.4 * 1);
  keep date gvkey beta;
run;



/*******************************/
/* Step 3. Form BAB Portfolios */
/*******************************/

/* Step 3.0. Form BAB Portfolios */

*For global stocks, we estimate breakpoints on entire universe unlike US where we use only NYSE stocks for breakpoints;
rsubmit;
proc upload data=Beta out=Beta; run;

proc sort data=Beta; by date beta; run;
proc rank data=Beta groups=10 out=Beta;
  by date;
  var beta;
  ranks Rank_beta;
run;
data Beta;
  set Beta;
  Rank_beta = Rank_beta + 1;
run;


*Assign all stocks in beta to one of the 10 portfolios;
proc sql;
  create table breakpoints as
  select distinct date, rank_beta, max(beta) as betabreakpoint
  from Beta
  group by date, rank_beta
  order by date, rank_beta;
quit;
proc transpose data=breakpoints out=breakpoints;
  by date;
  var betabreakpoint;
  id rank_beta;
run;
data breakpoints;
  set breakpoints;
  drop _Name_;
  rename _1 = beta1;
  rename _2 = beta2;
  rename _3 = beta3;
  rename _4 = beta4;
  rename _5 = beta5;
  rename _6 = beta6;
  rename _7 = beta7;
  rename _8 = beta8;
  rename _9 = beta9;
  rename _10 = beta10;
run;

proc sql;
  create table Beta2 as
  select distinct a.*, b.*
  from Beta as a, breakpoints as b
  where year(a.date)=year(b.date) and month(a.date)=month(b.date);
quit;
data Beta2;
  set Beta2;
  if beta <= beta1 then rank_beta = 1;
  if beta1 < beta <= beta2 then rank_beta = 2;
  if beta2 < beta <= beta3 then rank_beta = 3;
  if beta3 < beta <= beta4 then rank_beta = 4;
  if beta4 < beta <= beta5 then rank_beta = 5;
  if beta5 < beta <= beta6 then rank_beta = 6;
  if beta6 < beta <= beta7 then rank_beta = 7;
  if beta7 < beta <= beta8 then rank_beta = 8;
  if beta8 < beta <= beta9 then rank_beta = 9;
  if beta9 < beta then rank_beta = 10;
run;
data Beta2;
  set Beta2;
  keep date permno beta rank_beta;
run;


/* Step 3.1 Portfolio returns for the next month */
data Beta3;
  set Beta2;
  date1 = intnx("month",date,1,"E");
  format date1 date9.;
run;

**Getting price data from xpressfeed to calculate returns;
proc sql;
  create table DailyData_Temp as
  select distinct b.datadate format=date9., b.gvkey, upcase(b.loc) as loc, b.prccd, b.TRFD
  from Beta3 as a, xpf.g_secd as b
  where a.gvkey=b.gvkey and month(a.date1)=month(b.datadate) and year(a.date1)=year(b.datadate);
quit;

    ***Include foreign exchange rates;
proc sql;
  create table DailyData_Temp as
  select distinct a.*, b.rate as forex_rate,b.country
  from DailyData_Temp as a, exchange as b
  where a.datadate=b.date and a.loc=b.loc;
quit;

	***Calculating USD returns;
data DailyData_Temp;
	set DailyData_Temp;
	by gvkey;
	if first.gvkey=1 then ret_usd=.;
	else ret_usd=log(abs(prccd*forex_rate)/(abs(lag(prccd)*lag(forex_rate))*TRFD));
run;

proc sql;
  create table Beta3 as
  select distinct a.*, sum(b.ret_usd) as HoldRet label='Holding Period'
  from Beta3 as a, DailyData_Temp as b
  where a.gvkey=b.gvkey and month(a.date1)=month(b.datadate) and year(a.date1)=year(b.datadate)
  group by b.gvkey,month(b.datadate),year(b.datadate);
quit;
proc sql;
  create table Beta_RET as
  select distinct date, rank_beta, date1, mean(beta) as PortBeta_ExAnte, mean(HoldRet) as PortRet  
  from Beta3
  group by date, rank_beta, date1
  order by date, rank_beta, date1; 
 quit;


/* Step 3.2 BAB factor returns for the next month */

*Cross-sectional percentile ranks;
proc sort data=Beta; by date; run;
proc rank data=Beta fraction out=Beta4;
  by date;
  var beta;
  ranks PctlRank_Beta;
run;
proc sql;
  create table Beta4 as
  select distinct *, PctlRank_Beta-mean(PctlRank_Beta) as wt
  from Beta4
  group by date;
quit;

*Long low beta stocks, short high beta stocks;
data Beta4;
  set Beta4;
  if wt < 0 then long = 1;
  else long = 0;
run;

*Rescale the weights in long and short legs;
*Larger weight to stocks with lower betas in the long leg, larger weights to stocks with higher betas in the short leg;
proc sql;
  create table Beta4 as
  select distinct *, wt/sum(wt) as wt1
  from Beta4
  group by date, long;
quit;
proc sort data=Beta4; by date descending long beta; run;

*Next month returns;
data Beta4;
  set Beta4;
  date1 = intnx("month",date,1,"E");
  format date1 date9.;
run;
proc sql;
  create table Beta4 as
  select distinct a.*, b.HoldRet as HoldRet label='Holding Period'
  from Beta4 as a, Beta3 as b
  where a.gvkey=b.gvkey and month(a.date1)=month(b.date) and year(a.date1)=year(b.date);
quit;

***BAB portfolio return;
proc sql;
  create table BAB_RET as
  select distinct date, long, date1, sum(wt1*beta) as PortBeta_ExAnte, sum(wt1*HoldRet) as PortRet  
  from Beta4
  group by date, long
  order by date, long; 
 quit;




 *******************Done till here******************;


*Include risk-free rate;
proc sql;
   create table BAB_RET as
   select distinct a.*, b.rf
   from BAB_RET as a, ff.factors_monthly as b
   where intnx("month",a.date1,0,"E")=intnx("month",b.date,0,"E");
quit;

*BAB calculation;
data BAB_RET;
  set BAB_RET;
  ExPortret = PortRet - rf;
  DollarLongShort = 1 / PortBeta_ExAnte;
  LeveredPortRet = (1 / PortBeta_ExAnte) * (PortRet - rf);
  LeveredPort_ExAnteBeta = PortBeta_ExAnte /  PortBeta_ExAnte; *Both long and short portfolios have ex-ante beta of 1 by constuction;
run;
proc sql;
  create table BABFactor as
  select distinct a.date, 11 as rank_beta, a.date1, a.LeveredPort_ExAnteBeta - b.LeveredPort_ExAnteBeta as PortBeta_ExAnte,
                  a.LeveredPortRet - b.LeveredPortRet as PortRet label='BABFactor Port Ret'
  from (select distinct * from BAB_RET where long=1) as a, (select distinct * from BAB_RET where long=0) as b
  where a.date=b.date;
 quit;

*Combine Beta_Ret and BABFactor;
data BAB_RET_US;
  set Beta_RET BABFactor;
run;
proc sort data=BAB_RET_US; by date rank_beta; run;  

*Download required data locally;
proc download data=BAB_RET out=BAB_RET; run;
proc download data=BAB_RET_US out=BAB_RET_US; run;
endrsubmit;

*Copy to BB0419US;
data BB0419US.BAB_RET; set BAB_RET; run;
data BB0419US.BAB_RET_US; set BAB_RET_US; run;



/*********************************************/
/*** Step 4. Performance of BAB Portfolios ***/
/*********************************************/


/* Step 4.1 Dollars long and short */

proc sql;
  create table DollarLongShort as
  select distinct long, mean(DollarLongShort) as DollarLongShort
  from BAB_RET
  group by long;
quit;


/* Step 4.2 Data for performance statistics */

*Manually download FFC and FF5 factor return data in BB0419US;
PROC IMPORT OUT= WORK.FF5factors 
            DATAFILE= "G:\Data\Projects\Bettting Against Beta\SASData\2018-04-19\FF5factors.xlsx" 
            DBMS=XLSX REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
data FF5factors;
  set FF5factors;
  year = int(dateff/100);
  month = mod(dateff,100);
  MKTRF = MKTRF/100; *returns in fractions;
  SMB = SMB/100; *returns in fractions;
  HML = HML/100; *returns in fractions;
  RMW = RMW/100; *returns in fractions;
  CMA = CMA/100; *returns in fractions;
run;
data BB0419US.FF5factors; set FF5factors; run;


*Include factor return data;
proc sql;
  create table BAB_RET_US1 as
  select distinct a.*, a.PortRet-b.RF as ExPortRet, b.MKTRF, b.SMB, b.HML, b.UMD
  from (select distinct * from BB0419US.BAB_RET_US where rank_beta<=10) as a left join BB0419US.FFCFactors as b
  on intnx("month",a.date1,0,"E")=intnx("month",b.dateff,0,"E");

  create table BAB_RET_US1 as
  select distinct a.*, b.MKTRF as MKTRF5, b.SMB as SMB5, b.HML as HML5, b.RMW as RMW5, b.CMA as CMA5
  from BAB_RET_US1 as a left join BB0419US.FF5factors as b
  on year(a.date1)=b.year and month(a.date1)=b.month and a.rank_beta<=10;
quit;
proc sql;
  create table BAB_RET_US2 as
  select distinct a.*, a.PortRet as ExPortRet, b.MKTRF, b.SMB, b.HML, b.UMD
  from (select distinct * from BB0419US.BAB_RET_US where rank_beta=11) as a left join BB0419US.FFCFactors as b
  on intnx("month",a.date1,0,"E")=intnx("month",b.dateff,0,"E") and a.rank_beta=11;

  create table BAB_RET_US2 as
  select distinct a.*, b.MKTRF as MKTRF5, b.SMB as SMB5, b.HML as HML5, b.RMW as RMW5, b.CMA as CMA5
  from BAB_RET_US2 as a left join BB0419US.FF5factors as b
  on year(a.date1)=b.year and month(a.date1)=b.month and a.rank_beta=11;
quit;

data BAB_RET_US;
  set BAB_RET_US1 BAB_RET_US2;
run;
proc sql;
  drop table BAB_RET_US1, BAB_RET_US2;
quit;
proc sort data=BAB_RET_US; by date1 rank_beta; run;


/* Step 4.3 Performance stats of beta and BAB portfolios */

proc sort data=BAB_RET_US out=BAB_RET_US_PcRet; by rank_beta date1; run;
data BAB_RET_US_PcRet;
  set BAB_RET_US_PcRet;
  PortRet=PortRet*100;
  ExPortRet=ExPortRet*100;
  if PortRet > 0 then PortRet1 = 0; else PortRet1 = PortRet;
run;


**AVERAGE PERFORMANCE;
proc means data=BAB_RET_US_PcRet noprint;
  by rank_beta;
  var PortRet ExPortRet;
  output out=AvgPerf N=Nobs mean=Mean_PortRet Mean_ExPortRet;
quit;


**t-stats, AVERAGE PERFORMANCE;
proc means data=BAB_RET_US_PcRet noprint;
  by rank_beta;
  var PortRet ExPortRet;
  output out=TstatAvgPerf N=Nobs t=tstat_PortRet tstat_ExPortRet;
quit;


**VOLATILITY;
proc means data=BAB_RET_US_PcRet noprint;
  by rank_beta;
  var PortRet PortRet1;
  output out=Volatility std=Vol_PortRet Vol_PortRet1; *Vol_PortRet1 measures downside volatility;
quit;


*SKEWNESS, 1 PERCENTILE AND MINIMUM;
proc means data=BAB_RET_US_PcRet noprint;
  by rank_beta;
  var PortRet;
  output out=RiskMeasures skew=Skew_PortRet P1=Pctl1_PortRet min=Min_PortRet;
quit;


***COMBINE SUMMARY STATS;
proc sql;
  create table SumStats_Perf_Risk as
  select distinct a.rank_beta, a._Freq_ as N label='# of Holding Period months', 
         a.Mean_PortRet label='Holding Period Avg EW return', d.tstat_PortRet label='Holding Period t-Stat of Avg EW return',
         a.Mean_ExPortRet label='Holding Period Avg Excess EW return', d.tstat_ExPortRet label='Holding Period t-Stat of Excess Avg EW return',
         b.Vol_PortRet label='Volatility (Holding Period EW return)', b.Vol_PortRet1 label='Downside Volatility (Holding Period EW return)',
		 c.Skew_PortRet label='Skenewss (Holding Period EW return)', c.Pctl1_PortRet label='1 percentile (Holding Period EW return)', 
		 c.Min_PortRet label='Minimum (Holding Period EW return)'
  from AvgPerf as a, Volatility as b, RiskMeasures as c, TstatAvgPerf as d
  where a.rank_beta=b.rank_beta=c.rank_beta=d.rank_beta;
quit;


***SHARPE AND SORTINO RATIOS;
data SumStats_Perf_Risk;
  set SumStats_Perf_Risk;
  Sharpe = (Mean_ExPortRet/Vol_PortRet)*sqrt(12);
  Sortino = (Mean_ExPortRet/Vol_PortRet1)*sqrt(12);
  format Mean_PortRet 6.3 tstat_PortRet 6.3 Mean_ExPortRet tstat_ExPortRet 6.3 Vol_PortRet 6.3 Vol_PortRet1 6.3 
         Skew_PortRet 6.3 Pctl1_PortRet 6.3 Min_PortRet 6.3 Sharpe 6.3 Sortino 6.3;
run;
proc sql;
  drop table AvgPerf, Volatility, RiskMeasures, TstatAvgPerf, BAB_RET_US_PcRet;
quit;


/* Step 4.4 Alphas */

proc sort data=BAB_RET_US; by rank_beta date1; run;
proc reg data=BAB_RET_US noprint tableout outest=Alpha;
  by rank_beta;
  m1: model ExPortRet = MKTRF;
  m2: model ExPortRet = MKTRF SMB HML;
  m3: model ExPortRet = MKTRF SMB HML UMD;
  m4: model ExPortRet = MKTRF5 SMB5 HML5 RMW5 CMA5;
quit;
data Alpha;
  set Alpha;
  where _Type_ in ('PARMS','T');
  keep rank_beta _Model_ _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*100;
  rename Intercept=ExPortRet;
  rename _Type_ =Stat;
  rename _Model_=Model;
run;
data Alpha;
  set Alpha;
  if Stat='PARMS' then Stat='Alpha';
  if Stat='T' then Stat='Tstat';
  if Model='m1' then Model='1F';
  if Model='m2' then Model='3F';
  if Model='m3' then Model='4F';
  if Model='m4' then Model='5F';
run;


proc sort data=Alpha; by rank_beta Stat; run;
proc transpose data=Alpha out=Alpha;
  by rank_beta Stat;
  var ExPortRet;
  id Model;
run;
data Alpha;
  set Alpha;
  drop _Name_ _Label_;
  format _1F 6.3 _3F 6.3 _4F 6.3 _5F 6.3;
  rename _1F = CAPM;
  rename _3F = FF3;
  rename _4F = FF3C;
  rename _5F = FF5;
run;


*Arrange in proper format;
data Alpha;
  set Alpha;
  CAPMA = STRIP(PUT(CAPM, 8.3));
  if Stat='Tstat' then CAPMA=cats('(',CAPMA);
  if Stat='Tstat' then CAPMA=cats(CAPMA,')');

  FF3A = STRIP(PUT(FF3, 8.3));
  if Stat='Tstat' then FF3A=cats('(',FF3A);
  if Stat='Tstat' then FF3A=cats(FF3A,')');

  FF3CA = STRIP(PUT(FF3C, 8.3));
  if Stat='Tstat' then FF3CA=cats('(',FF3CA);
  if Stat='Tstat' then FF3CA=cats(FF3CA,')');

  FF5A = STRIP(PUT(FF5, 8.3));
  if Stat='Tstat' then FF5A=cats('(',FF5A);
  if Stat='Tstat' then FF5A=cats(FF5A,')');

  drop CAPM FF3 FF3C FF5;
  rename CAPMA = CAPM;
  rename FF3A = FF3;
  rename FF3CA = FF3C;
  rename FF5A = FF5;
run;


*Copy the full sample data and results;
data BB0419US.BAB_RET_US; set BAB_RET_US; run;
data BB0419US.BAB_Perf_US; set SumStats_Perf_Risk; run;
data BB0419US.BAB_Alpha_US; set Alpha; run;


*Clean working directory;
proc datasets lib=work nolist kill; quit;

