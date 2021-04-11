libname BB0419 'G:\Data\Projects\Bettting Against Beta\SASData\2018-04-19'; run;

%let wrds=wrds-cloud.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;        
signon username=_prompt_;               
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname naa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname FF ' /wrds/ff/sasdata'; run;
libname ash '/home/isb/adarshkp/BAB_US'; run; *temporary directory on WRDS;
endrsubmit;


rsubmit;


/********************************************************************/
/*** STEP-1: ESTIMATING EX-ANTE BETAS USING CRSP STOCK-LEVEL DATA ***/
/********************************************************************/


/* Step 1.1. Select common stocks (shrcd = 10,11) from CRSP */
proc sql;
  create table CRSPStocks as
  select distinct a.permno
  from crspa.dsf as a, crspa.dsenames as b
  where a.permno=b.permno and b.shrcd in (10,11);
quit;
*24,698 permnos;



/* Step 1.2. Creating unique permno dataset */
proc sort data=CRSPStocks nodupkey; by permno; run;
data CRSPStocks;
  format permnoid permno;
  set CRSPStocks;
  permnoid = _N_; 
run;
data NK.CRSPStocks; set CRSPStocks; run;

*totpermno contains total number of permnos;
proc sql noprint;
  select distinct count(*) into :totpermno from CRSPStocks;  
quit;
%put &totpermno;



/* Step 1.3. Last trading day by month and year */
proc sql;
  create table LastTradingDay as
  select distinct year(date) as year, month(date) as month, day(date) as LastTradingDay
  from crspa.dsf
  group by year(date), month(date)
  having date=max(date);
quit;
data NK.LastTradingDay; set LastTradingDay; run;



/* Step 1.4. Calculate stock-level ex-ante vols and correlations */

* Direct log of macro to a different directory;
proc printto log="G:\Data\Projects\Bettting Against Beta\SASLog\BAB.log"; run;

options mprint symbolgen;
%macro TSBeta;

%do i=1 %to &totpermno;

    ***Select one permno from perms by i counter;
    proc sql;
	  create table Permno_Temp as
	  select distinct *
	  from CRSPStocks
	  where permnoid=&i;
	quit;
    
	proc sql noprint;
      select permno into :perm from Permno_Temp;
    quit;

    
	***Create a temporary dataset containing daily return data for permno = &perm;
	proc sql;
	  create table DailyData_Temp as
	  select distinct date format=date9., permno, ret
	  from crspa.dsf
	  where permno=&perm and missing(ret)=0;
	quit;
	
	*Include market returns;
	proc sql;
  	  create table DailyData_Temp as
  	  select distinct a.*, b.vwretd
      from DailyData_Temp as a, (select distinct date, vwretd from crspa.dsi) as b
      where a.date=b.date;
    quit;
    
    *Estimating three day returns;
    proc sort data=DailyData_Temp; by permno date; run;
    data DailyData_Temp;
      set DailyData_Temp;
      by permno date;
      lag1ret = lag(ret);
      lag1vwretd = lag(vwretd);
      if first.permno=1 then lag1ret=.;
      if first.permno=1 then lag1vwretd=.;
    run;

    data DailyData_Temp;
      set DailyData_Temp;
      by permno date;
      lag2ret = lag(lag1ret);
      lag2vwretd = lag(lag1vwretd);
      if first.permno=1 then lag2ret=.;
      if first.permno=1 then lag2vwretd=.;
    run;
    
    data DailyData_Temp;
      format date permno lret lvwretd lret3d lvwretd3d;
      set DailyData_Temp;
      if missing(lag1ret)=1 or missing(lag2ret)=1 then delete;

      lret3d = sum(log(1+ret), log(1+lag1ret), log(1+lag2ret));
      lvwretd3d = sum(log(1+vwretd), log(1+lag1vwretd), log(1+lag2vwretd));

      *Log daily returns;
      lret = log(1+ret);
      lvwretd = log(1+vwretd);

      *Keep relevant variables;
      keep date permno lret lvwretd lret3d lvwretd3d;
    run;

    
	***Subsetting month end data;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct date, permno
	  from DailyData_Temp
	  group by month(date),year(date)
	  having date=max(date);
	quit;
	
     
	*Last observsation may not be month end dat, for instance, if the data ends on 15Aug2017, then the last row will be 15Aug2017. Remove such observsations;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct a.*
	  from MonthEnd_Temp as a, LastTradingDay as b
	  where year(a.date)=b.year and month(a.date)=b.month and day(a.date)=b.LastTradingDay;
	quit;
    
	***Create past 1-yr and 5-yr dates;
	data MonthEnd_Temp;
	  set MonthEnd_Temp;
	  vol_date=intnx('day',date,-365); *past 1-yr date;
	  corr_date=intnx('day',date,-1825); *past 5-yr date;
	  format vol_date date9. corr_date date9.;
	run;
    
	***Subset data for volatility;
	proc sql;
	  create table VolData_Temp as
	  select distinct a.date, a.permno, a.vol_date, b.date as date1, b.lret, b.lvwretd
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.permno=b.permno and a.vol_date<b.date<=a.date
	  group by a.date, a.permno
	  having count(distinct b.date)>=120; *Require good return for 120 trading days;
	quit;

	***Subset data for correlation;
	proc sql;
      create table CorrData_Temp as
	  select distinct a.date, a.permno, a.corr_date, b.date as date1, b.lret3d, b.lvwretd3d
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.permno=b.permno and a.corr_date<b.date<=a.date
	  group by a.date, a.permno
	  having count(distinct b.date)>=750; *Require good return for 120 trading days;
	quit;
    
    ***Estimate trailing stock and market volatilities;
	proc sql;
      create table Vol_&i as
	  select distinct date, permno, std(lret) as stockvol, std(lvwretd) as mktvol
      from VolData_Temp
	  group by date, permno;
	quit;
 
    ***Estimate trailing trailing 3day return correlation;
	proc corr data=CorrData_Temp noprint out=Corr_&i;
	  by date permno;
	  var lret3d lvwretd3d;
	run;
	proc sql;
	  create table Corr_&i as
	  select distinct date, permno, lret3d as corr
	  from Corr_&i
	  where _TYPE_='CORR' and _NAME_='lvwretd3d';
	quit;

   
	***Merge data;
	proc sql;
	  create table TSBeta_&i as
	  select distinct a.date, a.permno, b.stockvol, b.mktvol, c.corr
	  from MonthEnd_Temp as a, Vol_&i as b, Corr_&i as c
	  where a.date=b.date=c.date and a.permno=b.permno=c.permno and missing(b.stockvol)=0 and missing(c.corr)=0;
	quit;
     
	***Copy TSBeta_&i to NK;
	data NK.TSBeta_&i; set TSBeta_&i; run;
    
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
  drop table NK.CRSPStocks, NK.LastTradingDay;
quit;



/* Step 1.5. Create a consolidated dataset with ex-ante vols and correlations */

*Run a PROC CONTENTS to create a SAS data set with the names of the SAS data sets in the SAS data  library;
proc contents data=NK._all_ out=NK.NKcont(keep=memname) noprint; run;


*Eliminate any duplicate names of the SAS data set names stored in the SAS data set;
proc sort data=NK.NKcont nodupkey; by memname; run;
*24,698 obs;


*Run a DATA _NULL_ step to create 2 macro variables: one with the names of each SAS data set and 
the other with the final count of the number of SAS data sets;
data _null_;
  set NK.NKcont end=last;
  by memname;
  i+1;
  call symputx('name'||trim(left(put(i,8.))),memname);
  if last then call symputx('count',i);
run;

* Direct log of macro to a different directory;
proc printto log="G:\Data\Projects\Bettting Against Beta\SASLog\BAB.log"; run;

*Macro containing the PROC APPEND that executes for each SAS data set you want to concatenate together to create 1 SAS data set;
%macro combdsets;
  %do i=1 %to &count;
    proc append base=TSBeta data=NK.&&name&i force; run;
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
  select distinct permno
  from TSBeta;
quit;
*2675 obs. These are less than 2999 obs. There are some firms with zero obs with no valid beta;


*Copy to NK and download;
data NK.TSBeta; set TSBeta; run;

*Manually transfer NK.TSBeta to BB0419;
endrsubmit;





/*****************************************************/
/* Step 2. Shrink betas towards cross-sectional mean */
/*****************************************************/

data ash.TSBeta;
  set ash.TSBeta;
  TSbeta = corr * (stockvol / mktvol);
run;

rsubmit;
data Beta;
  set ash.TSBeta;
  beta = (0.6 * TSBeta) + (0.4 * 1);
  keep date permno beta;
run;



/*******************************************/
/* Step 3. BAB Portfolio Performance Stats */
/*******************************************/

/* Step 3.0. Form BAB Portfolios */

*Extract exchange codes;
/*proc upload data=Beta out=Beta; run;*/

proc sql;
  create table Beta1 as
  select distinct a.*, b.exchcd
  from Beta as a, crspa.msenames as b
  where a.permno=b.permno and b.namedt<=a.date<=b.nameendt;
quit;

*Take only NYSE listed stocks and sort by beta within a month;
data Beta1_nyse;
  set Beta1;
  if exchcd=1;
run;
proc sort data=Beta1_nyse; by date beta; run;
proc rank data=Beta1_nyse groups=10 out=Beta1_nyse;
  by date;
  var beta;
  ranks Rank_beta;
run;
data Beta1_nyse;
  set Beta1_nyse;
  Rank_beta = Rank_beta + 1;
run;


*Assign all stocks in beta to one of the 10 portfolios;
proc sql;
  create table nysebeta_breakpoints as
  select distinct date, rank_beta, max(beta) as betabreakpoint
  from Beta1_nyse
  group by date, rank_beta
  order by date, rank_beta;
quit;
proc transpose data=nysebeta_breakpoints out=nysebeta_breakpoints;
  by date;
  var betabreakpoint;
  id rank_beta;
run;
data nysebeta_breakpoints;
  set nysebeta_breakpoints;
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
  from Beta as a, nysebeta_breakpoints as b
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
proc sql;
  create table Beta3 as
  select distinct a.*, b.Ret as HoldRet label='Holding Period'
  from Beta3 as a, crspa.msf as b
  where a.permno=b.permno and intnx("month",a.date1,0,"E")=intnx("month",b.date,0,"E") and missing(b.Ret)=0;
quit;

proc sql;
  create table Beta_RET as
  select distinct date, rank_beta, date1, mean(beta) as PortBeta_ExAnte, mean(HoldRet) as PortRet  
  from Beta3
  group by date, rank_beta, date1
  order by date, rank_beta, date1; 
 quit;

rsubmit;
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
  select distinct a.*, b.Ret as HoldRet label='Holding Period'
  from Beta4 as a, crspa.msf as b
  where a.permno=b.permno and intnx("month",a.date1,0,"E")=intnx("month",b.date,0,"E") and missing(b.Ret)=0;
quit;

***BAB portfolio return;
proc sql;
  create table BAB_RET as
  select distinct date, long, date1, sum(wt1*beta) as PortBeta_ExAnte, sum(wt1*HoldRet) as PortRet  
  from Beta4
  group by date, long
  order by date, long; 
 quit;

*Include risk-free rate;
proc sql;
   create table BAB_RET as
   select distinct a.*, b.rf
   from ash.BAB_RET as a, ff.factors_monthly as b
   where intnx("month",a.date1,0,"E")=intnx("month",b.date,0,"E");
quit;

*BAB calculation;
data ash.BAB_RET;
  set BAB_RET;
  ExPortret = PortRet - rf;
  DollarLongShort = 1 / PortBeta_ExAnte;
  LeveredPortRet = (1 / PortBeta_ExAnte) * (PortRet - rf);
run;
proc sql;
  create table ash.BABFactor as
  select distinct a.date, a.LeveredPortRet - b.LeveredPortRet as PortRet label='BABFactor Port Ret'
  from (select distinct date, LeveredPortRet from ash.BAB_RET where long=1) as a, (select distinct date, LeveredPortRet from ash.BAB_RET where long=0) as b
  where a.date=b.date;
 quit;

*Download required data locally;
proc download data=Beta_RET out=Beta_RET; run;
proc download data=BAB_RET out=BAB_RET; run;
proc download data=BABFactor out=BABFactor; run;
endrsubmit;



***Quick stat check;
proc sql;
  create table DollarLongShort as
  select distinct long, mean(DollarLongShort) as DollarLongShort
  from BAB_RET
  group by long;
quit;

proc sql;
  create table PerfStats as
  select distinct mean(PortRet) as Mean, std(PortRet)*sqrt(12) as AnnVol, (mean(PortRet)*12) / (std(PortRet)*sqrt(12)) as Sharpe
  from BABFactor;
quit;

***Complete the code to generate Table 1 of BAB paper;
proc sql;
  create table PerfStats2 as
  select distinct rank_beta,mean(PortRet) as Mean, std(PortRet)*sqrt(12) as AnnVol, (mean(PortRet)*12) / (std(PortRet)*sqrt(12)) as Sharpe, mean(PortBeta_ExAnte) as beta
  from Beta_RET
  group by rank_beta;
quit;
