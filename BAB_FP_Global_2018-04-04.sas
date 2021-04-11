proc datasets lib=work kill nolist memtype=data;
quit;

libname BB 'E:\Drive\Local Disk F\Betting against beta\BAB FP paper'; run;

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
libname ash '/home/isb/adarshkp/'; run;
endrsubmit;





						/********************************************************************/
						/*** STEP-1: ESTIMATING EX-ANTE BETAS USING CRSP STOCK-LEVEL DATA ***/
						/********************************************************************/


/* Step 1.1. Extract CRSP Daily stock prices */
/* And sorted by permno, date */
data dailyprc;
	set dailyprc;
	keep shrcd; 
	if missing(PRC)=0 and missing(SHROUT)=0 and SHROUT>0 then ME=(abs(PRC)*SHROUT*1000)/1000000;  
	label ME='Market Cap ($M)';
	if missing(ME)=1 then delete;
	if missing(ret)=1 then delete;
	keep permno date prc shrout ret vwretd me;
run;



data dailyprc;
	set dailyprc;
	nrow=_N_;
	drop shrout prc;
run;


/* Step 1.2. Estimating three day returns */
options symbolgen;
%macro doit;
%do i=1 %to 2;

proc sql;
	create table dailyprc as
	select a.*,b.ret as ret_&i
	from dailyprc as a left join dailyprc as b
	on a.PERMNO=b.PERMNO and a.nrow=b.nrow+&i;
quit;

%end;
%mend doit;
%doit

data dailyprc;
	set dailyprc;
	ret3d=sum(ret,ret_1,ret_2);
	drop ret_1 ret_2;
run;

proc sort data=dailyprc; by permno date; run;


/* Step 1.3. Creating unique permno dataset and macros to calculate stock-level ex-ante betas;*/
proc sort data=dailyprc nodupkey out=perms; by permno; run;

data perms;
	set perms;
	nrow=_N_;
	keep permno nrow;
run;

proc sql noprint;
        select count(*) into :row from perms;
quit;

options symbolgen;
%macro doit1;
%do i=1 %to &row;

	
	data perms1;
		set perms;
		if nrow=&i;
	run;

	proc sql noprint;
     select permno into :perm from perms1;
    quit;

	*creating a subset by permno using Hash object;
	data dailyprc1;
	    if _n_=1 then do;
	        declare hash h(dataset:'perms1');
	        h.defineKey('permno');
	        h.defineData('permno');
			call missing(permno); 
	        h.defineDone();
			end;
	    set dailyprc;
    	if h.find() = 0 then output;
	run;

	*Subsetting month end data;
	proc sql;
		create table month_end as
		select distinct *
		from dailyprc1
		group by month(date),year(date)
		having date=max(date);
	quit;

	*Creating past 1-yr and 5-yr dates;
	data month_end;
		set month_end;
		vol_date=intnx('day',date,-365); *past 1-yr data;
		corr_date=intnx('day',date,-1825); *past 5-yr data;
		format vol_date date9. corr_date date9.;
	run;

	*Subset data for volatility;
	proc sql;
		create table volatility as
		select distinct a.*,b.date as dateh
		from dailyprc1 as a, month_end as b
		where b.vol_date<=a.date<b.date
		group by dateh
		having count(distinct Date)>=120;*Require good return for 120 trading days;
	quit;

	proc sort data=volatility; by dateh date; run;

	*Subset data for correlation;
	proc sql;
		create table correlation as
		select distinct a.*,b.date as dateh
		from dailyprc1 as a, month_end as b
		where b.corr_date<=a.date<b.date
		group by dateh
		having count(distinct Date)>=750;*Require good return for 750 trading days;
	quit;

	proc sort data=correlation; by dateh date; run;

	proc sql noprint;
	        select count(*) into :row1 from volatility;
			select count(*) into :row2 from correlation;
	quit;

	%if &row1>0 and &row2>0 %then %doit2;

	*Estimating volatility and correlations;
	options symbolgen;
	%macro doit2;
	%do;

	proc sql;
		create table volatility1 as
		select distinct dateh,std(ret) as sigperm, std(VWRETD) as sigmkt
		from volatility 
		group by dateh;
	quit;

	proc corr data = correlation noprint out=correlation1;
	by dateh;
	var ret VWRETD;
	run;

	data correlation1;
		set correlation1;
		if _TYPE_='CORR' and _NAME_='RET';
		keep dateh VWRETD;
	run; 

	data stats;
		merge volatility1 correlation1;
	run;

	data stats;
		set stats;
		permno=&perm;
		Beta=(VWRETD*sigperm)/sigmkt;
		if missing(Beta)=1 then delete;
	run;

	%end;
	%mend doit2;
	%doit2

	**Appending to the bb.final dataset;
	proc append data=stats base=bb.final; run;

	proc sql;
		drop table dailyprc1, perms1, month_end, volatility, correlation, volatility1, correlation1, stats;
	quit;
%end;
%mend doit1;
%doit1

**deleting duplicate entries;
proc sort data=bb.final nodupkey out=bb.final1; by permno dateh; run;	

data bb.final;
	set bb.final;
	rename VWRETD = correlation;
	label VWRETD = correlation;
run;

proc sort data=bb.final; by dateh permno; run;	

/* Step 1.4. bb.final Ex-ante beta calculation */
data bb.final;
	set bb.final;
	beta=(0.6*beta)+(0.4);  
	daten=intnx("month",dateh,1,"E");
	format daten date9.;
run;

/* Step 1.5. Adding returns to betas dataset */
proc sql;
  create table bb.final1 as
  select distinct a.*, b.Ret
  from bb.final(keep=permno dateh beta daten) as a, bb.monthlyret as b
  where a.permno=b.permno and month(a.daten)=month(b.date) and year(a.daten)=year(b.date)
  order by a.dateh,a.permno;
quit;

data bb.final1;
	set bb.final1;
	if missing(ret)=1 then delete;
run;

/* Step 1.6. Adding ff & liquidity factors to betas dataset */
proc sql;
  create table bb.final1 as
  select distinct a.*, b.*
  from bb.final1 as a left join bb.ff_monthly as b
  on month(a.daten)=month(b.dateff) and year(a.daten)=year(b.dateff)
  order by a.dateh,a.permno;
quit;

**Forming portfolios;

proc sort data=bb.final1; by daten beta; run;

data bb.final1;
	set bb.final1;
	exmkt=sum(mktrf,-rf);
run;



								/*******************************************/
								/***STEP-2: CONSTRUCTING NYSE BREAKPOINTS***/
								/*******************************************/


/*Step 2.1. Subsetting NYSE stocks*/
/*data nyse;*/
/*	set crspa.DSFHDR;*/
/*	if HEXCD=1;*/
/*run;	*/

*Taking only NYSE stocks from initial dataset;
proc sql;
  create table final1a as
  select distinct a.*
  from bb.final1 as a, bb.nyse as b
  where a.permno=b.permno
  order by a.daten,a.beta;
quit;

data final1a;
	set final1a;
	keep daten Beta;
run;

proc sort data=final1a; by daten beta; run;

/*Step 2.2. Estimating Beta Ranks */
proc rank data=final1a groups=10 out=nyse_betas;
  by daten;
  var beta;
  ranks Rank_beta;
run;

data nyse_betas;
  set nyse_betas;
  Rank_beta = Rank_beta + 1;
run;

/*Step 2.3. NYSE Breakpoint betas */
proc sql;
	create table nyse_breakpoints as
	select distinct *
	from nyse_betas
	group by daten, rank_beta
	having beta=max(beta)
	order by daten, rank_beta;
quit;

data nyse_breakpoints;
	set nyse_breakpoints;
	Beta_1=lag(Beta);
run;

*Stocks with beta between -100 and p10 will be in PF1;
*Stocks with beta above p90 will be in PF10;
data nyse_breakpoints;
	set nyse_breakpoints;
	if Rank_beta=1 then Beta_1=-100; 
	if Rank_beta=10 then beta=1000; 
run;

/*Step 2.4. Applying NYSE breakpoints to final stock data */
data bb.final1;
  set bb.final1;
  drop Rank_beta;
  exret=sum(ret,-rf);
run;

**assigning ranks to final1;
proc sql;
	create table final1 as
	select distinct a.*,b.Rank_beta
	from bb.final1 as a left join nyse_breakpoints as b
	on month(a.daten)=month(b.daten) and year(a.daten)=year(b.daten) and b.beta_1<a.beta<=b.beta
	order by a.daten,b.rank_beta,a.beta;
quit;


/*Step 2.5. Estimating BAB Ranks */
proc sort data=final1; by daten beta; run;

proc rank data=final1 groups=2 out=final1;
  by daten;
  var beta;
  ranks Rank_bab;
run;

data final1;
  set final1;
  Rank_bab = Rank_bab + 1;
run;



							/************************************/
							/***  STEP 3: TABLE-3 RESULTS     ***/
							/************************************/


/* Step 3.1. Estimating Excess portfolio returns for all portfolios */
proc sort data=final1; by Rank_beta permno; run;

*Estimating portfolio returns;
proc sql;
	create table final_test as
	select daten, Rank_beta, sum(ret)/count(distinct permno) as rp_new
	from final1
	group by daten, Rank_beta
	order by daten, Rank_beta;
quit;

*Appending FF factors to final_test;
proc sql;
	create table final_test as
	select distinct a.*,b.*
	from final_test as a, bb.ff_monthly as b
	where month(a.daten)=month(b.dateff) and year(a.daten)=year(b.dateff);
quit;

data final_test;
  set final_test;
  Exret=sum(rp_new,-rf);
  Exmkt=sum(mktrf,-rf);
run;

proc sort data=final_test; by Rank_beta; run;

proc means data=final_test noprint;
  by Rank_beta;
  var Exret;
  output out=exret mean=Mean_exret t=t;
quit;

*converting long to wide form;
 data exret;
  set exret;
  Mean_exret=Mean_exret*100;
  keep Rank_beta Mean_exret t;
run;

proc transpose data=exret out=exret prefix=Rank;
/*    by daten; *only one year per column;*/
    id Rank_beta; *will be split to columns;
/*    var Mean_ret; *by this variable;*/
run;

/* Step 3.2. CAPM alpha */
proc sort data=final_test; by Rank_beta daten; run;

proc reg data=final_test noprint tableout outest=CAPM_Alpha;
  by Rank_beta;
  model ExRet = Exmkt;
quit;

data CAPM_Alpha;
  set CAPM_Alpha;
  where _Type_ in ('PARMS','T');
  keep Rank_beta _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*100;
run;

proc sort data=CAPM_Alpha; by _type_ Rank_beta; run;
proc transpose data=CAPM_Alpha out=CAPM_Alpha prefix=Rank;
    by _type_; *only one year per column;
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data CAPM_Alpha;
  set CAPM_Alpha;
  drop _label_ _type_;
run;

/* Step 3.3. Three-factor alpha */
proc sort data=final_test; by Rank_beta daten; run;

proc reg data=final_test noprint tableout outest=TF_Alpha;
  by Rank_beta;
  model ExRet = Exmkt SMB HML;
quit;

data TF_Alpha;
  set TF_Alpha;
  where _Type_ in ('PARMS','T');
  keep Rank_beta _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*100;
run;

proc sort data=TF_Alpha; by _type_ Rank_beta; run;
proc transpose data=TF_Alpha out=TF_Alpha prefix=Rank;
    by _type_; *only one year per column;
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data TF_Alpha;
  set TF_Alpha;
  drop _label_ _type_;
run;

/* Step 3.4. Four-factor alpha */
proc sort data=final_test ; by Rank_beta daten; run;

proc reg data=final_test noprint tableout outest=FF_Alpha;
  by Rank_beta;
  model ExRet = Exmkt SMB HML UMD;
quit;

data FF_Alpha;
  set FF_Alpha;
  where _Type_ in ('PARMS','T');
  keep Rank_beta _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*100;
run;

proc sort data=FF_Alpha; by _type_ Rank_beta; run;
proc transpose data=FF_Alpha out=FF_Alpha prefix=Rank;
    by _type_; *only one year per column;
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data FF_Alpha;
  set FF_Alpha;
  drop _label_ _type_;
run;

/* Step 3.5. Five-factor alpha */
proc sort data=final_test; by Rank_beta daten; run;

proc reg data=final_test noprint tableout outest=FiF_Alpha;
  by Rank_beta;
  model ExRet = Exmkt SMB HML UMD liquidity;
quit;

data FiF_Alpha;
  set FiF_Alpha;
  where _Type_ in ('PARMS','T');
  keep Rank_beta _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*100;
run;

proc sort data=FiF_Alpha; by _type_ Rank_beta; run;
proc transpose data=FiF_Alpha out=FiF_Alpha prefix=Rank;
    by _type_; *only one year per column;
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data FiF_Alpha;
  set FiF_Alpha;
  drop _label_ _type_;
run;


/* Step 3.6. Beta(ex-ante) */
proc sql;
	create table beta_exante as
	select distinct Rank_beta, daten, mean(beta) as bp
	from final1
	group by Rank_beta,daten;
quit;

proc sql;
	create table beta_exante as
	select distinct Rank_beta,mean(bp) as bp
	from beta_exante
	group by Rank_beta;
quit;

proc transpose data=beta_exante out=beta_exante prefix=Rank;
/*    by _type_; *only one year per column;*/
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;


/* Step 3.7. Beta(realized) */
proc sort data=final_test; by Rank_beta daten; run;

proc reg data=final_test noprint tableout outest=CAPM_Beta;
  by Rank_beta;
  model ExRet = Exmkt;
quit;

data CAPM_beta;
  set CAPM_beta;
  where _Type_ in ('PARMS');
  keep Rank_beta _Type_ Exmkt;
run;

proc transpose data=CAPM_beta out=CAPM_beta prefix=Rank;
/*    by _type_; *only one year per column;*/
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

/* Step 3.8. Volatility */
proc sort data=final_test; by Rank_beta daten; run;

proc means data=final_test noprint;
  by Rank_beta;
  var Exret;
  output out=vol std=vol;
quit;

data vol;
  set vol;
  vol=vol*sqrt(12)*100;
  keep Rank_beta vol;
run;

proc transpose data=vol out=vol prefix=Rank;
/*    by _type_; *only one year per column;*/
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data vol;
  set vol;
  drop _label_;
run;

/* Step 3.9. Sharpe ratio */
proc means data=final_test noprint;
  by Rank_beta;
  var Exret;
  output out=SR mean=mean std=std;
quit;

data SR;
  set SR;
  SR=(mean*12)/(std*sqrt(12));
  keep Rank_beta SR;
run;

proc transpose data=SR out=SR prefix=Rank;
/*    by _type_; *only one year per column;*/
    id Rank_beta  ; *will be split to columns;
/*    var intercept; *by this variable;*/
run;

data SR;
  set SR;
  drop _label_;
run;

proc append data=exret base=Stats; run;
proc append data=capm_alpha base=Stats; run;
proc append data=tf_alpha base=Stats; run;
proc append data=ff_alpha base=Stats; run;
proc append data=fif_alpha base=Stats; run;
proc append data=beta_exante base=Stats; run;
proc append data=capm_beta base=Stats; run;
proc append data=vol base=Stats; run;
proc append data=SR base=Stats; run;

data bb.stats;
	set stats;
run;
