proc datasets lib=work kill nolist memtype=data;
quit;

/*libname BB1 'E:\Drive\Local Disk F\Betting against beta\Time variation in BAB'; run;*/
/**/
/**/
/*libname BB 'E:\Drive\Local Disk F\Betting against beta\BAB FP paper'; run;*/
/**/
/*libname LCT 'E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\LCT';run;*/
/**/
/*  proc import out=test*/
/*  datafile='E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\Reuters\Holdings.dta'*/
/*  dbms=DTA replace;*/
/*  run;*/
/**/
/**/
/*  proc import out=test1*/
/*  datafile='E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\CRSP\1.dta'*/
/*  dbms=DTA replace;*/
/*  run;*/
/**/
/*  proc import out=test2*/
/*  datafile='E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\CRSP\2.dta'*/
/*  dbms=DTA replace;*/
/*  run;*/
/**/
/*  proc import out=test3*/
/*  datafile='E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\CRSP\3.dta'*/
/*  dbms=DTA replace;*/
/*  run;*/
/**/
/*  proc import out=test4*/
/*  datafile='E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\CRSP\4.dta'*/
/*  dbms=DTA replace;*/
/*  run;*/

/*%let wrds=wrds-cloud.wharton.upenn.edu 4016; */
/*options comamid=TCP remote=WRDS;        */
/*signon username=adarshkp password=Iampetergriffin77;               */
/*                                         */
/*libname rwork slibref = work server = wrds; run;*/

/*rsubmit;*/
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname naa '/wrds/comp/sasdata/naa'; run; *refers to compa;

*Specify CRSP begin and end dates;
%let crspbegdate = '31Jan2006'd;
%let crspenddate = '30Sep2017'd; 


/*endrsubmit;*/

*Home Directory;

/*rsubmit;*/
libname home '/home/isb/adarshkp/';

/*data test;*/
/*set oldhome.test;*/
/*proc print data=test;*/
/*run;*/
/*endrsubmit;*/

************* AGGREGATE BETA ESTIMATION***************;

/*%let _sdtm=%sysfunc(datetime());*/

**instead of printing log, it saves log file at mentioned location;
/*proc printto log="E:\Drive\Local Disk F\Betting against beta\BAB FP paper\filename.log";*/
/*run;*/
/*rsubmit;*/

/*Month end days between two dates */

data month;
date=&crspbegdate;
do while (date<=&crspenddate);
    output;
    date=intnx('month', date, 1, 'E');
end;
format date date9.;
run;

data month;
	set month;
	date3m=intnx('month', date, -2, 'E');
	month=month(date);
	year=year(date);
	nrow=_N_;
	format date best12.;
run;


proc sql noprint;
        select count(*) into :row from month;
quit;
 
**Subset past 3month holdings data for each month;
	options symbolgen;
	%macro doit1;
	%do i=1 %to &row;

	proc sql noprint;
     select date3m into :date3m from month where nrow=&i;
    quit;

	proc sql noprint;
     select date into :date from month where nrow=&i;
    quit;

	proc sql noprint;
     select month into :mon from month where nrow=&i;
    quit;

	proc sql noprint;
     select year into :yr from month where nrow=&i;
    quit;

	data holdings1;
		set mf.Holdings;
		if &date3m <=report_dt and report_dt<=&date;
		keep crsp_portno report_dt nbr_shares market_val cusip permno ticker;
	run;

	data holdings1;
	set holdings1;
	date=&date;
	run;


** Add daily price & ret data for CRSP;
	proc sql;
		create table holdings as
		select distinct a.*,b.date as daily_date,b.prc,b.ret
		from holdings1 as a left join crspa.dsf as b
		on a.permno=b.permno and month(a.date)=month(b.date) and year(a.date)=year(b.date);
	quit;

	data holdings;
		set holdings;
		if missing(prc) then ME=market_val;
		else ME=abs(prc)*nbr_shares;
		if missing(permno)=1 then id=0;
		else id=1;
	run;

	**Estimating weights of each stock to calculate portfolio return and wt by assets;
proc sql;
	create table holdings as
	select distinct *, ME/sum(ME) as wt
	from holdings
	group by date, daily_date, crsp_portno;
quit;

proc sql;
	create table holdings as
	select distinct *, sum(ME) as TNA
	from holdings
	group by date, daily_date, crsp_portno;
quit;


**Estimating wt of equity;
proc sql;
	create table holdings as
	select distinct *, sum(wt) as sum_wt
	from holdings
	group by date, daily_date,crsp_portno,id;
quit;

**Remove funds with TNA < 15 Million and Wt of equity<80%;

data holdings;
	set holdings;
	if TNA>=15000000;
	if id=1 and sum_wt >= 0.8 or id=0 and sum_wt <= 0.2;
run;



**Rescaling portfolio weights;
proc sql;
	create table holdings as
	select distinct *, ME/sum(ME) as wt1
	from holdings
	group by date, daily_date, crsp_portno;
quit;

**Estimating portfolio returns;
proc sql;
	create table port_ret as
	select distinct date, daily_date, crsp_portno, sum(wt1*ret) as port_ret1
	from holdings
	group by date, daily_date,crsp_portno;
quit;


**Adding FF factors to the port_ret dataset;
proc sql;
	create table port_ret as
	select distinct a.*,b.*
	from port_ret as a left join ff.FACTORS_DAILY as b
	on a.daily_date=b.date;
quit;

proc sql;
	create table port_ret as
	select distinct a.*,b.mktrf,sum(b.mktrf,-b.rf) as Exmktret_1,b.date as dateh
	from port_ret as a left join ff.FACTORS_DAILY as b
	on intck('day',a.daily_date,b.date)=-1;
quit;

data port_ret;
	set port_ret;
	if missing(dateh)=1 then delete;
	ExFundret=sum(port_ret1,-rf);
	Exmktret=sum(mktrf,-rf);
	format daily_date date9. dateh date9.;
run;




proc sort data=port_ret; by crsp_portno dateh; run;

proc reg data=port_ret noprint tableout outest=Beta;
  by crsp_portno;
  model ExFundret = Exmktret Exmktret_1;
quit;

data beta;
	set beta;
	if _type_ in ('PARMS');
	Beta=sum(Exmktret_1, Exmktret);
	keep crsp_portno Beta Exmktret_1 Exmktret;
run;

/*proc download data=port_ret out=port_ret; run;*/

/*******************************************************************************************;*/

**Need monthly weights to estimate aggregate beta;

** Add Monthly price & ret data for CRSP;
proc sql;
	drop table holdings;
quit;
	proc sql;
		create table holdings as
		select distinct a.*,b.prc,b.ret
		from holdings1 as a left join crspa.msf as b
		on a.permno=b.permno and intck('month',a.date,b.date)=-1;
	quit;

	data holdings;
		set holdings;
		if missing(prc) then ME=market_val;
		else ME=abs(prc)*nbr_shares;
		if missing(permno)=1 then id=0;
		else id=1;
	run;

	**Estimating weights of each stock to calculate portfolio return and wt by assets;
proc sql;
	create table holdings as
	select distinct *, ME/sum(ME) as wt
	from holdings
	group by date, crsp_portno;
quit;

proc sql;
	create table holdings as
	select distinct *, sum(ME) as TNA
	from holdings
	group by date, crsp_portno;
quit;


**Estimating wt of equity;
proc sql;
	create table holdings as
	select distinct *, sum(wt) as sum_wt
	from holdings
	group by date, crsp_portno,id;
quit;

**Remove funds with TNA < 15 Million and Wt of equity<80%;

data holdings;
	set holdings;
	if TNA>=15000000;
	if id=1 and sum_wt >= 0.8 or id=0 and sum_wt <= 0.2;
run;

**Rescaling portfolio weights;
proc sql;
	create table holdings as
	select distinct date, crsp_portno, sum(ME) as TNA1
	from holdings
	group by date, crsp_portno;
quit;
proc sql;
	create table holdings as
	select distinct *, TNA1/sum(TNA1) as wt1
	from holdings
	group by date;
quit;

/******************;*/
/***Estimate aggregate beta ;*/
proc sql;
	create table test as
	select distinct b.date,a.beta*b.wt1 as beta_1
	from beta as a, holdings as b
	where a.crsp_portno=b.crsp_portno;
quit;

proc sql;
	create table test as
	select distinct date,sum(beta_1) as beta_port
	from test
	group by date;
quit;
 
/*proc download data=test out=test; run;*/
 
proc append data=test base=final; run;

%end;
%mend doit1;
%doit1

data home.final;
	set final;
	format date date9.;
run;

********************************************************************;
/*proc download data=home.final out=lct.final1; run;*/

/*endrsubmit;*/

/*signoff;*/

*re-enabling the log;
/*PROC PRINTTO PRINT=PRINT LOG=LOG ;*/
/*RUN;*/


/*%let _edtm=%sysfunc(datetime());*/
/*%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));*/
/*%put It took &_runtm second to run the program;*/
