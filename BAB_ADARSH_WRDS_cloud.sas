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

*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1928'd;
%let crspenddate = '31Mar2012'd; 

endrsubmit;


**Squeezing the file with algo from internet;

/*filename adarsh "E:\Drive\Local Disk F\Betting against beta\BAB FP paper";*/
/*%include adarsh(squeeze.txt);*/
/*%squeeze(dsnin=bb.dailyprc,dsnout=dailyprc)*/
/*%squeeze(dsnin=bb.perms,dsnout=perms)*/



rsubmit;
/*proc upload data=dailyprc out=dailyprc; run;*/
proc upload data=bb.perms out=perms; run;


proc sql noprint;
        select count(*) into :row from perms;
quit;

**Creating shrcd dataset;
proc sort data=crspa.stocknames nodupkey out=shrcd;by permno; run;

data shrcd;
	set shrcd;
	keep permno shrcd; 
run;


**Adding shrcd with price dataset;

proc sql;
	create table dailyprc as
	select distinct a.permno, a.date, a.ret, a.prc, a.shrout,b.shrcd
	from crspa.dsf as a, shrcd as b
	where &crspbegdate<=a.date and a.date<=&crspenddate and a.permno=b.permno
	order by a.permno,a.date;
quit;

**Adding 'Vwretd' with price dataset;
proc sql;
	create table dailyprc as
	select distinct a.*,b.vwretd
	from dailyprc as a, crspa.dsi as b
	where a.date=b.date;
quit;

data dailyprc;
	set dailyprc;
	if SHRCD in (10,11); *SELECT ONLY COMMON STOCKS;
	if missing(PRC)=0 and missing(SHROUT)=0 and SHROUT>0 then ME=(abs(PRC)*SHROUT*1000)/1000000; *SHROUT SHOULD BE GREATER THAN 0 TO MAKE SENSE;   
	label ME='Market Cap ($M)';
	if missing(ME)=1 then delete;
	if missing(ret)=1 then delete;
	keep permno date shrcd ret ME vwretd;
run;

data dailyprc;
	set dailyprc;
	nrow=_N_;
run;


**estimating three day returns;
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

%let _sdtm=%sysfunc(datetime());

options symbolgen;
%macro doit1;
%do i=2 %to 2;

	
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


	proc sql;
		create table month_end as
		select distinct *
		from dailyprc1
		group by month(date),year(date)
		having date=max(date);
	quit;

	data month_end;
		set month_end;
		vol_date=intnx('day',date,-365); *past 1-yr data;
		corr_date=intnx('day',date,-1825); *past 5-yr data;
		format vol_date date9. corr_date date9.;
	run;

	**volatility;
	proc sql;
		create table volatility as
		select distinct a.*,b.date as dateh
		from dailyprc1 as a, month_end as b
		where b.vol_date<=a.date<b.date
		group by dateh
		having count(distinct Date)>=120;*Require good return for 120 trading days;
	quit;

	proc sort data=volatility; by dateh date; run;

	**correlation;
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

	**Estimating volatility and correlations;
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
	%doit2;

	**Appending to the final dataset;
/*	proc append data=stats base=final; run;*/

/*	proc sql;*/
/*		drop table dailyprc1, perms1, month_end, volatility, correlation, volatility1, correlation1, stats;*/
/*	quit;*/

	%let _edtm=%sysfunc(datetime());
	%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));
	%put It took &_runtm second to run the program;

%end;
%mend doit1;
%doit1


proc download data=stats out=final; run;
endrsubmit;



