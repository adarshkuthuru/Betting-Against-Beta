proc datasets lib=work kill nolist memtype=data;
quit;

libname BB1 'E:\Drive\Local Disk F\Betting against beta\Time variation in BAB'; run;

libname BB 'E:\Drive\Local Disk F\Betting against beta\BAB FP paper'; run;

/*proc sql;*/
/*	create table test as*/
/*	select distinct a.*,b.beta*/
/*	from bb1.Tfnfund_holdings_qtrly as a left join bb.final as b*/
/*	on a.permno=b.permno and year(a.qdate)=year(b.dateh) and month(a.qdate)=month(b.dateh);*/
/*quit;*/
/**/
/*data test;*/
/*	set test;*/
/*	if missing(beta)=0;*/
/*run;*/
/**/
/*proc sql;*/
/*	create table test as*/
/*	select distinct *,wt_rdate/sum(wt_rdate) as wt*/
/*	from test*/
/*	group by WFICN, qdate;*/
/*quit;*/
/**/
/*proc sql;*/
/*	create table test1 as*/
/*	select distinct WFICN, qdate, sum(beta*wt_rdate) as Beta_port*/
/*	from test*/
/*	group by WFICN, qdate;*/
/*quit;*/


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
%let crspbegdate = '01Jan1996'd;
%let crspenddate = '30Sep2017'd; 
endrsubmit;


************* AGGREGATE BETA ESTIMATION***************;
rsubmit;

data monthly;
	set mf.MONTHLY_TNA_RET_NAV;
	if CALDT>='01Jan1996'd and CALDT<='30Sep2017'd;
	if missing(MTNA)=1 or MTNA=-99.0 then delete;
run;

data daily;
	set mf.DAILY_NAV_RET;
	if CALDT>='01Jan1996'd and CALDT<='30Sep2017'd;
	if missing(DNAV)=1 or DNAV=-99.0 then delete;
run;

proc sql;
	create table test as
	select distinct a.*,b.MTNA
	from daily as a, monthly as b
	where a.CRSP_FUNDNO=b.CRSP_FUNDNO and intck('month',a.CALDT,b.CALDT)=-1
	order by a.CALDT,a.CRSP_FUNDNO;
quit;

proc sql;
	create table test1 as
	select distinct CALDT,sum(dret*MTNA)/sum(MTNA) as MF_ret
	from test
	group by CALDT;
quit;

proc sql;
	create table test1 as
	select distinct a.*,b.*,sum(a.MF_ret,-b.rf) as Ex_MF_ret,month(a.CALDT) as mon,year(a.CALDT) as year
	from test1 as a left join ff.FACTORS_DAILY as b
	on a.CALDT=b.date;
quit;

proc sort data=test1; by year mon CALDT; run;
proc reg data=test1 noprint tableout outest=Alpha;
  by year mon;
  model Ex_MF_ret = MKTRF;
quit;
data Alpha;
  set Alpha;
  where _Type_ in ('PARMS');
  keep mon year MKTRF;
  rename MKTRF=Beta;
run;

proc download data=alpha out=beta; run;
endrsubmit;

data bb1.beta;
	set beta;
run;

*******Aggregate beta from Boguth & Simutin paper**********;




