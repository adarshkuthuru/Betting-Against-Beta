libname BAB 'E:\Drive\Local Disk F\Betting against beta'; run;



data BAB;
  set BAB.Bab_final_2017_08_11;
  rename _name_ = Country;
run;

proc sort data=BAB; by Country Date; run;

data BAB;
  set BAB;
  if missing(BAB_ret)=1 then delete;
run;

proc sort data=BAB; by Country D2; run;
proc means data=BAB noprint;
  by Country D2;
  var BAB_ret;
  output out=BABStats mean=mean t=tstat probt=pvalue;
run;


ods trace on;
ods output statistics = statistics TTests=ttests; 
proc ttest data=BAB;
  by Country;
  class D2;
  var BAB_ret;
run;
ods trace off;

data statistics;
  set statistics;
  if class='Diff (1-2)';
  D2 = .;
  keep country D2 mean;
run;

proc sql;
  create table statistics as
  select distinct a.*, b.tvalue as tstat, b.probt as pvalue
  from statistics as a, ttests as b
  where a.country=b.country and b.method='Pooled';
quit;

data BABStats1;
  set BABStats statistics;
  if D2=. then D2=2;
  drop _Type_;
  mean=mean*100;
  format mean 6.3 tstat 6.3 pvalue 6.3;
run;
proc sort data=BABStats1; by Country D2; run;

data BABStats2;
  set BABStats1;
  if D2=2 and pvalue > 0.10;
run;


/********************************/
/*** Cross-sectional strategy ***/
/********************************/

proc sort data=BAB; by Date D2; run;

proc sql;
  create table CountriesbyDate as
  select distinct date, count(distinct Country) as ncountry
  from BAB
  group by date;
quit;

proc sql;
  create table BAB1 as
  select distinct a.*
  from BAB as a, CountriesbyDate as b
  where a.date=b.date and b.ncountry>=10;
quit;
proc sort data=BAB1; by Date D2; run;

proc sql;
  create table PortRet as
  select distinct date, D2, mean(BAB_ret) as PortRet, count(distinct country) as ncountry
  from BAB1
  group by date, D2;
quit;
proc sql;
  create table HedgePortRet as
  select distinct a.date, 2 as D2, a.PortRet-b.PortRet as PortRet
  from PortRet(where=(D2=0)) as a, PortRet(where=(D2=1)) as b
  where a.date=b.date;
quit;

data PortRet;
  set PortRet HedgePortRet;
  drop ncountry;
run;
proc sql;
  create table PortRet as
  select distinct *, count(portret) as nobs
  from PortRet
  group by date;
quit;

data PortRet;
  set PortRet;
  if nobs^=3 then delete;
  drop nobs;
run;


***Add full sample;
proc sql;
  create table FullSample as
  select distinct date, 3 as D2, mean(BAB_ret) as PortRet
  from BAB
  group by date;
quit;

proc sql;
  create table FullSample as
  select distinct a.*
  from FullSample as a, (select distinct date from PortRet) as b
  where a.date=b.date;
quit;

data PortRet;
  set PortRet FullSample;
run;


proc sort data=PortRet; by D2 date; run;
proc means data=PortRet noprint;
  by D2;
  var PortRet;
  output out=StrategyStat mean=mean t=tstat std=std skew=skew min=min max=max;
run;
data StrategyStat;
  set StrategyStat;
  mean= mean*12*100;
  std=std*sqrt(12)*100;
  sharpe = mean/std;
run;
