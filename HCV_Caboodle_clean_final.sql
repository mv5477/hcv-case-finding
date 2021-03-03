--------------------------------------------------------------------------------------------------------------------------
--algorithm for HCV case finding
--last update: September 10, 2020
--how to use: run all the queries in the file, get the list of HCV patients with all available outcomes and info
--see 'HCV_Caboodle_dump' file for additional Caboodle queries (for this project or other tasks)


--TODO list of things to improve
--1) Parsing of HCV RNA labs
--		==> works fine on the current database, but would be useful to make the process more robust if we want to export to other sites
--			(e.g. properly casting strings to numerics to apply thresholds)
--2) Next-of-kin/emergency contact
--		==> very little data in Caboodle. MSDW had a 'next_of_kin' table that included way more data.
--			not sure if this info is still used in the outreach process though...
--3) Ethnicity
--		==> very sparse as well. Probably not used for outreach, and only an issue if we want to do stats on the patient population.


--Note about using ALT decrease to find patients who have probably been cured of HCV (even though we have no RNA labs indicating it):
--		==> not implemented in SQL (done in Java, see attached file)
--		==> for a patient with n HCV RNA labs: x_1 to x_n
--				==> for k=1..n-1
--						==> compute mean of lab results x_1 to x_k as A, mean of lab results x_k+1 to x_n as B
--						==> if B < 0.5*A (for any k), then we consider that there has been significant ALT decrease
--		==> this process captured ~80-90% of SVR patients ; hard to properly validate but it seems about right...


--------------------------------------------------------------------------------------------------------------------------
--HCV case-finding process
--get all HCV RNA labs
drop table if exists #HCVRNALabs;
create table #HCVRNALabs (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey bigint,
	DtCollection datetime,
	LabName nvarchar(300),
	OriginalValue nvarchar(300),
	Value nvarchar(300),
	Units nvarchar(300),
	IsPositive smallint,
	IsAnchor smallint,
	IsMostRecentValidAnchor smallint,
	IsLastTest tinyint
);

insert into #HCVRNALabs
select distinct pd.DurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Value, lcrf.Unit, -1, -1, -1, 0
from PRDREPORT.dbo.PatientDim pd
inner join PRDREPORT.dbo.EncounterFact ef on pd.DurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('HEPATITIS C VIRUS RNA QUANT','HEPATITIS C VIRUS RNA QUANT, GENOTYPE'
,'HEPATITIS C VIRUS SUBTYPE PCR W/REFLEX GENOTYPE', 'HCV RNA QUANTITATIVE', 'HCV RNA QUANTITATIVE', 'HCV RNA, QL, TMA'
, 'HCV RNA QN PCR', 'HEPATITIS C VIRUS RNA QUANT RT-PCR', 'HCV RNA IU/ML', 'HCV RNA, QN, PCR', 'HCV RNA QN PCR'
, 'HEPATITIS C QUANTITATION', 'COBAS TAQMAN HCV', 'HCV RNA IU/ML', 'HCV RNA QNPCR'
, 'HEPTIMAX (R) HCV RNA','HEPTIMAX RNA','HEPTIMAX (R) HCV RNA  LOG  IU/ML','HEPTIMAX (R) HCV RNA IU/ML')
--and pd.DurableKey > 3
AND pd.IsCurrent = 1 AND pd.IsValid = 1
AND pd.PrimaryMrn NOT LIKE '%*%' AND pd.PrimaryMrn NOT LIKE '%<%' AND pd.PrimaryMrn <> ''
order by pd.DurableKey, lcrf.CollectionInstant;


--format lab values
--this part is a bit too 'ad-hoc' and should probably be fixed to be more generic, maybe based on the data fron other sites(?)
--remove characters interfering with conversion to numeric, and try to handle dots wrongly inserted in numeric values (trying not to touch log values)
update #HCVRNALabs
set Value = RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(Value, ',', ''), '*', ''), ' ', '')));
update #HCVRNALabs
set Value = replace(Value,'.','')
where (len(Value)-len(replace(Value,'.',''))>1 or Value like '[0-9]%[0-9][.]' or Value like '[0-9]%[0-9][.]00' or Value like '[0-9]%[0-9][.]000');
update #HCVRNALabs
set Value = SUBSTRING(Value,1,len(Value)-2)
where ISNUMERIC(Value)=1 and CHARINDEX('.',Value)>0 and CHARINDEX('.',Value)<len(Value)-2;
update #HCVRNALabs
set Value = SUBSTRING(Value,1,len(Value)-2)
where ISNUMERIC(Value)=1 and CHARINDEX('.',Value)>0 and CHARINDEX('.',Value)<len(Value)-2;


--mark positive lab results (any result marked as positive/reactive/etc., or value > 15)
--conversion to numeric sometimes fails when trying to compare the converted value (i.e. value > 15 ?); I use len(Value) as proxy
--first 'where' line is for text values
--second line is for log values
--third line is for numeric values
update #HCVRNALabs
set IsPositive = 1
where (lower(Value) like '>%' or lower(Value) like 'det%' or lower(Value) like 'pos%' or lower(Value) like 'reac%' or lower(Value) like 'abnormal%'
or (ISNUMERIC(Value)=1 and (len(Value)>1 and (lower(Units) like 'log%' or CHARINDEX('.', Value)>0) and CONVERT(float,Value)>1.18))
or (ISNUMERIC(Value)=1 and CHARINDEX('.', Value)=0 and lower(Units) not like 'log%' and len(Value)>1));
--or (ISNUMERIC(Value)=1 and CHARINDEX('.', Value)=0 and lower(Units) not like 'log%' and len(Value)>3));

--mark negative lab results
update #HCVRNALabs
set IsPositive = 0
where (lower(Value) like '<%' or lower(Value) like 'neg%' or lower(Value) like '%non%det%' or lower(Value) like '%not%det%' or lower(Value) like 'nd%' or lower(Value) like 'nr%' or lower(Value) like '%non%rea%' or lower(Value) like '%not%rea%'
or (ISNUMERIC(Value)=1 and (len(Value)>1 and (lower(Units) like 'log%' or CHARINDEX('.', Value)>0) and CONVERT(float,Value)<=1.18))
or (ISNUMERIC(Value)=1 and CHARINDEX('.', Value)=0 and lower(Units) not like 'log%' and len(Value)<=1));
--or (ISNUMERIC(Value)=1 and CHARINDEX('.', Value)=0 and lower(Units) not like 'log%' and len(Value)<=3));

--select * from #HCVRNALabs where IsPositive = -1;

--mark anchor yes/no
--'anchor' = negative or low positive (<1000)
update #HCVRNALabs
set IsAnchor = 0 where IsPositive <> -1;

update #HCVRNALabs
set IsAnchor = 1
where (lower(Value) like '<%' or lower(Value) like 'neg%' or lower(Value) like 'low pos%' or lower(Value) like '%non%det%' or lower(Value) like '%not%det%' or lower(Value) like 'nd%' or lower(Value) like 'nr%' or lower(Value) like '%non%rea%' or lower(Value) like '%not%rea%'
or (ISNUMERIC(Value)=1 and (len(Value)>1 and (lower(Units) like 'log%' or CHARINDEX('.', Value)>0) and CONVERT(float,Value)<=3))
or (ISNUMERIC(Value)=1 and CHARINDEX('.', Value)=0 and lower(Units) not like 'log%' and len(Value)<=3 and len(Value)>1));


--find last (most recent) test for each patient
update #HCVRNALabs
set IsLastTest = 1
from (select t.*
from #HCVRNALabs t
where t.DtCollection = (select max(t2.DtCollection)
                      from #HCVRNALabs t2
                      where t2.PatientDurableKey = t.PatientDurableKey
					  and t2.IsPositive <> -1)
) x, #HCVRNALabs rna
where x.Id = rna.Id;

--find most recent anchor lab at least 3 months before last test
--update #HCVRNALabs
--set IsMostRecentValidAnchor = -1;
update #HCVRNALabs
set IsMostRecentValidAnchor = 0
where IsAnchor = 1;

--mark test
update #HCVRNALabs
set IsMostRecentValidAnchor = 1
from (select t.*
from #HCVRNALabs t
where t.DtCollection = (select max(AnchorDate)
						from (select distinct t1.PatientDurableKey,t1.DtCollection as AnchorDate,t2.DtCollection
							from #HCVRNALabs t1
							inner join #HCVRNALabs t2 on t1.PatientDurableKey = t2.PatientDurableKey
							where t2.IsLastTest = 1 and t2.IsPositive = 0
							and t1.IsAnchor = 1 and DATEDIFF(day, t1.DtCollection, t2.DtCollection)>90) ad
							where ad.PatientDurableKey = t.PatientDurableKey)
) x, #HCVRNALabs rna
where x.Id = rna.Id;

--edit if positive test after last anchor
update #HCVRNALabs
set IsMostRecentValidAnchor = 0
from (select t1.*
from #HCVRNALabs t1
inner join #HCVRNALabs t2 on t1.PatientDurableKey = t2.PatientDurableKey
where t2.DtCollection > t1.DtCollection
and t1.IsMostRecentValidAnchor = 1 and t2.IsPositive = 1
) x, #HCVRNALabs rna
where x.Id = rna.Id;



--HCV antibody labs
drop table if exists #HCVAntibodyLabs;
create table #HCVAntibodyLabs (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey bigint,
	DtCollection datetime,
	LabName nvarchar(300),
	Value nvarchar(300),
	Units nvarchar(300),
	IsPositive smallint,
	IsLastTest tinyint
);

insert into #HCVAntibodyLabs
select distinct pd.DurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Unit, -1, 0
from PRDREPORT.dbo.PatientDim pd
inner join PRDREPORT.dbo.EncounterFact ef on pd.DurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('HEPATITIS C ANTIBODY BY CIA INTERP','HEP. C AB','HEPATITIS C VIRUS AB','HEP C AB(874)','HCV AB,RIBA','HEP C AB')
AND pd.IsCurrent = 1 AND pd.IsValid = 1
AND pd.PrimaryMrn NOT LIKE '%*%' AND pd.PrimaryMrn NOT LIKE '%<%' AND pd.PrimaryMrn <> ''
order by pd.DurableKey, lcrf.CollectionInstant;

--select * from #HCVAntibodyLabs;
--select * from #HCVAntibodyLabs where IsPositive = -1;

--marking positive and negative antibody labs; only using text values since numeric values don't seem to be used here(?)
--positive
update #HCVAntibodyLabs
set IsPositive = 1
where (lower(Value) like 'det%' or lower(Value) like 'pos%' or lower(Value) like 'high pos%' or lower(Value) like 'low pos%' or lower(Value) like 'reac%' or lower(Value) like 'abnormal%');

--negative
update #HCVAntibodyLabs
set IsPositive = 0
where (lower(Value) like 'neg%' or lower(Value) like '%non%det%' or lower(Value) like '%not%det%' or lower(Value) like 'nd%' or lower(Value) like 'nr%' or lower(Value) like '%non%rea%' or lower(Value) like '%not%rea%');


update #HCVAntibodyLabs
set IsLastTest = 1
from (select t.*
from #HCVAntibodyLabs t
where t.DtCollection = (select max(t2.DtCollection)
                      from #HCVAntibodyLabs t2
                      where t2.PatientDurableKey = t.PatientDurableKey)
) x, #HCVAntibodyLabs rna
where x.Id = rna.Id;


--get all HCV ICD codes
drop table if exists #HCVICDCodes;
create table #HCVICDCodes (
	PatientDurableKey bigint,
	DtDiagnosis datetime,
	Code nvarchar(300),
	CodeType nvarchar(300)
);

insert into #HCVICDCodes
select distinct pd.DurableKey, ef.Date, dtd.Value, def.Type
from PRDREPORT.dbo.PatientDim pd
inner join PRDREPORT.dbo.EncounterFact ef on pd.DurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.DiagnosisEventFact def on ef.EncounterKey = def.EncounterKey
inner join PRDREPORT.dbo.DiagnosisTerminologyDim dtd on def.DiagnosisKey = dtd.DiagnosisKey
where def.Type in ('Billing Diagnosis','Encounter Diagnosis','Hospital Problem','Problem List')
and dtd.Type = 'ICD-10-CM'
and dtd.Value in ('B17.10','B17.11','B18.2','B19.10','B19.20','B19.21','K73.2','K74.60','K74.69','K76.8','Z86.19')
AND pd.IsCurrent = 1 AND pd.IsValid = 1
AND pd.PrimaryMrn NOT LIKE '%*%' AND pd.PrimaryMrn NOT LIKE '%<%' AND pd.PrimaryMrn <> ''
order by pd.DurableKey, ef.Date;

--select * from #HCVICDCodes;
--select count(distinct PatientDurableKey) from #HCVICDCodes;



--base patients list: any RNA+ OR any AB+ OR any ICD code for HCV
drop table if exists #HCVAllPatients;
create table #HCVAllPatients (
	MRN nvarchar(300),
	PatientDurableKey nvarchar(300)
);

insert into #HCVAllPatients
select distinct pd.PrimaryMrn, pd.DurableKey
from PRDREPORT.dbo.PatientDim pd
inner join
(select distinct PatientDurableKey from #HCVRNALabs where IsPositive = 1
union
select distinct PatientDurableKey from #HCVAntibodyLabs where IsPositive = 1
union
select distinct PatientDurableKey from #HCVICDCodes) z
on pd.DurableKey = z.PatientDurableKey;




--HCV medications
--merge data from MedicationEventFact and MedicationAdministrationFact (prescriptions and administrations)
drop table if exists #HCVMedications;
create table #HCVMedications (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey nvarchar(300),
	MedicationKey bigint,
	MedicationName nvarchar(300),
	GenericName nvarchar(300),
	MedicationCodeKey bigint,
	MedicationEventType nvarchar(50),
	MedicationEventDate datetime,
	IsLastMed tinyint
);

--using NDC codes for HCV meds (list should be solid; provided by Dr. Dieterich)
insert into #HCVMedications
select distinct PatientDurableKey, MedicationKey, Name, GenericName, MedicationCodeKey, Type, Date, 0
from
(select pd.PatientDurableKey, mef.MedicationKey, md.Name, md.GenericName, mcd.MedicationCodeKey, mef.Type, ef.Date
from #HCVAllPatients pd
inner join PRDREPORT.dbo.EncounterFact ef on pd.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.MedicationEventFact mef on ef.EncounterKey = mef.EncounterKey
inner join PRDREPORT.dbo.MedicationCodeDim mcd on mef.MedicationKey = mcd.MedicationKey
inner join PRDREPORT.dbo.MedicationDim md on mcd.MedicationKey = md.MedicationKey
where mcd.RawNumericCode in (61958180101,61958180301,61958220101,61958240101,00074262528,72626260101,72626270101)
and mcd.Type = 'NDC'
union
select pd.PatientDurableKey, maf.MedicationKey, md.Name, md.GenericName, mcd.MedicationCodeKey, 'Administration', ef.Date
from #HCVAllPatients pd
inner join PRDREPORT.dbo.EncounterFact ef on pd.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.MedicationAdministrationFact maf on ef.EncounterKey = maf.EncounterKey
inner join PRDREPORT.dbo.MedicationCodeDim mcd on maf.MedicationKey = mcd.MedicationKey
inner join PRDREPORT.dbo.MedicationDim md on mcd.MedicationKey = md.MedicationKey
where mcd.RawNumericCode in (61958180101,61958180301,61958220101,61958240101,00074262528,72626260101,72626270101)
and mcd.Type = 'NDC') z
order by PatientDurableKey, Date;

--select * from #HCVMedications order by PatientDurableKey;
--select count(distinct PatientDurableKey) from #HCVMedications;

update #HCVMedications
set IsLastMed = 1
from (select t.*
from #HCVMedications t
where t.MedicationEventDate = (select max(t2.MedicationEventDate)
                      from #HCVMedications t2
                      where t2.PatientDurableKey = t.PatientDurableKey)
) x, #HCVMedications meds
where x.Id = meds.Id;


--visits
drop table if exists #Visits;
create table #Visits (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey nvarchar(300),
	EncounterDateKey int,
	DepartmentName nvarchar(300),
	EncounterType nvarchar(300),
	VisitType nvarchar(300),
	IsPenultimateVisit tinyint,
	IsLastVisit tinyint
);

--could refine the filters; maybe use the type of encounter instead of the status
insert into #Visits
select distinct z.PatientDurableKey,vf.EncounterDateKey,dd.DepartmentName,vf.EncounterType,vf.VisitType, 0, 0
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.VisitFact vf on ef.EncounterKey = vf.EncounterKey
inner join PRDREPORT.dbo.DepartmentDim dd on vf.DepartmentKey = dd.DepartmentKey
where (vf.AppointmentStatus in ('Arrived','Completed'))
order by z.PatientDurableKey,vf.EncounterDateKey;

--select * from #Visits;
--select distinct EncounterType from PRDREPORT.dbo.VisitFact;

--marking the last two visits to get the dates for outreach prioritization
update #Visits
set IsLastVisit = 1
from (select t.*
from #Visits t
where t.EncounterDateKey = (select max(t2.EncounterDateKey)
                      from #Visits t2
                      where t2.PatientDurableKey = t.PatientDurableKey)
) x, #Visits vis
where x.Id = vis.Id;

update #Visits
set IsPenultimateVisit = 1
from (select t.*
from #Visits t
where t.EncounterDateKey = (select max(t2.EncounterDateKey)
                      from #Visits t2
                      where t2.PatientDurableKey = t.PatientDurableKey and t2.IsLastVisit = 0)
) x, #Visits vis
where x.Id = vis.Id;


--ALT
--details for ALT decrease => possible cure
drop table if exists #ALTLabs;
create table #ALTLabs (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey bigint,
	DtCollection datetime,
	LabName nvarchar(300),
	Value nvarchar(300),
	Units nvarchar(300),
	IsLastTest tinyint
);

insert into #ALTLabs
select distinct z.PatientDurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Unit, 0
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('ALT','ALT (SGPT)','ALT (SGPT) (POCT)','ALT (SGPT) P5P','ALT(SGPT)')
and ISNUMERIC(lcrf.Value) = 1
order by z.PatientDurableKey, lcrf.CollectionInstant;

--select * from #ALTLabs order by PatientDurableKey,DtCollection;

update #ALTLabs
set IsLastTest = 1
from (select t.*
from #ALTLabs t
where t.DtCollection = (select max(t2.DtCollection)
                      from #ALTLabs t2
                      where t2.PatientDurableKey = t.PatientDurableKey)
) x, #ALTLabs meds
where x.Id = meds.Id;


--FIB-4
drop table if exists #FIB4Labs;
create table #FIB4Labs (
	Id int IDENTITY(1,1) primary key,
	PatientDurableKey bigint,
	DtCollection datetime,
	ALT int,
	AST int,
	PLT int,
	FIB4 float,
	IsLastTest tinyint
);

--get the labs, with strong requirements on format to make sure the conversion to numeric works (for FIB-4 computation)
insert into #FIB4Labs
select distinct alt.PatientDurableKey,alt.CollectionInstant,alt.Value,ast.Value,plat.Value, 0, 0
from
(select distinct z.PatientDurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Unit
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('ALT','ALT (SGPT)','ALT (SGPT) (POCT)','ALT (SGPT) P5P','ALT(SGPT)')
and ISNUMERIC(lcrf.Value) = 1 and lcrf.Value not like '%[^0-9]%') alt,
(select distinct z.PatientDurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Unit
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('AST','AST (SGOT)','AST (SGOT) (POCT)','AST (SGOT) P5P')
and ISNUMERIC(lcrf.Value) = 1 and lcrf.Value not like '%[^0-9]%') ast,
(select distinct z.PatientDurableKey, lcrf.CollectionInstant, lcd.Name, lcrf.Value, lcrf.Unit
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.LabComponentResultFact lcrf on ef.EncounterKey = lcrf.EncounterKey
inner join PRDREPORT.dbo.LabComponentDim lcd on lcrf.LabComponentKey = lcd.LabComponentKey
where lcd.Name in ('PLATELET','PLT CNT')
and ISNUMERIC(lcrf.Value) = 1 and lcrf.Value not like '%[^0-9]%') plat
where alt.PatientDurableKey = ast.PatientDurableKey and ast.PatientDurableKey = plat.PatientDurableKey
--and DAY(alt.CollectionInstant) = DAY(ast.CollectionInstant) and DAY(ast.CollectionInstant) = DAY(plat.CollectionInstant)
and alt.CollectionInstant = ast.CollectionInstant and ast.CollectionInstant = plat.CollectionInstant
order by alt.PatientDurableKey,alt.CollectionInstant;

--select * from #FIB4Labs;
--select count(distinct PatientDurableKey) from #FIB4Labs;

update #FIB4Labs
set FIB4 = ROUND(((DATEDIFF(DAY, pd.BirthDate, z.DtCollection)/365.25)*z.AST)/(z.PLT*SQRT(z.ALT)), 2)
from #FIB4Labs z
inner join PRDREPORT.dbo.PatientDim pd on z.PatientDurableKey = pd.DurableKey
where pd.BirthDate is not null
and z.ALT > 0 and z.AST > 0 and z.PLT > 0;

update #FIB4Labs
set IsLastTest = 1
from (select t.*
from #FIB4Labs t
where t.DtCollection = (select max(t2.DtCollection)
                      from #FIB4Labs t2
                      where t2.PatientDurableKey = t.PatientDurableKey)
) x, #FIB4Labs meds
where x.Id = meds.Id;



--HIV ICD codes
--split 9/10
drop table if exists #HIVICDCodes;
create table #HIVICDCodes (
	PatientDurableKey bigint,
	DtDiagnosis datetime,
	Code nvarchar(300),
	CodeType nvarchar(300)
);

insert into #HIVICDCodes
select distinct z.PatientDurableKey, ef.Date, dtd.Value, def.Type
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.DiagnosisEventFact def on ef.EncounterKey = def.EncounterKey
inner join PRDREPORT.dbo.DiagnosisTerminologyDim dtd on def.DiagnosisKey = dtd.DiagnosisKey
where def.Type in ('Billing Diagnosis','Encounter Diagnosis','Hospital Problem','Problem List')
and dtd.Type in ('ICD-9-CM','ICD-10-CM')
and dtd.Value in ('079.53','B20','B97.35','V08','Z21')
order by z.PatientDurableKey, ef.Date;

--select * from #HIVICDCodes;
--select count(distinct PatientDurableKey) from #HIVICDCodes;

--diabetes ICD codes
drop table if exists #DiabetesICDCodes;
create table #DiabetesICDCodes (
	PatientDurableKey bigint,
	DtDiagnosis datetime,
	Code nvarchar(300),
	CodeType nvarchar(300)
);

insert into #DiabetesICDCodes
select distinct z.PatientDurableKey, ef.Date, dtd.Value, def.Type
from #HCVAllPatients z
inner join PRDREPORT.dbo.EncounterFact ef on z.PatientDurableKey = ef.PatientDurableKey
inner join PRDREPORT.dbo.DiagnosisEventFact def on ef.EncounterKey = def.EncounterKey
inner join PRDREPORT.dbo.DiagnosisTerminologyDim dtd on def.DiagnosisKey = dtd.DiagnosisKey
where def.Type in ('Billing Diagnosis','Encounter Diagnosis','Hospital Problem','Problem List')
and dtd.Type in ('ICD-10-CM')
and dtd.Value in ('E11.44','E11.43','E11.36','E11.22','E11.620','E11.41','E11.21','E11.610','E11.40','E11.52','E11.51'
,'E11.42','E11.621','E11.65','E11.01','E11.00','E11.641','E11.649','E11.321','E11.329','E11.331','E11.339','E11.59','E11.618','E11.29'
,'E11.49','E11.39','E11.638','E11.628','E11.622','E11.69','E11.630','E11.351','E11.359','E11.341','E11.349','E11.8','E11.311','E11.319','E11.9')
order by z.PatientDurableKey, ef.Date;

--select * from #DiabetesICDCodes;
--select count(distinct PatientDurableKey) from #DiabetesICDCodes;




--final table ; should look like the csv file ("patients list") from the previous version
drop table if exists #HCVPatientsList;
create table #HCVPatientsList (
	PatientDurableKey bigint primary key,
	MRN nvarchar(300),
	LastRNATestDate datetime,
	LastRNATestResult nvarchar(300),
	IsCuredLastRNA tinyint,
	IsCuredStrict tinyint,
	LastHCVMedicationDate datetime,
	LastHCVMedicationName nvarchar(300),
	OutcomeFromRNAAndMeds nvarchar(50),
	LastAntibodyTestDate datetime,
	LastAntibodyTestResult nvarchar(300),
	IsDeceased tinyint,
	DateOfDeath datetime,
	DateOfBirth datetime,
	FirstName nvarchar(100),
	LastName nvarchar(100),
	Sex nvarchar(300),
	FirstRace nvarchar(300),
	Ethnicity nvarchar(300),
	Address nvarchar(150),
	HomePhoneNumber nvarchar(200),
	WorkPhoneNumber nvarchar(200),
	EmailAddress nvarchar(300),
	NextOfKinName nvarchar(200),
	NextOfKinRelationship nvarchar(200),
	NextOfKinPhone nvarchar(200),
	LastVisitDateKey int,
	PenultimateVisitDateKey int,
	LastALTDate datetime,
	LastALTResult numeric,
	HasHIVCode tinyint,
	HasDiabetesCode tinyint,
	LastFIB4Date datetime,
	LastFIB4Result float
);

--select * from #HCVPatientsList order by PatientDurableKey;
--select * from #HCVRNALabs order by PatientDurableKey;
--select * from #HCVRNALabs where PatientDurableKey = 16265;
--select count(*) from #HCVPatientsList where len(EmailAddress)>1;

insert into #HCVPatientsList(PatientDurableKey)
select distinct PatientDurableKey from #HCVAllPatients;

--demographics and personal info
update z
set MRN = pd.PrimaryMrn, FirstName = pd.FirstName, LastName = pd.LastName, Ethnicity = pd.Ethnicity, FirstRace = pd.FirstRace, DateOfBirth = pd.BirthDate, DateOfDeath = pd.DeathDate
, Sex = pd.Sex, Address = pd.Address, HomePhoneNumber = pd.HomePhoneNumber, WorkPhoneNumber = pd.WorkPhoneNumber, EmailAddress = pd.EmailAddress
from #HCVPatientsList z
inner join PRDREPORT.dbo.PatientDim pd on z.PatientDurableKey = pd.DurableKey;

update #HCVPatientsList
set IsDeceased = 0;
update #HCVPatientsList
set IsDeceased = 1
where DateOfDeath is not null;

--next of kin data seems less complete here than in MSDW for some reason
--I tried different ways and tables, but could not find anything better than this
update z
set NextOfKinName = pavd.Value
from #HCVPatientsList z
inner join PRDREPORT.dbo.PatientAttributeValueDim pavd on z.PatientDurableKey = pavd.PatientDurableKey
inner join PRDREPORT.dbo.AttributeDim ad on pavd.AttributeKey = ad.AttributeKey
where ad.AttributeKey = 1583;
update z
set NextOfKinRelationship = pavd.Value
from #HCVPatientsList z
inner join PRDREPORT.dbo.PatientAttributeValueDim pavd on z.PatientDurableKey = pavd.PatientDurableKey
inner join PRDREPORT.dbo.AttributeDim ad on pavd.AttributeKey = ad.AttributeKey
where ad.AttributeKey = 1145;
update z
set NextOfKinPhone = pavd.Value
from #HCVPatientsList z
inner join PRDREPORT.dbo.PatientAttributeValueDim pavd on z.PatientDurableKey = pavd.PatientDurableKey
inner join PRDREPORT.dbo.AttributeDim ad on pavd.AttributeKey = ad.AttributeKey
where ad.AttributeKey = 2029;


--RNA labs and infection status from labs
update z
set z.LastRNATestDate = rna.DtCollection, z.LastRNATestResult = rna.OriginalValue
from #HCVPatientsList z
inner join #HCVRNALabs rna on z.PatientDurableKey = rna.PatientDurableKey
where rna.IsLastTest = 1;

--select * from
--#HCVPatientsList z
--inner join #HCVRNALabs rna on z.PatientDurableKey = rna.PatientDurableKey
--where rna.IsLastTest = 1 order by z.PatientDurableKey;

update #HCVPatientsList
set IsCuredLastRNA = 0, IsCuredStrict = 0
where LastRNATestResult is not null;

--similar to the 'is_cured' of the report (outreach version)
update z
set IsCuredLastRNA = 1
from #HCVPatientsList z
inner join #HCVRNALabs rna on z.PatientDurableKey = rna.PatientDurableKey
where rna.IsLastTest = 1 and rna.IsPositive = 0

--strict definition = manuscript definition (last test is negative AND anchor test 3+ months before that) OR only negative tests
--similar to the 'is_cured' of the report (regular version)
update z
set IsCuredStrict = 1
from #HCVPatientsList z
inner join #HCVRNALabs rna on z.PatientDurableKey = rna.PatientDurableKey
where rna.IsMostRecentValidAnchor = 1;

update z
set IsCuredStrict = 1
from #HCVPatientsList z
inner join (select distinct PatientDurableKey,count(Id) as cpt
from #HCVRNALabs group by PatientDurableKey) total on z.PatientDurableKey = total.PatientDurableKey
inner join (select distinct PatientDurableKey,count(Id) as cpt
from #HCVRNALabs where IsPositive = 0 group by PatientDurableKey) neg on z.PatientDurableKey = neg.PatientDurableKey
where total.PatientDurableKey = neg.PatientDurableKey and total.cpt = neg.cpt;


--HCV medications
update z
set LastHCVMedicationDate = meds.MedicationEventDate, LastHCVMedicationName = meds.MedicationName
from #HCVPatientsList z
inner join #HCVMedications meds on z.PatientDurableKey = meds.PatientDurableKey
where meds.IsLastMed = 1;

--cure status based on medications and RNA labs
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'noData'
where LastRNATestDate is null;
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'RNAOnly'
where LastRNATestDate is not null and LastHCVMedicationDate is null;
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'lostToFollowUp'
where LastRNATestDate is not null and LastHCVMedicationDate is not null
and LastRNATestDate <= LastHCVMedicationDate;
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'failed'
where LastRNATestDate is not null and LastHCVMedicationDate is not null
and LastRNATestDate > LastHCVMedicationDate and IsCuredLastRNA = 0;
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'potentially treated SVR unknown'
where LastRNATestDate is not null and LastHCVMedicationDate is not null
and LastRNATestDate > LastHCVMedicationDate and DATEDIFF(DAY, LastHCVMedicationDate, LastRNATestDate) < 90 and IsCuredLastRNA = 1;
update #HCVPatientsList
set OutcomeFromRNAAndMeds = 'SVR'
where LastRNATestDate is not null and LastHCVMedicationDate is not null
and LastRNATestDate > LastHCVMedicationDate and DATEDIFF(DAY, LastHCVMedicationDate, LastRNATestDate) > 90 and IsCuredLastRNA = 1;


--HCV antibody labs
update z
set LastAntibodyTestDate = ab.DtCollection, LastAntibodyTestResult= ab.Value
from #HCVPatientsList z
inner join  #HCVAntibodyLabs ab on z.PatientDurableKey = ab.PatientDurableKey
where ab.IsLastTest = 1;


--HIV and diabetes (ICD codes)
update #HCVPatientsList set HasHivCode = 0, HasDiabetesCode = 0;

update z
set HasHivCode = 1
from #HCVPatientsList z
inner join #HIVICDCodes hiv on z.PatientDurableKey = hiv.PatientDurableKey;

update z
set HasDiabetesCode = 1
from #HCVPatientsList z
inner join #DiabetesICDCodes diab on z.PatientDurableKey = diab.PatientDurableKey;


--ALT
update z
set LastALTDate = alt.DtCollection, LastALTResult = alt.Value
from #HCVPatientsList z
inner join  #ALTLabs alt on z.PatientDurableKey = alt.PatientDurableKey
where alt.IsLastTest = 1;


--FIB-4
update z
set LastFIB4Date = fib4.DtCollection, LastFIB4Result = fib4.FIB4
from #HCVPatientsList z
inner join  #FIB4Labs fib4 on z.PatientDurableKey = fib4.PatientDurableKey
where fib4.IsLastTest = 1;


--visits
update z
set LastVisitDateKey = vis.EncounterDateKey
from #HCVPatientsList z
inner join #Visits vis on z.PatientDurableKey = vis.PatientDurableKey
where vis.IsLastVisit = 1;

update z
set PenultimateVisitDateKey = vis.EncounterDateKey
from #HCVPatientsList z
inner join #Visits vis on z.PatientDurableKey = vis.PatientDurableKey
where vis.IsPenultimateVisit = 1;


select * from #HCVPatientsList;




