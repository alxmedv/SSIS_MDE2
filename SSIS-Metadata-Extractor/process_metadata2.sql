use [MYDW_META]
go

/* the functions shall be created before the next SQL is executed

--Get separated expression value (concrete usage - parse ConnectString)
create or alter function getsepvalue (@srchbuff varchar(max), @srchKey varchar(200))
returns varchar(max)
as
begin
declare @sepchar char(1) = ';';
declare @srchequ varchar(200) = @srchKey+'=';
declare @rv varchar(200) = NULL;
if (CHARINDEX(@srchequ, @srchbuff)>0)
set @rv = (SUBSTRING(SUBSTRING(@srchbuff, CHARINDEX(@srchequ, @srchbuff) + LEN(@srchequ), 100), 0, 
		CHARINDEX(@sepchar, SUBSTRING(@srchbuff, CHARINDEX(@srchequ, @srchbuff) + LEN(@srchequ), 100))));
return @rv;
end
go

create or alter function quoteonce (@mstr varchar(max))
returns varchar(max)
as
begin
if (CHARINDEX('[', @mstr)=1 AND CHARINDEX(']', @mstr)=len(@mstr))
return @mstr;
return quotename(@mstr);
end
go
*/


/* 
Step 1. Run Powershell extract-ssis-pack-info-new.ps1
Step 2. Run Powershell extract-ssis-connect-mgrs.ps1
*/

drop table if exists #tmp_ssisPkgMeta;

--Step 3. get all Package objects with connection properties
select 
	s.Package,s.ObjectType,s.RefId,s.ParentRef,s.dtsId,s.ObjectClass,s.ObjectName,s.ContentType,s.Content,s.ConnmanagerId,
	isnull(dbo.getsepvalue ([ConnectionString], 'Data Source'),dbo.getsepvalue ([ConnectionString], 'Server')) DbServerName,
	isnull(dbo.getsepvalue ([ConnectionString], 'Initial Catalog'),dbo.getsepvalue ([ConnectionString], 'Database')) DbName,
	s.id
into #tmp_ssisPkgMeta
from (
	select 
		m.id,m.Package,m.ObjectType,m.RefId,m.ParentRef,m.dtsId,m.ObjectClass,m.ObjectName,m.ContentType,m.Content,m.ConnManagerId,
		isnull(cm.objectClass,'Package.'+pcm.objecttype) con_objectClass,
		isnull(cm.dtsid,pcm.dtsid) con_dtsid,
		isnull(cm.objectName,pcm.objectName) ConnectionName,
		isnull(cm.creationname,pcm.objectclass) ConnectionClass,
		isnull(cm.connectionstring,case when pcm.contenttype='ConnectionString' then pcm.content end) ConnectionString
	from (
	select 
		pm.Package,pm.ObjectType,pm.RefId,pm.ParentRef,pm.dtsId,pm.ObjectClass,pm.ObjectName,pm.ContentType,
		pm.Content OrgContent, 
		coalesce(replace(pm.content, pvm.objectname, pvm.Content),pm.Content) Content, 
		pm.ConnManagerId,	coalesce(pvm.id,pm.id) Id, pm.id OrigId
	FROM [dbo].[ssisPkgMeta2] pm
	left join [dbo].[ssisPkgMeta2] pvm on pvm.objecttype='Variable' 
	and pm.Package = pvm.Package and pm.content like '%'+pvm.objectname+'%'
	where pm.objecttype != 'Variable'
	) m
	left join dbo.ssisConnectionManagers cm on replace(m.connmanagerid,':external','')=cm.dtsid
	left join [dbo].[ssisPkgMeta2] pcm on m.Package = pcm.Package and pcm.objecttype = 'ConnectionManager'
		and m.connmanagerid = case when m.contenttype='SqlStatementSource' then pcm.dtsid else pcm.refid end
) s


/* 
Step 4. ParseSQL

truncate table [dbo].[SQLOBJECTDEPENDENCIES]

Execute c# ParseSQL/Program.cs
It will populate [dbo].[SQLOBJECTDEPENDENCIES]

*/


/* 
Step 5. Process metadata

Execute all the following scripts

*/


drop table if exists #tmp_ssisPkgMeta3;

--enreach with DB and Tables data from SQLObjectDependencies
select 
pm.Package, pm.RefId, pm.ObjectType, pm.ObjectClass, pm.ObjectName, pm.ContentType, pm.Content, pm.ConnmanagerId,
pm.DbServerName, pm.DbName, pm.id, 
od.sqlObject, od.Operation, od.Nodes_Selection, od.Idents, 
isnull(od.Refinstance,pm.DbServerName) RefInstance,
isnull(od.refDb,pm.DbName) RefDb, 
case 
when pm.ContentType='OpenRowset' then substring(pm.Content,1,charindex('.',pm.Content)-1)
when (od.RefObjectName is not null or pm.ContentType='OpenRowset')  then isnull(od.refSchema,'[dbo]')  end RefSchema, 
case 
when pm.ContentType='OpenRowset' then substring(pm.Content, charindex('.',pm.Content)+1,len(pm.Content))
else od.RefObjectName end RefObjectName, 
case when pm.ContentType='OpenRowset' then 'Table' 
else od.RefObjectType end RefObjectType
into #tmp_ssisPkgMeta3
from #tmp_ssisPkgMeta pm
left join [dbo].[SQLOBJECTDEPENDENCIES] od on pm.id=od.sqlid
order by od.sqlid 


--select * from #tmp_ssisPkgMeta3 where refobjectname = '[KR_Police_02]'

drop table if exists #tmp_ssisPkgMeta4;

select  
pm.Package, pm.RefId, pm.ObjectType, pm.ObjectClass, pm.ObjectName, pm.ContentType, pm.Content, 
pm.DbServerName, pm.DbName, pm.id, 
pm.sqlObject, pm.Operation, pm.Nodes_Selection, pm.Idents, 
--pm.refSchema+'.'+pm.refObjectName,
--od.sqlObject, od.Operation, od.Nodes_Selection, od.Idents, 
case when pm.Operation = 'EXEC' then pm.refSchema+'.'+pm.refObjectName end sqlCaller,
case when pm.Operation = 'EXEC' then isnull(od.RefInstance,pm.Refinstance) else pm.Refinstance end RefInstance,
case when pm.Operation = 'EXEC' then isnull(od.RefDb,pm.refDb) else pm.refDb end RefDb, 
case when pm.Operation = 'EXEC' then isnull(od.refSchema,pm.refSchema) else pm.refSchema end RefSchema, 
case when pm.Operation = 'EXEC' then isnull(od.RefObjectName,pm.RefObjectName) else pm.RefObjectName end RefObjectName, 
case when pm.Operation = 'EXEC' then isnull(od.RefObjectType,pm.RefObjectType)  else pm.RefObjectType end RefObjectType,
case 
when pm.objectclass = 'Microsoft.OLEDBDestination' then 'Target'
when pm.objectclass = 'Microsoft.OLEDBSource' then 'Source'
when pm.objectclass in ('Microsoft.ExecuteSQLTask','Microsoft.Lookup') and pm.Operation = 'SELECT' then 'Source'
when pm.objectclass = 'Microsoft.ExecuteSQLTask' and pm.Operation in ('INSERT','TRUNCATE','DELETE','UPDATE') then 'Target'
when pm.objectclass in ('Microsoft.ExecuteSQLTask','Microsoft.Lookup') and od.Operation = 'SELECT' then 'Source'
when pm.objectclass = 'Microsoft.ExecuteSQLTask' and od.Operation in ('INSERT','TRUNCATE','DELETE','UPDATE') then 'Target'
end as RefObjDir
into #tmp_ssisPkgMeta4
from #tmp_ssisPkgMeta3 pm
left join [dbo].[SQLOBJECTDEPENDENCIES] od on od.sqlobject = pm.refSchema+'.'+pm.refObjectName and pm.Operation = 'EXEC'


drop table if exists #tmp_ssisPkgTableMatrix ;

select distinct id, package, sqlCaller,Refinstance, RefDb, RefSchema, RefObjectName, RefObjectType, RefObjDir 
into #tmp_ssisPkgTableMatrix 
from #tmp_ssisPkgMeta4 pm
where RefObjectName is not null

drop table if exists #pack_relations;

--Build Package-executing-package relationships
WITH MyCTE
AS ( 
SELECT 0 lvl,--Package ppath,
content,id,Package,Package ParentPkg 
FROM [MYDW_META].[dbo].[ssisPkgMeta2]
where objectclass in ('Microsoft.Package', 'Microsoft.ExecutePackageTask')
UNION ALL
SELECT mycte.lvl+1,--mycte.ppath+'\'+a.package,
a.content,a.id,a.Package, mycte.ParentPkg
FROM [MYDW_META].[dbo].[ssisPkgMeta2] a
INNER JOIN MyCTE ON a.Package = MyCTE.content
where objectclass in ('Microsoft.Package', 'Microsoft.ExecutePackageTask')
)
SELECT distinct lvl,
ParentPkg, id, Package, content PackInvoke
into #pack_relations
FROM MyCTE


drop table if exists dbo.ssisPkgDependencyMatrix;

--create dependency matrix
select 
r.ParentPkg rootPkg,r.lvl,
m.id,
r.Package,r.PackInvoke,
m.sqlCaller sqlSPinvoke, 
m.RefObjectType,
m.RefObjDir RefObjRole,
dbo.quoteonce(refInstance) RefInstance,
dbo.quoteonce(refdb) RefDb,
dbo.quoteonce(refschema) RefSchema,
dbo.quoteonce(RefObjectName) RefObjectName
into dbo.ssisPkgDependencyMatrix
from #pack_relations r
left join #tmp_ssisPkgTableMatrix m on r.Package=m.Package and r.PackInvoke is null
where 1=1
and coalesce(packInvoke, RefObjectName) is not null

/*
-- USAGES:

select * from dbo.ssisPkgDependencyMatrix
where rootPkg='Job_Weekend.dtsx'--_Factsonly'
and (sqlSPinvoke is null or sqlSPinvoke not in ('[dbo].[DMA_EndBatch]','[dbo].[DMA_StartBatch]','[dbo].[sp_rename]'))
order by 1,2,3,4


select distinct 
refdb,
refschema,
RefObjectName,
RefObjectType ,
RefObjRole
from dbo.ssisPkgDependencyMatrix
where RefObjectName is not null
and rootPkg='Job_Dag_AKT.dtsx'
--and RefObjRole = 'Source'
order by 1

select distinct 
refdb,
refschema,
RefObjectName,
RefObjectType ,
RefObjRole
from dbo.ssisPkgDependencyMatrix
where RefObjectName is not null
and rootPkg='Job_Weekend.dtsx'
--and RefObjRole = 'Source'
order by 1



select distinct 
refdb,
refschema,
RefObjectName,
RefObjectType 
from dbo.ssisPkgDependencyMatrix
where RefObjectName is not null
and rootPkg='Job_Weekend.dtsx'
and RefObjRole = 'Source'
order by 1


select *
from dbo.ssisPkgDependencyMatrix
where RefObjectName = '[AFTALEPARTNER_DIM]'
and RefObjRole = 'Target'
order by 1


select * from dbo.ssisPkgDependencyMatrix
where rootPkg='DF_DW_DMA_AFTALEPARTNER_DIM.dtsx'
*/