
$cons = @()
foreach ($dtsxfile in (ls "C:\Users\Me\Source\repos\MyRepo\MySolution\*.conmgr").fullname) {

	$xmld = [xml](Get-Content $dtsxfile)
	$ns = [System.Xml.XmlNamespaceManager]($xmld.NameTable)
	$ns.AddNamespace("DTS", "www.microsoft.com/SqlServer/Dts")

	$cons += New-Object PSObject -Property @{
		objectClass = "Project.ConnectionManagers"
		objectName = [string]$xmld.selectnodes("//DTS:ConnectionManager",$ns).ObjectName | select -first 1
		DTSID = [string]$xmld.selectnodes("//DTS:ConnectionManager",$ns).DTSID | select -first 1
		CreationName = [string]$xmld.selectnodes("//DTS:ConnectionManager",$ns).CreationName | select -first 1
		ConnectionString = [string]$xmld.selectnodes("//DTS:ConnectionManager",$ns).ConnectionString | select -first 1
		PropertyExpression = [string]$xmld.selectnodes("//DTS:ConnectionManager/DTS:PropertyExpression[@DTS:Name='ConnectionString']/text()",$ns).Value | select -first 1
	}

}

#Store into the database table

$dbserver="MyServer\MyInstance" 
$dbname="MyDW_Meta" 
$dbschema="dbo" 
$dbtablename="ssisConnectionManagers" 

write-host "truncate table [$dbschema].[$dbtablename]"
Invoke-Sqlcmd -ServerInstance $dbserver -database $dbname -Query "truncate table [$dbschema].[$dbtablename]"
write-host "insert into table [$dbschema].[$dbtablename]"
$cons | Write-SqlTableData -ServerInstance $dbserver -DatabaseName $dbname -SchemaName $dbschema -TableName $dbtablename -force