param ([string]$dtsxpacs,[switch]$noload)


#analytic/debug 
function get-node {
	param ([string] $refid)
	($exdb | where refid -eq $refid | select node).node
}

function get-propertybyname {
	param ([PSObject] $cn, [string] $prname)
	($cn.properties.property | where name -eq $prname).'#text'
}

function get-connectionManagerID {
	param ([PSObject] $cn)
	$cn.connections.connection.connectionManagerID
}

#Get column expressions
function get-DerivedColumnExpr {
	param ([PSObject] $cn)
	$inputs = $null
	$outputs = $null
	if ($cn.inputs) {
		$inputs = '{inputs:'+($cn.inputs.input.refid -join ',')+'}'
	}
	if ($cn.outputs) {
		$outexprs = $cn.outputs.output.outputcolumns.outputcolumn | where {$_.properties.property.name -eq 'expression'} | % {$_.name+'='+($_.properties.property|where name -eq 'expression').'#text'}
		#$outexprs = $cn.outputs.output.outputcolumns.outputcolumn | select @{e={$_.name+'='+($_.properties.property|where name -eq 'expression').'#text'}} -join ';'
		if ($outexprs) {
			$outputs = 'output:{'+($outexprs -join '},{')+'}'
		}
	}
	#$inputs + $outputs
	$outputs
}

#Convert most of the components into the common structure
function conv-component {
	param ([PSObject] $comp, [string] $parentref)
	
	$enumoledbsource = @( 'OpenRowset', 'OpenRowsetVariable', 'SqlCommand', 'SqlCommandVariable' )
	$enumoledbdestin = @( 'OpenRowset', 'OpenRowsetVariable', 'SqlCommand', 'OpenRowset', 'OpenRowsetVariable' ) #OpenRowset,OpenRowsetVariable,SqlCommand

	$content = $null
	$conn = $null
	$connmgrid = $null
	$cnttype = $null
	
	$cn = $comp
	if ($noload) { "Component $cn.componentClassID: '$cn.refid'" }	

	if ($cn.componentclassid -in ('Microsoft.OLEDBSource','Microsoft.OLEDBDestination')) {
		$dbaccessmode = ($cn.properties.property | where name -eq 'AccessMode').'#text'
		$cnttype = switch ($cn.componentclassid) {
			'Microsoft.OLEDBSource' { $enumoledbsource[$dbaccessmode] }
			'Microsoft.OLEDBDestination' { $enumoledbdestin[$dbaccessmode] }
		}
		$content = get-propertybyname $cn $cnttype
		$connmgrid = get-connectionManagerID $cn
	}
	elseif ($cn.componentclassid -eq 'Microsoft.OLEDBCommand') {
		$cnttype = "SqlCommand"
		$content = get-propertybyname $cn $cnttype
		$connmgrid = get-connectionManagerID $cn
	}
	elseif ($cn.componentClassID -eq 'Microsoft.Lookup') {
		$cnttype = "SqlCommandParam"
		$content = get-propertybyname $cn $cnttype
		if (!$content) {
			$cnttype = "SqlCommand"
			$content = get-propertybyname $cn $cnttype	
		}
		$conntypecd = "ConnectionType"
		$connectiontype = get-propertybyname $cn $conntypecd
		$connmgrid = get-connectionManagerID $cn	
	}
	elseif ($cn.componentClassID -eq 'Microsoft.DerivedColumn') {
		$cnttype = "custom:Expressions"
		$content = get-DerivedColumnExpr $cn
	}

	New-Object PSObject -Property @{
				objecttype = "Component"
				refid = $cn.refid
				dtsid = $cn.dtsid
				objectclass = $cn.componentClassID
				objectname  = $cn.name
				#dbaccessmode = $dbaccessmode
				contenttype = $cnttype
				content  = $content
				connmanagerid = $connmgrid
				parentref = $parentref
				node = $comp
	} 
}

#Get Variables properties and convert into to common structure
function get-variables {
	param ([System.Xml.XmlElement] $ex, [string] $parentref)
	
	foreach ($va in $ex.variables.variable) {

		if ($noload) { "Variable: '$va.refid'" }	
	
		New-Object PSObject -Property @{
					objecttype = "Variable"
					refid = $va.refid
					dtsid = $va.dtsid
					objectclass = '(DataType:'+$va.VariableValue.DataType+')'
					objectname  = $va.Namespace +'::'+ $va.ObjectName
					contenttype = "VariableValue"
					content  = ($va.VariableValue.'#text')
					connmanagerid = $null
					parentref = $parentref
					node = $va
		} 
	}	
}	

#Convert Executable Components properties to common structure
function conv-executable {
	param ([System.Xml.XmlElement] $ex, [string] $parentref)
	
	$content = $null
	$contenttype = $null
	$conn = $null

	if ($noload) { "$ex.executabletype: '$ex.refid'" }	
	
	if ($ex.executabletype -eq 'Microsoft.ExecutePackageTask') {
		$content = $ex.objectdata.ExecutePackageTask.packagename
	}
	
	if ($ex.executabletype -eq 'Microsoft.ExecuteSQLTask') {
		$contenttype = "SqlStatementSource"
		$conn = $ex.objectdata.sqltaskdata.connection
		$content = $ex.objectdata.sqltaskdata.SqlStatementSource	
	}
	
	New-Object PSObject -Property @{
				objecttype = "Executable"
			    refid = $ex.refid
				dtsid = $ex.dtsid
				objectclass = $ex.CreationName
				objectname  = $ex.ObjectName
				contenttype = $contenttype
				content  = $content
				connmanagerid = $conn
				parentref = $parentref
				node = $ex
		   } 	

	if ($ex.executabletype -eq 'Microsoft.Pipeline') {
		if ($ex.objectdata) {
			foreach ($pp in $ex.objectdata.pipeline) {
				foreach ($comp in $pp.components.component) {
					conv-component $comp $ex.refid
				}
			}
		}
	}
}

#Get Connections properties into common structure
function get-ConnectionManagers {
	param ([System.Xml.XmlElement] $ex, [string] $parentref)
	
	foreach ($cm in $ex.connectionmanagers.connectionmanager) {

		if ($noload) { "ConnectionManager: '$cm.refid'" }
	
		$cmo = $cm.objectdata.connectionmanager
		if ($cmo.connectionstring) {
			$contenttype = "ConnectionString"
			$content  = $cmo.connectionstring		
		}
		
		New-Object PSObject -Property @{
					objecttype = "ConnectionManager"
					refid = $cm.refid
					dtsid = $cm.dtsid
					objectclass = $cm.CreationName
					objectname  = $cm.ObjectName
					contenttype = $contenttype
					content  = $content
					connmanagerid = $null
					parentref = $parentref
					#node = $cm
		} 
	}		
}

#Get Executables and subordinary components. Process executables tree structures by recursion
function get-executable {
	param ([System.Xml.XmlElement] $ex, [string] $parentref)
	
	conv-executable $ex $parentref

	if ($ex.variables) {
		get-variables $ex $ex.refid
	}
	if ($ex.ConnectionManagers) {
		get-ConnectionManagers $ex $ex.refid
	}
	if ($ex.executables) {
		foreach ($exchld in $ex.executables.executable) {
			get-executable $exchld $ex.refid
		}
	}
}

#define working variables and constants
$ppath = "C:\Users\adm-xam\source\Workspaces\BI TeamProjekt\Data Art ETL Solutions Master Branch Development\Main\ETL\Optimator"

if (!$dtsxpacs) {$dtsxpacs = "$ppath\*.dtsx"}

$dbserver="MyServer\MyInstance" 
$dbname="MyDW_Meta" 
$dbschema="dbo" 
$dbtablename="ssisPkgMeta2" 

$exdb = @()

#process path containing DTSX files
foreach ($fil in (get-childitem $dtsxpacs)) {

	$dtsxfile=$fil.fullname 
	#$dtsxfile="c:\users\adm-xam\source\repos\commercialdata\monolit\etl\optimator\DW_DW_DataMigration.dtsx"
	$dtsxfilename = split-path $dtsxfile -leaf
	"Extracting metadata for [$dtsxfilename]"
	
	$xmld = [xml](Get-Content $dtsxfile)
	$ns = [System.Xml.XmlNamespaceManager]($xmld.NameTable)
	$ns.AddNamespace("DTS", "www.microsoft.com/SqlServer/Dts")
	$packroot = $xmld.executable;

	$ex = get-executable $packroot
	$ex = $ex | % {$_ | Add-Member -NotePropertyMembers @{Package=$dtsxfilename} -PassThru}
	
	$exdb += $ex 
}

# use dot-sourcing with noload if you want to analyze collected objects. use get-node with component reference-id to deep-dive into the selected component
if (!$noload) {
	$exdb = $exdb | select Package, objecttype, refid, parentref, dtsid, objectclass, objectname, contenttype, content, connmanagerid
	#Store into the database table
	write-host "truncate table [$dbschema].[$dbtablename]"
	Invoke-Sqlcmd -ServerInstance $dbserver -database $dbname -Query "truncate table [$dbschema].[$dbtablename]"
	write-host "insert package data into table [$dbschema].[$dbtablename]"
	#$exdb |gm -membertype noteproperty
	$exdb | Write-SqlTableData -ServerInstance $dbserver -DatabaseName $dbname -SchemaName $dbschema -TableName $dbtablename #-force
}