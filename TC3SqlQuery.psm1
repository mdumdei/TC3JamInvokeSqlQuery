<#
    TC3SqlQuery Module
     Mike Dumdei, Texas Community College Consortium
     May be freely used by Jenzabar and Jenzabar customers
    Rev 060123: Created for Jenzabar JAM 2023
#>

# Data structures for holding SQLDebug information
Class SqlDebug { $idx; $srv; $db; $cmd; $params; $data; 
    SqlDebug($srv, $db, $c, $p) { 
        $this.idx = $Global:SqlHist.Count; $this.data = $null;
        $this.srv = $srv; $this.db = $db; $this.cmd = $c; $this.params = $p; 
        $Global:SqlHist.Add($this)
    } 
}
$Global:SqlHist = New-Object System.Collections.Generic.List[SqlDebug]

function Open-SqlConnection {
<#
.SYNOPSIS
    Open a connection to a SQL server.
.DESCRIPTION
    Open an SQL connection to a server. Invoke-SqlQuery automatically opens and closes connections using 1) the ConnectionString parameter, or 2) the Server and Database parameters. Calling Open-SqlConnection directly is only necessary if you want to pass Invoke-SqlQuery an open connection via the -SqlConnection parameter.
.PARAMETER ConnectionString
    Connection string to use for the connection. Credential may be embedded or passed in the Credential parameter. Integrated Security will NOT be automically added if using a connection string -- only SqlCred paramaters.
.PARAMETER SqlCreds
    Credential for connection, if not provided, Integrated Security (currently logged in user) is automatically added for Server/DB specified connections.
.PARAMETER SqlServer
    If not using a connection string, this is the server for the connection.
.PARAMETER Database
    If not using a connection string, this is the database for the connection.
.INPUTS
    None.
.OUTPUTS
    SqlConnection, Exception.
.EXAMPLE
    PS:\>$connStr = "Server=$srv1;Database=$db;"
    PS:\>$conn = Open-SqlConnection -ConnectionString $connStr -SqlCreds $creds

    Open an SQL connection using a connection string. Credentials are passed separately. You can also embed them in the connection string. 
.EXAMPLE
    PS:\>$conn = Open-SqlConnection -Server Srv1 -Database DB1 -Credential $creds

    Open an SQL connection to Srv1 with the default database set to DB1.
.EXAMPLE
    PS:\>$connStr = "Server=$srv1;Database=$db;MultipleActiveResultSets=true;User ID=$user;Password=$pass;"
    PS:\>$conn = Open-SqlConnection -ConnectionString $connStr

    Open an SQL connection using a connection string and a plaintext password stored in a PS variable.
.NOTES
    Author: Mike Dumdei
#>
    [OutputType([System.Data.SqlClient.SQLConnection])]
    [CmdletBinding(DefaultParameterSetName='ConnectionString')]
    param (
        [Parameter(ParameterSetName='ConnectionString',Mandatory)][string]$ConnectionString,
        [Parameter(ParameterSetName='ServerDB',Mandatory)][string]$SqlServer,
        [Parameter(ParameterSetName='ServerDB',Mandatory)][string]$Database,
        [Parameter()][PSCredential]$SqlCreds,
        [Parameter()][switch]$Silent
    )
    if ([string]::IsNullOrEmpty($ConnectionString) -eq $false) {
        $intCreds = ""
        $connStr = $ConnectionString
    } else {
        $intCreds = ";Integrated Security=true"
        $connStr = "Data Source=$SqlServer;Initial Catalog=$Database"
    }
    if ($null -ne $SQLCreds) {
        $connStr = $connStr.Trim(';') + ";User ID=$($SqlCreds.UserName);Password=$($SqlCreds.GetNetworkCredential().Password)"
    } else {
        $connStr += $intCreds
    }
    try {
        [System.Data.SqlClient.SQLConnection]$conn = New-Object System.Data.SqlClient.SQLConnection($connStr)
        $conn.Open()
        return $conn
    } catch {
        if ($Silent -eq $false) {
            Write-Host -Foreground Red `
             "SQL connection to $SQLServer/$Database failed: $_"
        }
        return $null
    }
}

function Invoke-SqlQuery {
<#
.SYNOPSIS
    Execute a Reader, Scalar, or NonQuery SQL query with optional capture to a trace log.
.DESCRIPTION
    The purpose of the Invoke-SqlQuery command is 1) to centralize SQL calls a script makes to a single function, and 2) add the ability to trace SQL commands and query results obtained during execution of the script. Invoke-SqlQuery processes all 3 of the basic query types: Reader, Scalar, and NonQuery. Reader queries are implemented as SQL ExecuteSqlReader calls, Scalars as ExecuteScalar, and NonQuerys as ExecuteNonQuery.

    Invoke-SqlQuery supports paramertized queries, table value parameter queries, and both text and stored procedure query requests. To run a stored procedure, begin the query text with 'EXEC' followed by a space. To add parameters to a SQL query, use standard @arg1, @val notation in the Query text followed by a -SqlParams @{ arg1 = 'Sales'; val = 'Qtr1' } Invoke-SqlQuery parameter to specify the values.

    -- Tracing --
    Setting the $Global:SqlDebug to $true activates an in-memory trace of each query processed by Invoke-SqlQuery. Trace items are PSCustomObjects that contain the server (Srv) and database (DB) accessed, the query text (Cmd), query parameters (Parms), and the resulting output (Data). Trace information can be accessed as objects ($Global:SqlHist) or as string items suitable for viewing on the console or writing to a text file using Write-SqlHist. 

    -- Connections --
    SQL connections can be passed as either an already open SQLConnection object, a ConnectionString, or as a the SQL server and database you want to connect to. 

    -- Credentials --
    Credentials can be embedded in the connection string or passed separately as an argument. If no credential is supplied the connection is created as the logged in user.
.PARAMETER Reader
    Switch parameter identifying the query returns tabular results.
.PARAMETER Scalar
    Switch parameter identifying the query returns a single data value.
.PARAMETER NonQuery
    Switch parameter identifying the query does not return values from the database. Use for INSERT, UPDATE, DELETE statements. Returns number of rows affected.
.PARAMETER QueryStr
    The query string for the query. Precede the 'EXEC ' or 'EXECUTE ' to run a stored procedure.
.PARAMETER SqlParams
    Parameter table if using parameterized queries or a stored procedure. Pass as key/value pairs (hashtable).
.PARAMETER CommandTimeOut
    Time in seconds before the query times out. Use for long running queries.
.PARAMETER ConnectionString
    The connection string to use to connect to SQL for the query. Can be preset via $Global:ConnectionString = "connectionstring".
.PARAMETER SqlConnection
    An existing open SqlConnection object to use for the query. If re-using connections your connection may require the MulipleActiveResultSets option in the initial connection string. Can be preset via $Global:SqlConnection = connectionobject.
.PARAMETER SqlServer
    Server to connect to for the query (in place of a connection or connection string). Can be preset via $Global:SqlServer = "server".
.PARAMETER Database
    Database to connect to for the query (in place of a connection or connection string). Can be preset via $Global:Database = "database".
.PARAMETER SqlCreds
    A PSCredential object containing the username and password to use for the connection. Can be preset via $Global:SqlCreds = pscredentialobject.
.INPUTS
    None.
.OUTPUTS
    A DataSet (when multiple tables are returned), DataTable (when 1 table is returned), or returned object for non-tabular queries.
.EXAMPLE
    PS:\>$qry = "SELECT FirstName, LastName, Department FROM EmpTable WHERE Department = @dept"
    PS:\>$data = Invoke-SqlQuery -Reader -QueryStr $qry -Params @{ 'dept' = "Finance" } -SqlServer Srv1 -Database EmpDB

    Run a 'Reader' TEXT query using a parameterized argument and Integrated Security.
.EXAMPLE
    PS:\>$Global:ConnectionString = "Data Source=Srv1;Initial Catalog=EmpDB"
    PS:\>$Global:SqlCredential = Get-Credential -Message "Enter credentials for SQL access:"
    PS:\>$qry = "SELECT FirstName, LastName, Department FROM EmpTable WHERE Department = @dept"
    PS:\>$data = Invoke-SqlQuery -Reader -QueryStr $qry -Params @{ 'dept' = "Finance" } 

    Using globals for connection string and creds. Advantage is fewer args required for this Invoke-SqlQuery call and subsequent calls. Use of globals also works to persist ConnectionString, SqlCreds, and SqlConnection parameters.
.EXAMPLE
    PS:\>$qry = "EXEC sp_DoStuff @yr, @dept"
    PS:\>$connStr = "Data Source=srv1;Initial Catalog=erpDB;"
    PS:\>$parms = @{ @yr = 2024; 'dept' = "Finance" }
    PS:\>$data = Invoke-SqlQuery -Reader -QueryStr $qry -Params $parms -ConnectionString $connStr -SqlCreds $cred

    Run a 'Reader' stored procedure using a connection string without embedded credentials and passing credentials as a separate parameter.
.EXAMPLE
    PS:\>$topSal = Invoke-SqlQuery -Scalar -SqlQuery "SELECT MAX(Salary) FROM EmpTable WHERE Department = 'Sales'" -SqlConnection $conn

    Run a Scalar query to find the top salary being paid to a Sales employee using an existing open connection.
.NOTES
    Author: Mike Dumdei
#>    
    [CmdletBinding()]
    [OutputType([Object], ParameterSetName='Scalar')]    
    [OutputType([Int32], ParameterSetName='NonQuery')]    
    [OutputType([Object], ParameterSetName='Reader')]
    Param (
        [Parameter(ParameterSetName='Reader',Mandatory)][Switch]$Reader,
        [Parameter(ParameterSetName='Scalar',Mandatory)][Switch]$Scalar, 
        [Parameter(ParameterSetName='NonQuery',Mandatory)][Switch]$NonQuery, 
        [string]$QueryStr, 
        [Object]$SqlParams, 
        [int]$CmdTimeOut = 0,
        [string]$ConnectionString,
        [System.Data.SqlClient.SqlConnection]$SqlConnection,
        [string]$SqlServer, [string]$Database, [PSCredential]$SqlCreds,
        [Switch]$Silent
    )
    if ([String]::IsNullOrWhiteSpace($QueryStr)) {
        return $null
    }
    $closeConn = $true
    if ($null -ne $SqlConnection) {
        $conn = $SqlConnection
        $closeConn = $false
    } elseif ($null -ne $Global:SqlConnection) {
        $conn = $Global:SqlConnection
        $closeConn = $false
    } else {
        if ($SqlCreds.Length -eq 0 -and $Global:SqlCreds.Length -ne 0) { $SqlCreds = $Global:SqlCreds }
        if ($ConnectionString.Length -eq 0) { $ConnectionString = $Global:ConnectionString }
        if ($ConnectionString.Length -eq 0) {
            if ($SqlServer.Length -eq 0) { $SqlServer = $Global:SqlServer }
            if ($Database.Length -eq 0) { $Database = $Global:Database }
            $ConnectionString = "Data Source=$SqlServer;Initial Catalog=$Database"
            if ($SqlCreds.Length -eq 0) {
                $ConnectionString += ";Integrated Security=true"
            }
        }
        if ($SqlCreds.Length -gt 0) {
            $ConnectionString = $ConnectionString.Trim(';') + ";User ID=$($SqlCreds.UserName);Password=$($SqlCreds.GetNetworkCredential().Password)"
        }
        $conn = Open-SqlConnection -ConnectionString $ConnectionString -Silent:$Silent
    }   
    if ($null -eq $conn) {
        throw $("SQL connection [${ConnectionString}] failed" -replace "Password=[^;]+", "Password=*pass*")
    }
    try {  
        [System.Data.SqlClient.SqlCommand]$cmd = $conn.CreateCommand()
        $cmd.CommandTimeout = $CmdTimeOut
        $QueryStr = $QueryStr.Trim()
        if ( $QueryStr.Substring(0, 5) -eq 'EXEC ' -or $QueryStr.Substring(0, 8) -eq 'EXECUTE ') {
            $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
            $QueryStr = $QueryStr.Substring($QueryStr.IndexOf(' ') + 1)
        } else {
            $cmd.CommandType = [System.Data.CommandType]::Text
        }
        $cmd.CommandText = $QueryStr
        if ($SqlParams.Count -gt 0) {
            $SqlParams.GetEnumerator() | ForEach-Object {
                $val = $_.Value
                $valType = $val.GetType().Name
                if ($valType -eq 'DataTable') {  # table value parameter
                    $param = New-Object System.Data.SqlClient.SqlParameter("@$($_.Key)", [System.Data.SqlDbType]::Structured)
                    $param.Value = $val
                    $cmd.Parameters.Add($param) | Out-Null
                } elseif ($valType -in @('SwitchParameter','Boolean')) {
                    $val = @(0, 1)[$val -eq $true]
                } else {
                    $cmd.Parameters.AddWithValue("@$($_.Key)", $($_.Value)) | Out-Null
                }
            }
        }
        if ($Global:SqlDebug) {
            New-Object SqlDebug $conn.DataSource, $conn.Database, $QueryStr, $SqlParams | Out-Null
        }
        if ($Reader) {                      # returns DataSet
            [System.Data.DataSet]$dset = New-Object System.Data.DataSet
            [System.Data.SqlClient.SqlDataAdapter]$da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
            $da.Fill($dset) | Out-Null
            if ($dset.Tables.Count -eq 1) {
                $rval = $dset.Tables[0]
            } else {
                $rval = $dset
            }
            if ($Global:SqlDebug) { $Global:SQLHist[$SQLHist.Count - 1].data = $rval }
            return , $rval
        } elseif ($NonQuery) {              # returns count of rows affected
            $rval = $cmd.ExecuteNonQuery()
        } else {                            # pull single value
            $rval = $cmd.ExecuteScalar()
        } 
        if ($Global:SqlDebug) { $Global:SQLHist[$SQLHist.Count - 1].data = $rval }
        return $rval
    } catch { 
        if ($Silent -eq $false) {
            Write-Host
            Write-Host -ForegroundColor Red '-- Error executing SQL command:'
            Write-Host $QueryStr
            Write-Host -ForegroundColor Red '-------------------------------'
            Write-Host -ForegroundColor Red $Error[0].ToString()
        }
        if ($Global:SqlDebug) { $Global:SQLHist[$SQLHist.Count - 1].data = $_.Message }
        throw $_   # re-throw so error is propagated and script aborted if not handled
    } finally { if ($null -ne $conn -and $closeConn -eq $true) { $conn.Close() | Out-Null; $conn.Dispose() | Out-Null }}
}


Function Convert-ListToDataTable {
<#
.SYNOPSIS
    Converts a PSCustomObject or a simple one column array object to a DataTable.
.DESCRIPTION
    Conversion utility to convert PSCustomObjects, such as imported CSV files, or single column arrays into a DataTable object. The objective is to provide a mechanism to be able to use those objects as sources for table value parameters.
.PARAMETER ary
    The item to be convert.
.PARAMETER colName
    If the item being converted is a PSCustom object, the column names are derived from the object. For single column arrays, however, this parameter allows you to assign a field name to the result.
.OUTPUTS
    DataTable.
.EXAMPLE
    PS:\>$data = Import-Csv file.csv
    PS:\>$tbl = Convert-ListToDataTable $data
    PS:\>Invoke-SqlQuery -Reader -QueryStr "EXEC sp_xyz" -SqlParams @{ spParam1 = $tbl } -SqlConnection $conn

    Convert a CSV to a table value parameter and pass it as a parameter to a stored procedure. Requires table type to be created and a stored procedure that takes that TVP type as a parameter.
    SQL side requirements for this example:
      CREATE TYPE myTvpType AS TABLE(last varchar(50), first varchar(30))
      GO
      CREATE PROCEDURE sp_xyz @spParam1 myTvpType READONLY AS 
      BEGIN
        SELECT id_num, last_name, first_name 
        FROM namemaster n JOIN @spParam1 p ON n.last_name = p.last AND n.first_name = p.first
      END
      GO
      GRANT EXEC ON sp_xyz TO [** you **]
      GRANT EXEC ON TYPE::myTvpType TO [** you **]
.NOTES
    Author: Mike Dumdei
#>
    param($ary, $colName = 'col1')
    [System.Data.DataTable]$tbl = New-Object System.Data.DataTable
    $dataType = $ary[0].GetType().Name
    if ($dataType -eq 'DataTable') {
        return @(,$ary)
    }
    if ($dataType -eq 'PSCustomObject') { 
        $ary[0].PSObject.Properties | ForEach-Object {
            $tbl.Columns.Add($(New-Object System.Data.DataColumn($_.Name, $_.Value.GetType())))
        }
        $colNames = $tbl.Columns.ColumnName
        foreach ($itm in $ary) {
            [System.Data.DataRow]$r = $tbl.NewRow()
            $colNames | ForEach-Object { 
                $r[$_] = $itm.($_) 
            }
            $tbl.Rows.Add($r)
        }
    } else {  # this assumes single column array with same data type for all elements
        $tbl.Columns.Add($(New-Object System.Data.DataColumn($colName, $ary[0].GetType().Name)))
        $ary | ForEach-Object {
            [System.Data.DataRow]$r2 = $tbl.NewRow()
            $r2[$colName] = [Convert]::ChangeType($_, $_.GetType())
            $tbl.Rows.Add($r2)
        }
    }
    # without the comma, the DataTable gets converted to an array of PSObjects
    # when returned - defeats the purpose
    return @(,$tbl) 
}

function Get-TvpTypeDDL {
<#
.SYNOPSIS
    Generates the DDL to make a table value parameter type.
.DESCRIPTION
    Given a name for the TVP type and a DataTable, this will create the DDL to create a TVP type for the table. Typical use is convert a PSCustomObject or imported CSV to a DataTable (Convert-ListToDataTable) and use that DataTable as the input to this function.
.PARAMETER TvpName
    The name for the table value type. It may be proceeded with a schema if not using 'dbo'. These are created in SSMS under 'DBName\Programmability\Types\User-Defined Table Types'.
.PARAMETER Table
    The datatable with the data elements you are wanting to use as the source structure for the DDL.
.PARAMETER WithNoReplace
    Prevents overwriting an existing TVP if one exists with the same name in the same schema.
.PARAMETER WithDrop
    Drops the TVP if it exists and recreates it with the new definition.
.PARAMETER szVarchar
    The size argument for the VARCHAR when converting String values - defaults to 'max', 512 may be more appropriate.
.PARAMETER szDecimal
    The size argument for DECIMAL when converting Decimal values - defaults to '18,2' - the default is probably way to big.
.EXAMPLE
    PS:\> Get-TvpTypeDLL -TvpName 'dbo.myTvpType' -Table $someTable -WithDrop -szVarchar 512 -szDecimal '10,2'

    Generates the DLL statements to create a Table type for use with table valued parameters.
.NOTES
    Author: Mike Dumdei    
#>    
    param([string]$TvpName, [System.Data.DataTable]$Table, [switch]$WithNoReplace, [switch]$WithDrop, $szVarchar = 'max', $szDecimal = '18,2')
    $map = @{ 'String' = "varchar($szVarchar)"; 'Decimal' = "decimal($szDecimal)"
              'Int32' = "int"; 'Int64' = "bigint"; 'Double' = "float"; 'Single' = "float"; 
              'DateTime' = "dateTime"; 'TimeSpan' = "time"; 'Boolean' = "bit"; 'Guid' = "uniqueidentifier" }
    [string[]]$sAry = $TvpName.Replace("[","").Replace("]","").Split('.')
    if ($sAry.Length -lt 2) {
        $TvpSchema = 'dbo'
    } else {
        $TvpSchema = $sAry[0]
        $TvpName = $sAry[1]
    }
    $ddl = ""
    if ($WithNoReplace) {
        $ddl += `
         "IF NOT EXISTS (SELECT 1 FROM sys.table_types t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = '$TvpName' AND s.name = '$TvpSchema')`r`n"
    }
    if ($WithDrop) {
        $ddl += "DROP TYPE IF EXISTS [$TvpSchema].[$TvpName]`r`n"
    }
    $ddl += "CREATE TYPE [$TvpSchema].[$TvpName] AS TABLE ("
    foreach ($c in $Table.Columns) {
        $ddl += "`r`n  [$($c.ColumnName)] $($map[($c.DataType).Name]),"
    }
    $ddl = $ddl.TrimEnd(',') + "`r`n)"
    return $ddl
}


function Write-SQLHist {
<#
.SYNOPSIS
    Writes $SQLHist to a file or the console if no file given. The -ExpandTables switch will cause data in 'Reader' results to be expanded to show all content.
.DESCRIPTION
    This outputs trace data in a user friendly view suitable for viewing on the console or sending to a log file. To enable tracing set $Global:SqlDebug = $true. The raw trace data as a list of objects is stored in $Global:SqlHist.
.PARAMETER LogFile
    Write the trace data to the specified file.
.PARAMETER ExpandTables
    Expand the content of Reader results showing all rows and columns of data. Without this switch, a summary of the data is displayed.
.NOTES
    Author: Mike Dumdei
#>    
    param($LogFile, [Switch]$ExpandTables)
    foreach ($r in $SqlHist) {
        $pStr = ''
        if ($r.params.Count -gt 0) {
            $pStr = '@{ '
            $r.params.GetEnumerator() | ForEach-Object {
                if ($pStr -ne '@{ ') { $pStr += '; ' }
                if (($_.value).GetType().Name -ne 'DataTable') {
                    $pStr += "'$($_.key)'=`"$($_.value)`"" 
                } else {
                    $pStr += "'$($_.key)'= {{Table Value Parameter}} " 
                }
            }
            $pStr += ' }'
        }
        $dataType = $r.data.GetType().Name
        if ($dataType -ne 'DataTable' -and $dataType -ne 'DataSet') {
            $dStr = $r.data
        } else {
            $tblIdx = -1
            $dStr = $tStr = ""
            while ($true) {
                $dStr += $tStr
                ++$tblIdx
                if ($dataType -eq 'DataTable' -and $tblIdx -eq 0) {
                    $tbl = $r.data
                } elseif ($dataType -eq 'DataSet' -and $tblIdx -lt $r.data.Tables.Count) { 
                    if ($tblIdx -gt 0) { 
                        $dstr += "`r`n         "
                    }
                    $tbl = $r.data.Tables[$tblIdx]
                } else {
                    break
                }
                $rows = $tbl.Rows.Count
                $cols = $(,$tbl).Columns.Count
                $tStr = "[r=$rows,c=$cols] { "
                if ($cols -gt 0 -and $rows -gt 0) {
                    if ($cols -eq 1) { $more = ']'} else { $more = ',.]'}
                    if ($rows -gt 1) {
                        for ($i = 0; $i -lt $rows -and $tStr.Length -lt 70; $i++) {
                            if ($i -ne 0) { $tStr += ', ' }
                            $tStr += "[$($($($tbl)[$i][0]).ToString())$more"
                        }
                        if ($i -lt $rows) { $tStr += ', ...' }
                    } else {
                        $tStr += '['
                        for ($i = 0; $i -lt $cols -and $tStr.Length -lt 60; $i++) {
                            if ($i -ne 0) { $tStr += ', ' }
                            $tStr += "$($($($tbl)[$i]).ToString())"
                        }
                        $tStr += @(']', ',...]')[$i -ne $cols]
                    }
                }
                $tStr += ' }'
            }
        }
        $s = @"
idx    : $($r.idx)
srv    : $($r.srv)
db     : $($r.db)
cmd    : $($r.cmd)
params : $pStr
data   : $dStr
"@
        if ($null -ne $LogFile) { 
            $s | Out-File $LogFile -Append
        } else { 
            $s | Out-Host 
        }
        if ($ExpandTables -and ($dataType -eq 'DataTable' -or $dataType -eq 'DataSet')) {
            $tblIdx = -1
            while ($true) {
                ++$tblIdx
                if ($dataType -eq 'DataTable' -and $tblIdx -eq 0) {
                    $tbl = $r.data
                } elseif ($dataType -eq 'DataSet' -and $tblIdx -lt $r.data.Tables.Count) { 
                    $tbl = $r.data.Tables[$tblIdx]
                } else {
                    break
                }
                if ($null -ne $LogFile) {
                    $tbl | ConvertTo-Csv -NoTypeInformation | Out-File $LogFile -Append
                } else {
                    $tbl | ConvertTo-Csv -NoTypeInformation | Out-Host
                }
            }
        }
        if ($LogFile) {
            [System.Environment]::NewLine | Out-File $LogFile -Append
        } else {
            [System.Environment]::NewLine | Out-Host
        }
    }
}

function Write-Color {
<#
.SYNOPSIS 
    Multi-featured Write to console function with a primary objective of easily using color in messages.
.DESCRIPTION
    A write to console function supporting embedded color codes, word wrap, timestamps, and parameterized strings with arguements. It makes adding color to scripts much easier and comes with an option to strip out the embedded coding for redirecting to a log file. This has nothing to do with SQL, but is a handy function if you like nice looking screen output.
.PARAMETER Message
    The text to output. It can contain embedded color strings as well as {0}, {1} -f type parameter values. 
.PARAMETER ArgList
    Array of values to be inserted into the string at {0}, {1} type designated format codes found in the Message parameter.
.PARAMETER NoNewline
    Suppress the newline after writing the text.
.PARAMETER TimeStamp
    Prefix the text with a timestamp.
.PARAMETER ReturnText
    Returns a version of the text with the embedded color strings removed (plaintext version).
.PARAMETER FromLog
    Indicates called from a Log to file type function. Returns a plain text copy suitable for log output as the first element of a 2-element string array and a timestamp as the second element.
.PARAMETER WordWrap
    Word wraps the message to a specified width.
.EXAMPLE
    PS:\>Write-Color "<Cyan>Data: <Yellow>$Value"

    Outputs a string of text with the tag in cyan and data value in yellow.
.EXAMPLE
    PS:\>Write-Color -TimeStamp -NoNewline "<Magenta>Process [<Yellow>{0}<Magenta>] starting: " -ArgList "My Process"
    ....
    PS:\>Write-Color "<Green>Completed"

    Outputs a notification of a process starting and adds completed in green when it "finishes".
.EXAMPLE
    PS:\>$c1 = 'Cyan'; $c2 = 'Yellow'; $width = 30
    PS:\>Write-Color "<{0}>Long message that want to word wrap at <{1}>{2}<{0}> characters with the text color determined at runtime." -ArgList $c1,$c2,$width -WordWrap $width

    Word wrap example with runtime determination of the width and colors used.
.EXAMPLE
    PS:\>$rawText = Write-Color -TimeStamp "<Cyan>Something happened on: <Yellow>{0}" -ReturnText -ArgList $sysName
    PS:\>$rawText | Out-File "MyLog.txt" -Append

    Example capturing the processed text and writing it to a log file. The timestamp and evaluated {0} parameter are logged, but the color tags are stripped out.
.NOTES
    Author: Mike Dumdei
#>    
    param ($Message, $ArgList = $null, [Switch]$NoNewline, [Switch]$TimeStamp, [Switch]$ReturnText, [Switch]$FromLog, $WordWrap = 0)
    [string[]]$ConsoleColors = @('white', 'cyan', 'yellow', 'green', 'red', 'magenta', 'blue', 'gray',
      'darkcyan', 'darkyellow', 'darkgreen', 'darkred', 'darkmagenta', 'darkblue', 'darkgray')
    if ($null -ne $ArgList) {
        $Message = [String]::Format($Message, $ArgList)
    }
    $plainTxt = $Message -replace "<($($ConsoleColors -join '|'))>"
    $tm = "[$(Get-Date -Format 'MM-dd-yy HH:mm:ss')] "
    $rval = @((@($plainTxt, $tm), $plainTxt)[!$FromLog], $null)[!$ReturnText -and !$FromLog]
    if ($Silent) { return $rval }
    if ($TimeStamp) { 
        $plainTxt = "${tm}${plainTxt}"
        $Message = "${tm}${Message}"
    }
    if ($WordWrap -gt 0) {
        $s = $plainTxt; $lines = @(); $k = 0
        while ($true) {
            if ($s.Length -le $WordWrap) { $lines += ,$s; break }        
            $k = $s.LastIndexOf(' ', $WordWrap)
            if ($k -lt 0) { $k = $WordWrap }
            $lines += ,$s.Substring(0, $k)
            $s = $s.Substring($k)
            if ($s[0] -eq ' ') { $s = $s.Substring(1) }
        }
    }
    $sAry = $Message -split "(?=<\w+>)"
    $i = $k = 0; $ln = ""; $fgClr = "White"
    while ($i -lt $sAry.Count) {
        $s = $sAry[$i++]
        if ($s.StartsWith("<")) {
            $j = $s.IndexOf(">")
            $clr = $s.Substring(1, $j - 1)
            if ($ConsoleColors.Contains($clr.ToLower())) {
                $fgClr = $clr; $s = $s.Substring($j + 1)
            }
        }
        if ($WordWrap -le 0) {
            Write-Host -NoNewline -ForegroundColor $fgClr $s
        } else {
            $m = $ln.Length; $ln += $s 
            do {
                if ($ln.Length -le $lines[$k].Length) {
                    Write-Host -NoNewline -ForegroundColor $fgClr $s
                    break               
                } else {
                    $toWrite = $s.Substring(0, $lines[$k].Length - $m)
                    Write-Host -ForegroundColor $fgClr $toWrite
                    $s = $s.Substring($toWrite.Length)
                    if ($s[0] -eq ' ') { $s = $s.Substring(1) } 
                    $m = 0; $ln = $s; $k++
                } 
            } while ($s.Length -gt 0)
        }
    }
    if ($NoNewline -eq $false) { Write-Host }
    if ($null -ne $rval) { return $rval }
}


Export-ModuleMember -Function Open-SqlConnection, Invoke-SqlQuery, Convert-ListToDataTable, Get-TvpTypeDDL, Write-SQLHist, Write-Color
