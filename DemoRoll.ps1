[CmdletBinding()]
param (
    [Parameter(Mandatory)][string]$YearTerm,
    [Parameter()][string]$CourseCode,
    [Parameter()][string]$CsvFile
)

Import-Module TC3SqlQuery
$Global:ConnectionString = 'Data Source=l-mc-it-mdzbk;Initial Catalog=TmseDemo;Integrated Security=true'
$Global:SqlDebug = $true

function Get-Roll {
    param($YearTerm, $CourseCode)
    $qry = @"
   SELECT s.yr_cde + s.trm_cde [YearTerm],
    LEFT(s.crs_cde, 4) + SUBSTRING(s.crs_cde, 6, 4) + '-' + TRIM(SUBSTRING(s.crs_cde, 11, 5)) + '-' + TRIM(SUBSTRING(crs_cde, 16, 5)) [Course],
    crs_cde [RawCourse],
    n.last_name + ', ' + COALESCE(n.preferred_name, n.first_name, '') + ISNULL(' '+ UPPER(LEFT(n.middle_name,1)),'') [Name]
   FROM student_crs_hist s
   JOIN namemaster n ON n.id_num = s.id_num
   WHERE s.transaction_sts IN ('C','H') 
    AND s.yr_cde = @year AND s.trm_cde = @term
    AND s.crs_cde LIKE @course + '%'
   ORDER BY s.crs_cde, [Name]
"@
    $params = @{ 'year' = $YearTerm.Substring(0, 4); 
                 'term' = $YearTerm.Substring(4, 2);
                 'course' = $CourseCode }
    $crslst = Invoke-SqlQuery -Reader -QueryStr $qry -SqlParams $params
    if ($CsvFile.Length -gt 0) {
        $crslst.Rows `
         | Select-Object YearTerm, Course, Name `
         | ConvertTo-Csv -IncludeTypeInformation:$false -UseQuotes AsNeeded `
         | Out-File $CsvFile
    } else {
        $last = $clr = ''
        foreach ($c in $crsLst) {
            if ($last -ne $c.Course) {
                $last = $c.Course
                $clr = @("Yellow","Green")[$clr -eq "Yellow"]
            }
            Write-Color "<$clr>$($c.Course) <Cyan>$($c.Name)"
        }
    }
}

Get-Roll $YearTerm $CourseCode
