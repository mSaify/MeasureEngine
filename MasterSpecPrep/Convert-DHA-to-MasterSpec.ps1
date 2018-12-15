param(
    $Server = "ASHNPDWMDB-Q01",
    $DB = "DIW_GREENISLAND_REPORTING",
    $EncounterLimit = 100000
)

$ErrorActionPreference = "STOP"

function RunQuery($sql) {
    $cn = [System.Data.SqlClient.SqlConnection]::new("Server=$Server; Initial Catalog=$DB; Integrated Security=True;")
    try {
        $cn.Open()
        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($sql, $cn)
        $table = [System.Data.DataTable]::new()
        $adapter.Fill($table) | Out-Null
        return $table.Rows
    } finally {
        $cn.Close()
    }
}

function dt($row, $name) {
    if($row[$name] -eq $null) {
        ""
    } else {
        $row[$name].ToString("yyyy-MM-dd")
    }
}

RunQuery @"
; with NumberedEncounters as (
    select EncounterKey, row_number() over (order by EncounterKey) as RowNum from dbo.DimEncounter
),
Top100KEncounters as (
    select EncounterKey from NumberedEncounters where RowNum <= $EncounterLimit
)
select
    E.EncounterKey as EncounterId,
    E.PatientMasterKey as PatientId,
    E.AdmissionDate as AdmissionDatetime,
    E.DischargeDate as DischargeDatetime,
    case E.SourceProduct 
        when 'CCC_OP' then 'O' 
        when 'CCC_IP' then 'I' 
        else null end as InpatientOutpatientFlag, 
    DAS.AdmissionSourceCode as AdmissionSource,
    DAT.AdmissionTypeCode as AdmissionType,
    DD.DispositionCode as DischargeDisposition,
    MSDRG.MsDrgCode as MSDRG,
    APRDRG.AprDrgCode as APRDRG
from dbo.DimEncounter E
left join dbo.DimAdmissionSource DAS on E.AdmissionSourceKey = DAS.AdmissionSourceKey
left join dbo.DimAdmissionType DAT on DAS.AdmissionTypeKey = DAT.AdmissionTypeKey
left join dbo.DimDischargeDisposition DD on E.DischargeDispositionKey = DD.DischargeDispositionKey
left join dbo.DimMsDrg MSDRG on E.MsDrgKey = MSDRG.MsDrgKey
left join dbo.DimAprDrg APRDRG on E.AprDrgKey = APRDRG.AprDrgKey
where EncounterKey in (select EncounterKey from Top100KEncounters)
"@ | Foreach-Object -Begin {
    "EncounterID|PatientID|AdmissionDatetime|DischargeDatetime|InpatientOutpatientFlag|AdmissionSource|AdmissionType|DischargeDisposition|MSDRG|APRDRG"
} -Process {
    "$($_["EncounterId"])|$($_["PatientId"])|$(dt $_ "AdmissionDatetime")|$(dt $_ "DischargeDatetime")|$($_["InpatientOutpatientFlag"])|$($_["AdmissionSource"])|$($_["AdmissionType"])|$($_["DischargeDisposition"])|$($_["MSDRG"])|$($_["APRDRG"])"
} | Out-File (Join-Path $PSScriptRoot "EncountersPatientBilling00001.txt") -Encoding utf8

RunQuery @"
; with NumberedEncounters as (
    select EncounterKey, row_number() over (order by EncounterKey) as RowNum from dbo.DimEncounter
),
Top100KEncounters as (
    select EncounterKey from NumberedEncounters where RowNum <= $EncounterLimit
)
select
    P.PatientMasterKey as PatientID, 
    P.PatientName,
    P.DateOfBirth as DateofBirth,
    case P.Gender when 'U' then 'UN' else P.Gender end as Gender
from dbo.DimPatientMaster P
inner join dbo.DimEncounter E on P.PatientMasterKey = E.PatientMasterKey
where E.EncounterKey in (select EncounterKey from Top100KEncounters)
"@ | Foreach-Object -Begin {
    "PatientID|FirstName|LastName|DateofBirth|Gender"
} -Process {
    $name = @($_["PatientName"].ToString().Split(","))
    $lastName = $name[0].Trim()
    $firstName = if($name.Length -gt 1) { $name[1].Trim() } else { "" }
    "$($_["PatientId"])|$firstName|$lastName|$(dt $_ "DateofBirth")|$($_["Gender"])"
} | Out-File (Join-Path $PSScriptRoot "PatientDemographicsPatientBilling00001.txt") -Encoding utf8

RunQuery @"
; with NumberedEncounters as (
    select EncounterKey, row_number() over (order by EncounterKey) as RowNum from dbo.DimEncounter
),
Top100KEncounters as (
    select EncounterKey from NumberedEncounters where RowNum <= $EncounterLimit
)
select
    D.EncounterKey as EncounterId,
    D2.Code as Code,
    CS.CodeSystemDesc as CodeSystem,
    D.DiagnosisSequence as Sequence,
    POA.POACode as PresentOnAdmission
from dbo.RelEncounterDiagnosis D
    left join dbo.DimDiagnosis D2 on D.DiagnosisKey = D2.DiagnosisKey
    left join dbo.DimCodeSystem CS on D2.CodeSystemKey = CS.CodeSystemKey
    left join dbo.DimPresentOnAdmission POA on D.POAKey = POA.POAKey
where D.EncounterKey in (select EncounterKey from Top100KEncounters)
"@ | Foreach-Object -Begin {
    "EncounterID|Code|CodeSystem|Sequence|PresentOnAdmission"
} -Process {
    "$($_["EncounterId"])|$($_["Code"])|$($_["CodeSystem"])|$($_["Sequence"])|$($_["PresentOnAdmission"])"
} | Out-File (Join-Path $PSScriptRoot "DiagnosesPatientBilling00001.txt") -Encoding utf8


RunQuery @"
; with NumberedEncounters as (
    select EncounterKey, row_number() over (order by EncounterKey) as RowNum from dbo.DimEncounter
),
Top100KEncounters as (
    select EncounterKey from NumberedEncounters where RowNum <= $EncounterLimit
)
select
    P.EncounterKey as EncounterID,
    P2.Code as Code,
    CS.CodeSystemDesc as CodeSystem,
    P.ProcedureSequence as Sequence
from dbo.RelEncounterProcedure P
    left join dbo.DimProcedure P2 on P.ProcedureKey = P2.ProcedureKey
    left join dbo.DimCodeSystem CS on P2.CodeSystemKey = CS.CodeSystemKey
where P.EncounterKey in (select EncounterKey from Top100KEncounters)
"@ | Foreach-Object -Begin {
    "EncounterID|Code|CodeSystem|Sequence"
} -Process {
    "$($_["EncounterId"])|$($_["Code"])|$($_["CodeSystem"])|$($_["Sequence"])"
} | Out-File (Join-Path $PSScriptRoot "ProceduresPatientBilling00001.txt") -Encoding utf8
