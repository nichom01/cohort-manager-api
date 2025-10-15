The parameter for this feature will be :

GPPractice data set (defined below)

public class GpPractice
{
    [Key]
    [Column("GP_PRACTICE_CODE")]
    public string GpPracticeCode { get; set; }
    [Column("BSO")]
    public string BsoCode { get; set; }
    [Column("COUNTRY_CATEGORY")]
    public string CountryCategory { get; set; }
    [Column("AUDIT_ID")]
    public decimal AuditId { get; set; }
    [Column("AUDIT_CREATED_TIMESTAMP", TypeName = "datetime")]
    public DateTime AuditCreatedTimeStamp { get; set; }
    [Column("AUDIT_LAST_MODIFIED_TIMESTAMP", TypeName = "datetime")]
    public DateTime AuditLastUpdatedTimeStamp { get; set; }
    [Column("AUDIT_TEXT")]
    public string AuditText { get; set; }

}

The feature ahould also have the latest demographic and participantManagement record for this user. It may be more efficient to have these 3 datasets passed in as parameters.

and run a number of up to 50 validation rules, returning the results as a list. The rules will be independant and should run in parallel.

one basic example rule would be that the that the demographic.primaryCareProvider exists in GpPractice.GP_PRACTICE_CODE dataset

This process should be idempotent.