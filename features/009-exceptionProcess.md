The exception data structure is as follows:

modelBuilder.Entity("Model.ExceptionManagement", b =>
                {
                    b.Property<int>("ExceptionId")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int")
                        .HasColumnName("EXCEPTION_ID");

                    SqlServerPropertyBuilderExtensions.UseIdentityColumn(b.Property<int>("ExceptionId"));

                    b.Property<int?>("Category")
                        .HasColumnType("int")
                        .HasColumnName("CATEGORY");

                    b.Property<string>("CohortName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("COHORT_NAME");

                    b.Property<DateTime?>("DateCreated")
                        .HasColumnType("datetime")
                        .HasColumnName("DATE_CREATED");

                    b.Property<DateTime?>("DateResolved")
                        .HasColumnType("date")
                        .HasColumnName("DATE_RESOLVED");

                    b.Property<string>("ErrorRecord")
                        .HasColumnType("nvarchar(max)")
                        .HasColumnName("ERROR_RECORD");

                    b.Property<DateTime?>("ExceptionDate")
                        .HasColumnType("datetime")
                        .HasColumnName("EXCEPTION_DATE");

                    b.Property<string>("FileName")
                        .HasMaxLength(250)
                        .HasColumnType("nvarchar(250)")
                        .HasColumnName("FILE_NAME");

                    b.Property<short?>("IsFatal")
                        .HasColumnType("smallint")
                        .HasColumnName("IS_FATAL");

                    b.Property<string>("NhsNumber")
                        .HasMaxLength(50)
                        .HasColumnType("nvarchar(50)")
                        .HasColumnName("NHS_NUMBER");

                    b.Property<DateTime?>("RecordUpdatedDate")
                        .HasColumnType("datetime")
                        .HasColumnName("RECORD_UPDATED_DATE");

                    b.Property<string>("RuleDescription")
                        .HasColumnType("nvarchar(max)")
                        .HasColumnName("RULE_DESCRIPTION");

                    b.Property<int?>("RuleId")
                        .HasColumnType("int")
                        .HasColumnName("RULE_ID");

                    b.Property<string>("ScreeningName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("SCREENING_NAME");

                    b.Property<DateTime?>("ServiceNowCreatedDate")
                        .HasColumnType("date")
                        .HasColumnName("SERVICENOW_CREATED_DATE");

                    b.Property<string>("ServiceNowId")
                        .HasMaxLength(50)
                        .HasColumnType("nvarchar(50)")
                        .HasColumnName("SERVICENOW_ID");

                    b.HasKey("ExceptionId");

                    b.HasIndex(new[] { "NhsNumber", "ScreeningName" }, "IX_EXCEPTIONMGMT_NHSNUM_SCREENINGNAME");

                    b.ToTable("EXCEPTION_MANAGEMENT", "dbo");
                });

    
This feature should have an api which 
1. persists one or many exception records
2. resolves all exceptions for an nhs number