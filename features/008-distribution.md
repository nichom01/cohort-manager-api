This feature collects the data to be distributed to the downstream system, the feature requires 3 endpoints

1. api endpoint to persist the distribution data for a single record or a set of records
2. to retrieve data that has not previously been taken, this api on success will record on the record who collected and when.
3. replay a previous collection

distribution data structure is as follows:

modelBuilder.Entity("Model.CohortDistribution", b =>
                {
                    b.Property<int>("CohortDistributionId")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("int")
                        .HasColumnName("BS_COHORT_DISTRIBUTION_ID");

                    SqlServerPropertyBuilderExtensions.UseIdentityColumn(b.Property<int>("CohortDistributionId"));

                    b.Property<string>("AddressLine1")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("ADDRESS_LINE_1");

                    b.Property<string>("AddressLine2")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("ADDRESS_LINE_2");

                    b.Property<string>("AddressLine3")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("ADDRESS_LINE_3");

                    b.Property<string>("AddressLine4")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("ADDRESS_LINE_4");

                    b.Property<string>("AddressLine5")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("ADDRESS_LINE_5");

                    b.Property<string>("CurrentPosting")
                        .HasMaxLength(10)
                        .HasColumnType("nvarchar(10)")
                        .HasColumnName("CURRENT_POSTING");

                    b.Property<DateTime?>("CurrentPostingFromDt")
                        .HasColumnType("datetime")
                        .HasColumnName("CURRENT_POSTING_FROM_DT");

                    b.Property<DateTime?>("DateOfBirth")
                        .HasColumnType("datetime")
                        .HasColumnName("DATE_OF_BIRTH");

                    b.Property<DateTime?>("DateOfDeath")
                        .HasColumnType("datetime")
                        .HasColumnName("DATE_OF_DEATH");

                    b.Property<string>("EmailAddressHome")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("EMAIL_ADDRESS_HOME");

                    b.Property<DateTime?>("EmailAddressHomeFromDt")
                        .HasColumnType("datetime")
                        .HasColumnName("EMAIL_ADDRESS_HOME_FROM_DT");

                    b.Property<string>("FamilyName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("FAMILY_NAME");

                    b.Property<short>("Gender")
                        .HasColumnType("smallint")
                        .HasColumnName("GENDER");

                    b.Property<string>("GivenName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("GIVEN_NAME");

                    b.Property<short>("InterpreterRequired")
                        .HasColumnType("smallint")
                        .HasColumnName("INTERPRETER_REQUIRED");

                    b.Property<short>("IsExtracted")
                        .HasColumnType("smallint")
                        .HasColumnName("IS_EXTRACTED");

                    b.Property<long>("NHSNumber")
                        .HasColumnType("bigint")
                        .HasColumnName("NHS_NUMBER");

                    b.Property<string>("NamePrefix")
                        .HasMaxLength(35)
                        .HasColumnType("nvarchar(35)")
                        .HasColumnName("NAME_PREFIX");

                    b.Property<string>("OtherGivenName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("OTHER_GIVEN_NAME");

                    b.Property<long>("ParticipantId")
                        .HasColumnType("bigint")
                        .HasColumnName("PARTICIPANT_ID");

                    b.Property<string>("PostCode")
                        .HasMaxLength(10)
                        .HasColumnType("nvarchar(10)")
                        .HasColumnName("POST_CODE");

                    b.Property<string>("PreferredLanguage")
                        .HasMaxLength(35)
                        .HasColumnType("nvarchar(35)")
                        .HasColumnName("PREFERRED_LANGUAGE");

                    b.Property<string>("PreviousFamilyName")
                        .HasMaxLength(100)
                        .HasColumnType("nvarchar(100)")
                        .HasColumnName("PREVIOUS_FAMILY_NAME");

                    b.Property<string>("PrimaryCareProvider")
                        .HasMaxLength(10)
                        .HasColumnType("nvarchar(10)")
                        .HasColumnName("PRIMARY_CARE_PROVIDER");

                    b.Property<DateTime?>("PrimaryCareProviderDate")
                        .HasColumnType("datetime")
                        .HasColumnName("PRIMARY_CARE_PROVIDER_FROM_DT");

                    b.Property<string>("ReasonForRemoval")
                        .HasMaxLength(10)
                        .HasColumnType("nvarchar(10)")
                        .HasColumnName("REASON_FOR_REMOVAL");

                    b.Property<DateTime?>("ReasonForRemovalDate")
                        .HasColumnType("datetime")
                        .HasColumnName("REASON_FOR_REMOVAL_FROM_DT");

                    b.Property<DateTime?>("RecordInsertDateTime")
                        .HasColumnType("datetime")
                        .HasColumnName("RECORD_INSERT_DATETIME");

                    b.Property<DateTime?>("RecordUpdateDateTime")
                        .HasColumnType("datetime")
                        .HasColumnName("RECORD_UPDATE_DATETIME");

                    b.Property<Guid>("RequestId")
                        .HasColumnType("uniqueidentifier")
                        .HasColumnName("REQUEST_ID");

                    b.Property<long?>("SupersededNHSNumber")
                        .HasColumnType("bigint")
                        .HasColumnName("SUPERSEDED_NHS_NUMBER");

                    b.Property<string>("TelephoneNumberHome")
                        .HasMaxLength(35)
                        .HasColumnType("nvarchar(35)")
                        .HasColumnName("TELEPHONE_NUMBER_HOME");

                    b.Property<DateTime?>("TelephoneNumberHomeFromDt")
                        .HasColumnType("datetime")
                        .HasColumnName("TELEPHONE_NUMBER_HOME_FROM_DT");

                    b.Property<string>("TelephoneNumberMob")
                        .HasMaxLength(35)
                        .HasColumnType("nvarchar(35)")
                        .HasColumnName("TELEPHONE_NUMBER_MOB");

                    b.Property<DateTime?>("TelephoneNumberMobFromDt")
                        .HasColumnType("datetime")
                        .HasColumnName("TELEPHONE_NUMBER_MOB_FROM_DT");

                    b.Property<DateTime?>("UsualAddressFromDt")
                        .HasColumnType("datetime")
                        .HasColumnName("USUAL_ADDRESS_FROM_DT");

                    b.HasKey("CohortDistributionId");

                    b.HasIndex(new[] { "IsExtracted", "RequestId" }, "IX_BSCOHORT_IS_EXTACTED_REQUESTID");

                    b.HasIndex(new[] { "NHSNumber" }, "IX_BS_COHORT_DISTRIBUTION_NHSNUMBER");

                    b.ToTable("BS_COHORT_DISTRIBUTION", "dbo");
                });