IF OBJECT_ID('[dbo].[MAHSO_1]') IS NOT NULL
    DROP TABLE [dbo].[MAHSO_1];

CREATE TABLE [dbo].[MAHSO_1](
    [Column_1] nvarchar(255) NULL,
    [Column_2] nvarchar(255) NULL,
    [Highly Rural] nvarchar(255) NULL,
    [Insular Islands] nvarchar(255) NULL,
    [Rural] nvarchar(255) NULL,
    [Urban] nvarchar(255) NULL,
    [Unknown] nvarchar(255) NULL
);

IF OBJECT_ID('[dbo].[MAHSO_2]') IS NOT NULL
    DROP TABLE [dbo].[MAHSO_2];

CREATE TABLE [dbo].[MAHSO_2](
    [Column_1] nvarchar(255) NULL,
    [Column_2] nvarchar(255) NULL,
    [Column_3] nvarchar(255) NULL,
    [Mental_Health_FY15] nvarchar(255) NULL,
    [Mental_Health_FY16] nvarchar(255) NULL,
    [Mental_Health_FY17] nvarchar(255) NULL,
    [Primary_Care_FY15] nvarchar(255) NULL,
    [Primary_Care_FY16] nvarchar(255) NULL,
    [Primary_Care_FY17] nvarchar(255) NULL,
    [Specialty_Care_FY15] nvarchar(255) NULL,
    [Specialty_Care_FY16] nvarchar(255) NULL,
    [Specialty_Care_FY17] nvarchar(255) NULL
);

IF OBJECT_ID('[dbo].[MAHSO_3]') IS NOT NULL
    DROP TABLE [dbo].[MAHSO_3];

CREATE TABLE [dbo].[MAHSO_3](
    [Column_1] nvarchar(255) NULL,
    [Column_2] nvarchar(255) NULL,
    [Column_3] nvarchar(255) NULL,
    [FY15_Unique_Patients] nvarchar(255) NULL,
    [FY16_Unique_Patients] nvarchar(255) NULL,
    [FY17_Unique_Patients] nvarchar(255) NULL
);

IF OBJECT_ID('[dbo].[MAHSO_4]') IS NOT NULL
    DROP TABLE [dbo].[MAHSO_4];

CREATE TABLE [dbo].[MAHSO_4](
    [Column_1] nvarchar(255) NULL,
    [Column_2] nvarchar(255) NULL,
    [(130) EMERGENCY DEPT] nvarchar(255) NULL,
    [(131) URGENT CARE UNIT] nvarchar(255) NULL
);

IF OBJECT_ID('[dbo].[MAHSO_5]') IS NOT NULL
    DROP TABLE [dbo].[MAHSO_5];

CREATE TABLE [dbo].[MAHSO_5](
    [Column_1] nvarchar(255) NULL,
    [Column_2] nvarchar(255) NULL,
    [FY15_Encounters] nvarchar(255) NULL,
    [FY15_Unique_Patients] nvarchar(255) NULL,
    [FY16_Encounters] nvarchar(255) NULL,
    [FY16_Unique_Patients] nvarchar(255) NULL,
    [FY17_Encounters] nvarchar(255) NULL,
    [FY17_Unique_Patients] nvarchar(255) NULL
);

