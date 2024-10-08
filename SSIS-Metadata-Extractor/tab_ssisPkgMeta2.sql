USE [PFADW_META]
GO

/****** Object:  Table [dbo].[ssisPkgMeta2]    Script Date: 13-04-2022 19:34:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

drop  TABLE [dbo].[ssisPkgMeta2]
go

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ssisPkgMeta2]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ssisPkgMeta2](
	[Package] [nvarchar](200) NULL,
	[objecttype] [nvarchar](200) NULL,
	[refid] [nvarchar](2000) NULL,
	[parentref] [nvarchar](2000) NULL,
	[dtsid] [nvarchar](200) NULL,
	[objectclass] [nvarchar](200) NULL,
	[objectname] [nvarchar](200) NULL,
	[contenttype] [nvarchar](200) NULL,
	[content] [nvarchar](max) NULL,
	[connmanagerid] [nvarchar](200) NULL,
	[id] bigint identity(1,1)
) 
END
GO


