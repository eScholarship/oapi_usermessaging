-- Creates message tables

CREATE TABLE [Message Files] (
	id int IDENTITY(1,1) NOT NULL,
	[File Name] varchar(120) COLLATE Latin1_General_CI_AS NULL,
	[Email Date] datetime NULL,
	[Contents Added] datetime DEFAULT getdate() NULL,
	CONSTRAINT [PK__Message __3213E83F3C4CBFFB] PRIMARY KEY (id)
);

CREATE TABLE [Messages] (
	Email nvarchar(320) COLLATE Latin1_General_CI_AS NULL,
	[Message File ID] int NULL,
	CONSTRAINT FK__Messages__Messag__4E1F62D7 FOREIGN KEY ([Message File ID]) REFERENCES [Message Files](id)
);
