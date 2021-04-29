-- Common Table Expression to store relevant user data

SET TRANSACTION ISOLATION LEVEL SNAPSHOT
BEGIN TRANSACTION

;WITH u AS (
	SELECT	[User].[ID],
			[User].[Last Name],
			[User].[First Name],
			[User].[Email],
			[User].[Position],
			[User].[Primary Group Descriptor],
			-- which OA Policy applies to the user?
			[Group].[OA Policy ID],
			-- get the OA Policy name
			(	SELECT	[Name]
				FROM	[OA Policy] oap
				WHERE	oap.[ID] = [Group].[OA Policy ID]
			) AS [OA Policy Name]
	FROM	[User]
			JOIN [Group] ON [Group].[Primary Group Descriptor] = [User].[Primary Group Descriptor]
						 OR ([User].[Primary Group Descriptor] IS NULL AND [Group].[ID] = 1) -- Users without a primary group descriptor are assigned to the top-level group
	WHERE	-- limit to current, active, academic users
			[User].[Is Current Staff] = 1
			AND [User].[Is Login Allowed] = 1
			AND [User].[Is Academic] = 1
			-- exclude the anonymous and system users
			AND [User].[ID] NOT IN (0,1)
),
puroa AS (
	SELECT	pur.[Publication ID],
			pur.[User ID],
			poap.[OA Policy ID],
			poap.[Compliance Status]
	FROM	[Publication OA Policy] poap
			JOIN [Publication User Relationship] pur
				ON pur.[Publication ID] = poap.[Publication ID]
	WHERE	pur.[Type] = 'Authored by'	-- limit to authorship links (optional)
)
SELECT		u.[ID] as "ID",
			u.[Last Name] as "Last Name",
			u.[First Name] as "First Name",
			u.[Email] as "Email",
			u.[Primary Group Descriptor] as "Primary Group",
			u.[Position] as "Position",
			u.[OA Policy ID],
			u.[OA Policy Name],

			(SELECT COUNT([Time]) FROM [Login Log] WHERE [User ID] = u.[ID]) as "Number of Logins",

			(SELECT MAX([Time]) FROM [Login Log] WHERE [User ID] = u.[ID]) as "Last Login",

			(   SELECT COUNT(DISTINCT pp.[Publication ID]) FROM [Pending Publication] pp
				WHERE pp.[User ID] = u.[ID]
			) as "Pending Publications",

			(   SELECT COUNT(DISTINCT pp.[Publication ID]) FROM [Pending Publication] pp
				JOIN [Publication] p ON pp.[Publication ID] = p.[ID]
				WHERE pp.[User ID] = u.[ID]
				-- last two weeks, don't forget to change if LBL goes to weekly notifications
				AND pp.[Modified When] >= DATEADD(day,-14, GETDATE())
			) as "New Pending Publications",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
				LEFT JOIN [Publication Record] pr ON pr.[Publication ID] = puroa.[Publication ID] AND pr.[Data Source] = 'eScholarship'
				JOIN [Publication] p ON p.[ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID		
				AND pr.[Publication ID] IS NULL
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
			) AS "OA Publications Needing Upload",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
				LEFT JOIN [Publication Record] pr ON pr.[Publication ID] = puroa.[Publication ID] AND pr.[Data Source] = 'eScholarship'
				JOIN [Publication] p ON p.[ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID		
				AND pr.[Publication ID] IS NULL
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND p.[Flagged As Not Externally Funded] != 'true'
				AND p.[Last Flagged As Grant Not Listed] IS NULL
			) AS "LBL OA Publications Needing Upload",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
				JOIN [Publication Record] pr ON	pr.[Publication ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND pr.[repository-status] = 'Public'
				AND pr.[Data Source] = 'eScholarship'
			) AS "Completed OA Publications",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
				JOIN [Grant Publication Relationship] gur ON puroa.[Publication ID] = gur.[Publication ID]
				JOIN [Grant] g ON gur.[Grant ID] = g.[ID]
				WHERE	puroa.[User ID] = u.ID
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND g.[funder-name] LIKE '%DOE%'
			) AS "LBL OA Pubs with LBL Grant Links",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
				LEFT JOIN [Grant Publication Relationship] gur ON gur.[Publication ID] = puroa.[Publication ID]
				JOIN [Publication] p ON p.[ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND gur.[Publication ID] IS NULL
				AND p.[Flagged As Not Externally Funded] != 'true'
				AND p.[Last Flagged As Grant Not Listed] IS NULL
			) AS "LBL Claimed OA Pubs without Grant Links",

			(	SELECT TOP 2 COALESCE(REPLACE(REPLACE(p.[Title], CHAR(13), ''), CHAR(10), ''), '') + '||' AS 'data()' 
				FROM	puroa
				LEFT JOIN [Publication Record] pr ON pr.[Publication ID] = puroa.[Publication ID] AND pr.[Data Source] = 'eScholarship'
				JOIN [Publication] p ON p.[ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID
				AND pr.[Publication ID] IS NULL
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND p.[Flagged As Not Externally Funded] != 'true'
				AND p.[Last Flagged As Grant Not Listed] IS NULL
				FOR XML PATH('')
			) AS "OA Titles Needing Upload",

			(	SELECT TOP 2 COALESCE(REPLACE(REPLACE(p.[Title], CHAR(13), ''), CHAR(10), ''), '') + '||' AS 'data()' 
				FROM	puroa
				LEFT JOIN [Grant Publication Relationship] gur ON gur.[Publication ID] = puroa.[Publication ID]
				JOIN [Publication] p ON p.[ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND gur.[Publication ID] IS NULL
				AND p.[Flagged As Not Externally Funded] != 'true'
				AND p.[Last Flagged As Grant Not Listed] IS NULL
				FOR XML PATH('')
			) AS "OA Titles Without Grants",

			(   SELECT TOP 2 COALESCE(REPLACE(REPLACE(p.[Title], CHAR(13), ''), CHAR(10), ''), '') + '||' AS 'data()'
				FROM [Pending Publication] pp
				JOIN [Publication] p ON p.[ID] = pp.[Publication ID]
				WHERE pp.[User ID] = u.[ID]
				FOR XML PATH('')
			) as "Titles Pending",

			(	SELECT	COUNT(DISTINCT puroa.[Publication ID])
				FROM	puroa
                                JOIN [Publication OA Policy Exception] pope ON pope.[Publication ID] = puroa.[Publication ID]
				WHERE	puroa.[User ID] = u.ID
				AND puroa.[OA Policy ID] = u.[OA Policy ID]		-- in policy
				AND pope.[Type] IS NOT NULL                             -- not sure if I should be checking for NULL, empty string, or both?
			) AS "OA Policy Exception Publications"

FROM		u
WHERE		u.[Primary Group Descriptor] IN ('lbl-user','ucb-lbl-senate','ucd-lbl-senate','uci-lbl-senate',
											'ucla-lbl-senate','ucm-lbl-senate','ucr-lbl-senate','ucsb-lbl-senate',
											'ucsc-lbl-senate','ucsd-lbl-senate','ucsf-lbl-senate')
ORDER BY	u.[ID]
;

COMMIT TRANSACTION
