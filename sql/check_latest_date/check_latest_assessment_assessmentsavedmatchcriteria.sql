SELECT TOP(1) assess.{date_col}
from [{entry_database}].[dbo].[ASSESSMENTSAVEDMATCHCRITERIA] asmc
join [{entry_database}].[dbo].[ASSESSMENT] assess on assess.AssessmentID = asmc.AssessmentID
order by assess.{date_col} Desc
