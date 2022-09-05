SELECT asmc.[AssessmentID]
      ,[ProductID]
      ,[ProblemID]
      ,[QuestionID]
      ,[AnswerCriteria]
      ,[Operator]
      ,[Error]
      ,[ASSESSMENTSAVEDMATCHCRITERIAID]
      ,assess.[ModifiedDate]
      ,assess.[ModifiedByUserID]
      ,assess.[CreateDate]
      ,assess.[EndDate]
FROM [{database}].[dbo].[ASSESSMENTSAVEDMATCHCRITERIA] asmc
join [{database}].[dbo].[ASSESSMENT] assess on assess.AssessmentID = asmc.AssessmentID
where assess.CreateDate > '{last_date}'