/****** Object:  StoredProcedure [etl].[create_f_ns_SalesOrderLine]    Script Date: 3/28/2024 1:06:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etl].[create_f_ns_SalesOrderLine]
AS
/******************************************************************************************************
Author: David Corrigall
Date: 05.24.2023
Purpose: Updates the data warehouse layer with new and changed source data.

Notes:


Change History:
2023.09.28 - Heena - Updated the join on NativeTransactionId and NativeTransactionLineId
2023.12.11 - David Corrigall - Adding 4 new columns
	IsClosedReportingFilter
	IsContractTeamInclusion
	LineLevelFees
	LineLevelGP
******************************************************************************************************/

MERGE [dwh].[f_ns_SalesOrderLine] AS t
USING [etl].[v_f_ns_SalesOrderLine] AS s
	ON	t.[NativeTransactionId]		= s.[NativeTransactionId]
	and  t.[NativeTransactionLineId] = s.[NativeTransactionLineId] -- t.[LineNumber] = s.[LineNumber]
WHEN MATCHED
	THEN
		UPDATE
		SET 
		t.[OpportunityId]					 = s.[OpportunityId]					
		,t.[DepartmentId]					 = s.[DepartmentId]					
		,t.[SubsidiaryId]					 = s.[SubsidiaryId]					
		,t.[SalesRepId]						 = s.[SalesRepId]						
		,t.[CustomerId]						 = s.[CustomerId]						
		,t.[ItemId]							 = s.[ItemId]							
		,t.[ProductFamilyId]				 = s.[ProductFamilyId]				
		,t.[TransactionDateId]				 = s.[TransactionDateId]				
		,t.[CreatedDateId]					 = s.[CreatedDateId]					
		,t.[CloseDateId]					 = s.[CloseDateId]					
		,t.[startdateId]					 = s.[startdateId]					
		,t.[TransactionType]				 = s.[TransactionType]				
		,t.[NativeTransactionId]			 = s.[NativeTransactionId]			
		,t.[NativeTransactionLineId]		 = s.[NativeTransactionLineId]		
		,t.[TransactionDate]				 = s.[TransactionDate]				
		,t.[SalesOrderNumber]				 = s.[SalesOrderNumber]				
		,t.[LineNumber]						 = s.[LineNumber]						
		,t.[MainLine]						 = s.[MainLine]						
		,t.[AccountingLineType]				 = s.[AccountingLineType]				
		,t.[TotalAmount]					 = s.[TotalAmount]					
		,t.[NetAmount]						 = s.[NetAmount]						
		,t.[GrossProfit]					 = s.[GrossProfit]					
		,t.[IFFFee]							 = s.[IFFFee]							
		,t.[OtherFee]						 = s.[OtherFee]						
		,t.[CreditCardFee]					 = s.[CreditCardFee]					
		,t.[RebateAmount]					 = s.[RebateAmount]					
		,t.[RegistrationAmount]				 = s.[RegistrationAmount]				
		,t.[GP]								 = s.[GP]								
		,t.[GrossProfitMarginPercent]		 = s.[GrossProfitMarginPercent]		
		,t.[LineEstGrossProfit]				 = s.[LineEstGrossProfit]				
		,t.[Quantity]						 = s.[Quantity]						
		,t.[posting]						 = s.[posting]						
		,t.[PostingPeriod]					 = s.[PostingPeriod]					
		,t.[BillingStatus]					 = s.[BillingStatus]					
		,t.[StartDate]						 = s.[StartDate]						
		,t.[DaysOpen]						 = s.[DaysOpen]						
		,t.[ProjectReference]				 = s.[ProjectReference]				
		,t.[SalesOrderStatus]				 = s.[SalesOrderStatus]				
		,t.[SalesOrderClass]				 = s.[SalesOrderClass]				
		,t.[ContractVehicles]				 = s.[ContractVehicles]				
		,t.[NativeOpportunityId]			 = s.[NativeOpportunityId]			
		,t.[NativeProductFamilyId]			 = s.[NativeProductFamilyId]			
		,t.[NativeEmergentContractVehicleId] = s.[NativeEmergentContractVehicleId]
		,t.[NativeMythicsContractVehicleId]	 = s.[NativeMythicsContractVehicleId]	
		,t.[NativeSubsidiaryId]				 = s.[NativeSubsidiaryId]				
		,t.[NativeCustomerId]				 = s.[NativeCustomerId]				
		,t.[NativeEmployeeId]				 = s.[NativeEmployeeId]				
		,t.[NativeLastModifiedDate]			 = s.[NativeLastModifiedDate]
		,t.[ServiceTypeItemsId]				= s.[ServiceTypeItemsId]
		,t.[BurdenCategoryId]				= s.[BurdenCategoryId]
		,t.[DeliveryApproachId]             = s.[DeliveryApproachId]
		,t.[JobId]							= s.[JobId]
		,t.[OptionYearsId]					= s.[OptionYearsId]
		,t.[PartnerId]						= s.[PartnerId]
		,t.[PoPStartDate]					= s.[PoPStartDate]
		,t.[PoPEndDate]						= s.[PoPEndDate]
		,t.[LicenseOrganicCredit]			= s.[LicenseOrganicCredit]
		,t.[RelatedOpportunities]			= s.[RelatedOpportunities]	
		,t.IsClosedReportingFilter			= s.IsClosedReportingFilter	
		,t.IsContractTeamInclusion			= s.IsContractTeamInclusion	
		,t.LineLevelFees					= s.LineLevelFees			
		,t.LineLevelGP						= s.LineLevelGP				
		,t.[ETLModifiedDate]				= GETDATE()
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
		[OpportunityId]
		,[DepartmentId]
		,[SubsidiaryId]
		,[SalesRepId]
		,[CustomerId]
		,[ItemId]
		,[ProductFamilyId]
		,[TransactionDateId]
		,[CreatedDateId]
		,[CloseDateId]
		,[startdateId]
		,[TransactionType]
		,[NativeTransactionId]
		,[NativeTransactionLineId]
		,[TransactionDate]
		,[SalesOrderNumber]
		,[LineNumber]
		,[MainLine]
		,[AccountingLineType]
		,[TotalAmount]
		,[NetAmount]
		,[GrossProfit]
		,[IFFFee]
		,[OtherFee]
		,[CreditCardFee]
		,[RebateAmount]
		,[RegistrationAmount]
		,[GP]
		,[GrossProfitMarginPercent]
		,[LineEstGrossProfit]
		,[Quantity]
		,[posting]
		,[PostingPeriod]
		,[BillingStatus]
		,[StartDate]
		,[DaysOpen]
		,[ProjectReference]
		,[SalesOrderStatus]
		,[SalesOrderClass]
		,[ContractVehicles]
		,[NativeOpportunityId]
		,[NativeProductFamilyId]
		,[NativeEmergentContractVehicleId]
		,[NativeMythicsContractVehicleId]
		,[NativeSubsidiaryId]
		,[NativeCustomerId]
		,[NativeEmployeeId]
		,[NativeLastModifiedDate]	
		,[ServiceTypeItemsId]
		,[BurdenCategoryId]
		,[DeliveryApproachId]
		,[JobId]
		,[OptionYearsId]
		,[PartnerId]
		,[PoPStartDate]
		,[PoPEndDate]
		,[LicenseOrganicCredit]
		,[RelatedOpportunities]
		,[IsClosedReportingFilter]		
		,[IsContractTeamInclusion]		
		,[LineLevelFees]		
		,[LineLevelGP]				
			)
		VALUES (
		s.[OpportunityId]
		,s.[DepartmentId]
		,s.[SubsidiaryId]
		,s.[SalesRepId]
		,s.[CustomerId]
		,s.[ItemId]
		,s.[ProductFamilyId]
		,s.[TransactionDateId]
		,s.[CreatedDateId]
		,s.[CloseDateId]
		,s.[startdateId]
		,s.[TransactionType]
		,s.[NativeTransactionId]
		,s.[NativeTransactionLineId]
		,s.[TransactionDate]
		,s.[SalesOrderNumber]
		,s.[LineNumber]
		,s.[MainLine]
		,s.[AccountingLineType]
		,s.[TotalAmount]
		,s.[NetAmount]
		,s.[GrossProfit]
		,s.[IFFFee]
		,s.[OtherFee]
		,s.[CreditCardFee]
		,s.[RebateAmount]
		,s.[RegistrationAmount]
		,s.[GP]
		,s.[GrossProfitMarginPercent]
		,s.[LineEstGrossProfit]
		,s.[Quantity]
		,s.[posting]
		,s.[PostingPeriod]
		,s.[BillingStatus]
		,s.[StartDate]
		,s.[DaysOpen]
		,s.[ProjectReference]
		,s.[SalesOrderStatus]
		,s.[SalesOrderClass]
		,s.[ContractVehicles]
		,s.[NativeOpportunityId]
		,s.[NativeProductFamilyId]
		,s.[NativeEmergentContractVehicleId]
		,s.[NativeMythicsContractVehicleId]
		,s.[NativeSubsidiaryId]
		,s.[NativeCustomerId]
		,s.[NativeEmployeeId]
		,s.[NativeLastModifiedDate]
		,s.[ServiceTypeItemsId]
		,s.[BurdenCategoryId]
		,s.[DeliveryApproachId]
		,s.[JobId]
		,s.[OptionYearsId]
		,s.[PartnerId]
		,s.[PoPStartDate]
		,s.[PoPEndDate]
		,s.[LicenseOrganicCredit]
		,s.[RelatedOpportunities]
		,s.[IsClosedReportingFilter]	
		,s.[IsContractTeamInclusion]	
		,s.[LineLevelFees]		
		,s.[LineLevelGP]				
			);
GO

