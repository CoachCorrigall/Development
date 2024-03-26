create or replace view DEV_CDL_DB.RPT.FACT_ACCOUNTBALANCES(
	YEAR_MONTH_ID,
	ACCOUNT_ID,
	LEDGER_TYPE_CODE,
	LEDGER_TYPE_DESC,
	SUBLEDGER_G_L,
	SUBLEDGER_TYPE,
	COMPANY_CODE,
	BUSINESS_UNIT_CODE,
	BUSINESS_UNIT_TYPE,
	BU_STATUS,
	PROJECT_MANAGER_CODE,
	OBJECT_ACCOUNT_CODE,
	ACCOUNT_SUBSIDIARY,
	IS_INCLUDED_ACCOUNT_SUBSIDIARY,
	ACCOUNT_LEVEL_OF_DETAIL,
	CATEGORY,
	AMOUNT_NET_POSTING,
	AMOUNT_BEGINNING_BALANCE_PYE_FORWARD,
	AMOUNT_ORIGINAL_BEGINNING_BUDGET,
	IS_REVISED_AMOUNT,
	IS_REVISED_UNIT,
	IS_CHANGE_AMOUNT,
	IS_CHANGE_UNIT,
	IS_ORIG_AMT_FLAG,
	IS_ORIG_UNIT_FLAG,
	IS_BILL_TO_DATE_AMT_FLAG,
	IS_PREV_YR_COST_TO_DATE_AMT_FLAG,
	LATEST_CLOSED_PERIOD_YEAR_MONTH_ID,
	IS_LATEST_CLOSED_PERIOD_FLAG,
	IS_COST_TO_DATE_AMT_FLAG,
	IS_ACTUAL_QTY_FLAG,
	IS_ACTUAL_AMT_FLAG,
	PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE,
	PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_COST,
	PROFIT_RECOGNITION_PROJECTED_MARGIN,
	PROFIT_RECOGNITION_PROJECTED_MARGIN_PERCENT,
	PROFIT_RECOGNITION_EARNED_REVENUE,
	PROFIT_RECOGNITION_EARNED_COST,
	PROFIT_RECOGNITION_EARNED_MARGIN,
	PROFIT_RECOGNITION_EARNED_MARGIN_PERCENT,
	PROFIT_RECOGNITION_BACKLOG,
	PROFIT_RECOGNITION_ACTUAL_COST,
	PROFIT_RECOGNITION_PERCENT_COMPLETE,
	PROFIT_RECOGNITION_COST_OF_REVENUE,
	PROFIT_RECOGNITION_ACTUAL_REVENUE,
	PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_REVENUE,
	PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_COST,
	PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_MARGIN,
	PROFIT_RECOGNITION_ACCRUED_LOSS
) as

/*============================================================================================================
Created by: David Corrigall
Date: 10/24/2023
Purpose: Creates Account Balance fact table for reporting on financials at <Insert Corporaton>.

Notes: 
    10/24/2023 - This view is restricted to just a few months and is in a testing state.  More columns will need to be added.
    11/07/2023 - David Corrigall - Adding ledger type desc to the warehouse
    11/15/2023 - David Corrigall - Adding a field for Billed to Date Amount
    11/15/2023 - David Corrigall - Removing Date field
    12/04/2023 - David Corrigall - adding IS_PREV_YR_COST_TO_DATE_AMT_FLAG to data to suppport measure logic
    02/27/2024 - David Corrigall - adding a field for project manager code in order to support RLS

        
   
============================================================================================================*/

-- Select financials from Profit Recognition and add month, year, and Business unit for join to account balance
with FACT_PROFIT_RECOGNITION as (
    select 
    PR.DATE_EFFECTIVE                           AS JULIAN_DATE_EFFECTIVE,   --Primary Key (PK)
    PR.TYPE_OF_RECORD                           AS TYPE_OF_RECORD,          --Primary Key (PK)
    PR.BUSINESS_UNIT                            AS BUSINESS_UNIT_CODE,      --Primary Key (PK)
    tor.DESCRIPTION                             AS TYPE_OF_RECORD_DESC,
    D_DATE_EFFECTIVE.CALENDAR_DATE              AS DATE_EFFECTIVE,
    YEAR(D_DATE_EFFECTIVE.CALENDAR_DATE)        AS YearEFF,
    MONTH(D_DATE_EFFECTIVE.CALENDAR_DATE)       AS MonthEFF,
    -- financials
    PR.ACTUAL_REVENUE_TO_DATE/100               AS ACTUAL_REVENUE_TO_DATE,
    PR.ACTUAL_COST_TO_DATE/100                  AS ACTUAL_COST_TO_DATE,
    PR.PROJECTED_FINAL_REVENUE/100              AS PROJECTED_FINAL_REVENUE,
    PR.PROJECTED_FINAL_COST/100                 AS PROJECTED_FINAL_COST,
    PR.PROJECTED_FINAL_REVENUE_ADJUSTED/100     AS PROJECTED_FINAL_REVENUE_ADJUSTED,
    PR.PROJECTED_FINAL_COST_ADJUSTED/100        AS PROJECTED_FINAL_COST_ADJUSTED,
    PR.EARNED_JOB_TO_DATE_REVENUE/100           AS EARNED_JOB_TO_DATE_REVENUE,
    PR.EARNED_JOB_TO_DATE_COST/100              AS EARNED_JOB_TO_DATE_COST,
    PR.DATE_UPDATED                             AS DATE_UPDATED,
    PR.TIME_LAST_UPDATED                        AS TIME_LAST_UPDATED
    from SOURCE_JDEDWARDS.PROFIT_RECOGNITION PR
    Inner Join DEV.IL_CORE.DIM_DATE D_DATE_EFFECTIVE ON D_DATE_EFFECTIVE.julian_date=PR.DATE_EFFECTIVE
    -- type of record description
    Left Join DEV.SOURCE_JDEDWARDS.USER_DEFINED_CODE_VALUES tor ON PR.TYPE_OF_RECORD = tor.USER_DEFINED_CODE    
        AND tor.product_code= 'H50' 
        AND tor.user_defined_codes= 'TY'
    Where PR.TYPE_OF_RECORD <> 0 -- do not incluide records that have a type of record = 0 (Project level, No Profit Data)
),

-- CTE that will find the latest closed period of a company and support reporting for <Insert Company>
    -- <Insert Company> does not expect to see the values in periods after closing
COMPANY_CLOSED_PERIOD AS (
		SELECT 
			SRC.COMPANY                                 as COMPANY_CODE,    -- Primary Key (PK)
			SRC.NAME                                    as COMPANY_NAME,
			FISCAL_Y.calendar_date                      as FISCAL_YEAR_BEGIN_DATE,
			YEAR(FISCAL_Y.calendar_date)                as FISCAL_YEAR,
			SRC.PERIOD_NUMBER_CURRENT                   as PERIOD_NUMBER_CURRENT,
			CASE 
				WHEN SRC.PERIOD_NUMBER_CURRENT = 1 THEN 12
				ELSE SRC.PERIOD_NUMBER_CURRENT - 1 
			END                                         as LATEST_CLOSED_PERIOD,
			CONCAT(
				CASE 
					WHEN SRC.PERIOD_NUMBER_CURRENT = 1 THEN YEAR(FISCAL_Y.calendar_date) - 1 
					ELSE YEAR(FISCAL_Y.calendar_date)
				END,
				LPAD(CASE 
					WHEN SRC.PERIOD_NUMBER_CURRENT = 1 THEN 12 
					ELSE SRC.PERIOD_NUMBER_CURRENT - 1 
				END, 2, '0')
			)                                            as CLOSEDPERIOD_YEARMONTHID
		FROM DEV.SOURCE_JDEDWARDS.COMPANY_CONSTANTS SRC
		LEFT JOIN DEV.IL_CORE.DIM_DATE FISCAL_Y ON FISCAL_Y.julian_date = SRC.DATE_FISCAL_YEAR_BEGINS
),

-- This CTE is used to unpivot the ACCOUNT_BALANCES stage table and add base logic to reference later
 AccountBalancesUnpivotCTE AS (
    SELECT
         ab.ACCOUNT_ID                              AS ACCOUNT_ID
        ,CONCAT(ab.CENTURY, ab.FISCAL_YEAR)         AS FISCAL_YEAR
        ,CAST(TO_VARIANT(f.INDEX + 1) AS STRING)    AS FISCAL_MONTH
        ,ab.LEDGER_TYPES                            AS LEDGER_TYPE_CODE
        ,ltc.DESCRIPTION                            AS LEDGER_TYPE_DESC
        ,ab.SUBLEDGER_G_L                           AS SUBLEDGER_G_L
        ,ab.SUBLEDGER_TYPE                          AS SUBLEDGER_TYPE
        ,ab.COMPANY                                 AS COMPANY_CODE
        ,ab.BUSINESS_UNIT                           AS BUSINESS_UNIT_CODE
        ,bum.BUSINESS_UNIT_TYPE                     AS BUSINESS_UNIT_TYPE
        ,bum.CATEGORY_CODE_BUSINESS_UNIT_17         AS BU_STATUS
        ,bum.ADDRESS_NUMBER2                        AS PROJECT_MANAGER_CODE
        ,ab.OBJECT_ACCOUNT                          AS OBJECT_ACCOUNT_CODE
        ,ab.SUBSIDIARY                              AS ACCOUNT_SUBSIDIARY
        ,CASE
            WHEN ab.SUBSIDIARY NOT LIKE '99%' OR ab.SUBSIDIARY = '99999955' OR ab.SUBSIDIARY IS NULL THEN 1
            ELSE 0
            END                                     AS IS_INCLUDED_ACCOUNT_SUBSIDIARY
        ,am.ACCOUNT_LEVEL_OF_DETAIL                 AS ACCOUNT_LEVEL_OF_DETAIL
        ,CASE
            WHEN ab.SUBSIDIARY LIKE '8500%' OR ab.SUBSIDIARY = '99999955' OR ab.OBJECT_ACCOUNT LIKE '5%' THEN 'Revenue'
            WHEN ab.OBJECT_ACCOUNT LIKE '4%' THEN 'Other'
            ELSE 'Cost'
            END                                     AS CATEGORY
        ,f.VALUE::float                             AS AMOUNT_NET_POSTING
        ,AMOUNT_BEGINNING_BALANCE_PYE_FORWARD / 100 AS AMOUNT_BEGINNING_BALANCE_PYE_FORWARD
        ,AMOUNT_ORIGINAL_BEGINNING_BUDGET / 100     AS AMOUNT_ORIGINAL_BEGINNING_BUDGET  
        ,CASE
            WHEN bum.BUSINESS_UNIT_TYPE IN ('P1', 'P2') AND ab.LEDGER_TYPES = 'BA' THEN 1
            WHEN bum.BUSINESS_UNIT_TYPE NOT IN ('P1', 'P2') AND ab.LEDGER_TYPES = 'JA' THEN 1
            ELSE 0
            END AS IS_REVISED_AMOUNT
        ,CASE
            -- P1 and P2 units are always revised when ledger type is BU
            WHEN bum.BUSINESS_UNIT_TYPE IN ('P1', 'P2') AND ab.LEDGER_TYPES = 'BU' THEN 1
      
            -- Other units are only revised if ledger type is JU and account detail level is 7
            WHEN bum.BUSINESS_UNIT_TYPE NOT IN ('P1', 'P2') AND ab.LEDGER_TYPES = 'JU' THEN 1 -- AND am.ACCOUNT_LEVEL_OF_DETAIL = 7 THEN 1 /*REMOVING FOR P&L on 1/16/24*/
      
            -- All other cases are not revised
            ELSE 0
            END AS IS_REVISED_UNIT

        ,CASE
            WHEN ab.LEDGER_TYPES = 'QA' THEN 1
            ELSE 0
            END AS IS_CHANGE_AMOUNT
        ,CASE
            WHEN ab.LEDGER_TYPES = 'QU' AND am.ACCOUNT_LEVEL_OF_DETAIL = 7 THEN 1
            ELSE 0
            END AS IS_CHANGE_UNIT
        ,CASE
            -- Units P1 and P2 have original amount flag set for ledger type BA
            WHEN BUSINESS_UNIT_TYPE IN ('P1', 'P2') AND LEDGER_TYPES IN ('BA') THEN 1
  
            -- Other units have original amount flag set for ledger type JA
            WHEN BUSINESS_UNIT_TYPE NOT IN ('P1', 'P2') AND LEDGER_TYPES IN ('JA') THEN 1
  
            -- All other cases have the original amount flag unset
            ELSE 0
            END AS IS_ORIG_AMT_FLAG

        ,CASE
            -- Units P1 and P2 have original amount flag set for ledger type BA
            WHEN BUSINESS_UNIT_TYPE IN ('P1', 'P2') AND LEDGER_TYPES IN ('BU') THEN 1
  
            -- Other units have original amount flag set for ledger type JA
            WHEN BUSINESS_UNIT_TYPE NOT IN ('P1', 'P2') AND LEDGER_TYPES IN ('JU') THEN 1
  
            -- All other cases have the original amount flag unset
            ELSE 0
            END AS IS_ORIG_UNIT_FLAG

        ,CASE 
            WHEN LEDGER_TYPES IN ('AA') AND ab.OBJECT_ACCOUNT LIKE '5%' THEN 1
            ELSE 0
            END                         as IS_BILL_TO_DATE_AMT_FLAG

        ,COALESCE
        (
          case
            when ab.LEDGER_TYPES = 'AA' and ab.OBJECT_ACCOUNT between '6000' and '6099'                         then 1 -- WILL USE AMOUNT_BEGINNING_BALANCE_PYE_FORWARD in power BI Measure
            when ab.LEDGER_TYPES = 'AA' and ab.BUSINESS_UNIT in ('90090', '90091', '90092', '90093', '90094')   then 1 -- WILL USE AMOUNT_BEGINNING_BALANCE_PYE_FORWARD in power BI Measure
            else 0
          end,
          0
        )                               as IS_PREV_YR_COST_TO_DATE_AMT_FLAG
        ,com.CLOSEDPERIOD_YEARMONTHID   as LATEST_CLOSED_PERIOD
    
      ,CASE
        -- AA transactions within specific account range
        WHEN ab.LEDGER_TYPES = 'AA' AND ab.OBJECT_ACCOUNT BETWEEN '6000' AND '6099' THEN 1
        -- AA transactions in specific business units
        WHEN ab.LEDGER_TYPES = 'AA' AND ab.BUSINESS_UNIT IN ('90090', '90091', '90092', '90093', '90094') THEN 1
        -- All other transactions (0)
        ELSE 0
      END AS IS_COST_TO_DATE_AMT_FLAG

      ,CASE 
		WHEN LEDGER_TYPE_CODE = 'AU' THEN 1
        ELSE 0
		END AS IS_ACTUAL_QTY_FLAG

	   ,CASE 
		WHEN LEDGER_TYPE_CODE = 'AA' THEN 1
        ELSE 0
		END AS IS_ACTUAL_AMT_FLAG

    From DEV.SOURCE_JDEDWARDS.ACCOUNT_BALANCES ab
    LEFT JOIN DEV.SOURCE_JDEDWARDS.ACCOUNT_MASTER           am  ON ab.ACCOUNT_ID      = am.ACCOUNT_ID
    LEFT JOIN DEV.SOURCE_JDEDWARDS.BUSINESS_UNIT_MASTER     bum ON ab.BUSINESS_UNIT   = bum.BUSINESS_UNIT
    LEFT JOIN COMPANY_CLOSED_PERIOD                         com ON ab.COMPANY         = com.COMPANY_CODE
    LEFT JOIN DEV.SOURCE_JDEDWARDS.USER_DEFINED_CODE_VALUES ltc ON ab.LEDGER_TYPES    = ltc.USER_DEFINED_CODE and ltc.product_code= '09' and ltc.user_defined_codes= 'LT'
    
    -- This code is used to unpivot the net posting amounts from columns to rows
    ,LATERAL 
        FLATTEN(
            INPUT => 
            ARRAY_CONSTRUCT(
                AMOUNT_NET_POSTING_01 / 100,
                AMOUNT_NET_POSTING_02 / 100,
                AMOUNT_NET_POSTING_03 / 100,
                AMOUNT_NET_POSTING_04 / 100,
                AMOUNT_NET_POSTING_05 / 100,
                AMOUNT_NET_POSTING_06 / 100,
                AMOUNT_NET_POSTING_07 / 100,
                AMOUNT_NET_POSTING_08 / 100,
                AMOUNT_NET_POSTING_09 / 100,
                AMOUNT_NET_POSTING_10 / 100,
                AMOUNT_NET_POSTING_11 / 100,
                AMOUNT_NET_POSTING_12 / 100
            )
        ) AS f
),

UnpivotWithAdditionalColumnsCTE AS (
    SELECT
         ROUND(abu.FISCAL_YEAR * 100 + abu.FISCAL_MONTH, 0) AS YEAR_MONTH_ID
        ,abu.ACCOUNT_ID
        ,abu.LEDGER_TYPE_CODE
        ,abu.LEDGER_TYPE_DESC
        ,abu.SUBLEDGER_G_L
        ,abu.SUBLEDGER_TYPE
        ,abu.COMPANY_CODE
        ,abu.BUSINESS_UNIT_CODE
        ,abu.BUSINESS_UNIT_TYPE
        ,abu.BU_STATUS
        ,abu.PROJECT_MANAGER_CODE
        ,abu.OBJECT_ACCOUNT_CODE
        ,abu.ACCOUNT_SUBSIDIARY
        ,abu.IS_INCLUDED_ACCOUNT_SUBSIDIARY
        ,abu.ACCOUNT_LEVEL_OF_DETAIL
        ,abu.CATEGORY
        ,abu.AMOUNT_NET_POSTING
        ,abu.AMOUNT_BEGINNING_BALANCE_PYE_FORWARD
        ,abu.AMOUNT_ORIGINAL_BEGINNING_BUDGET
        ,abu.IS_REVISED_AMOUNT
        ,abu.IS_REVISED_UNIT
        ,abu.IS_CHANGE_AMOUNT
        ,abu.IS_CHANGE_UNIT
        ,abu.IS_ORIG_AMT_FLAG
        ,abu.IS_ORIG_UNIT_FLAG
        ,abu.IS_BILL_TO_DATE_AMT_FLAG
        ,abu.IS_PREV_YR_COST_TO_DATE_AMT_FLAG
        ,LATEST_CLOSED_PERIOD as LATEST_CLOSED_PERIOD_YEAR_MONTH_ID
        ,case when YEAR_MONTH_ID > LATEST_CLOSED_PERIOD_YEAR_MONTH_ID then 0 else 1 end as IS_LATEST_CLOSED_PERIOD_FLAG
        ,IS_COST_TO_DATE_AMT_FLAG
        ,IS_ACTUAL_QTY_FLAG
        ,IS_ACTUAL_AMT_FLAG


        -- PROFIT RECOGNITION FIELDS 
            -- -- 02.14.2024 -- --
            /*
            After some  data discovery, validation, and other considerations of the business, we have determined that many financial fields will be pulled directly from account balance as opposed to profit rec.
            Profit rec is necessary in our calculation for backlog as well as any other projected value.
            However, many of these fields will be calucalted through a formula from account balance.
            If needed, we can revisit adding these fields in the future.
            
            */
            
        
        -- Original + Revised Values
        ---- Revenue Categories
        --,ABS(PR.AMOUNT_ORIGINAL_REVENUE_BUDGET)                                                                    as PROFIT_RECOGNITION_ORIGINAL_REVENUE_BUDGET
        --,ABS(PR.REVENUE_CHANGES)                                                                                   as PROFIT_RECOGNITION_REVISED_REVENUE_CHANGES
        --,(PROFIT_RECOGNITION_ORIGINAL_REVENUE_BUDGET + PROFIT_RECOGNITION_REVISED_REVENUE_CHANGES)                 as PROFIT_RECOGNITION_REVISED_REVENUE_BUDGET
        ---- Cost Categories               
        --,PR.AMOUNT_ORIGINAL_COST_BUDGET                                                                            as PROFIT_RECOGNITION_ORIGINAL_COST_BUDGET
        --,PR.COST_CHANGES                                                                                           as PROFIT_RECOGNITION_REVISED_COST_CHANGES
        --,(PROFIT_RECOGNITION_ORIGINAL_COST_BUDGET + PROFIT_RECOGNITION_REVISED_COST_CHANGES)                       as PROFIT_RECOGNITION_REVISED_COST_BUDGET
        ---- Margin Categories             
        --,(PROFIT_RECOGNITION_ORIGINAL_REVENUE_BUDGET - PROFIT_RECOGNITION_ORIGINAL_COST_BUDGET)                    as PROFIT_RECOGNITION_ORIGINAL_MARGIN_BUDGET
        --,DIV0(PROFIT_RECOGNITION_ORIGINAL_MARGIN_BUDGET, PROFIT_RECOGNITION_ORIGINAL_REVENUE_BUDGET)               as PROFIT_RECOGNITION_ORIGINAL_MARGIN_BUDGET_PERCENT
        --,(PROFIT_RECOGNITION_REVISED_REVENUE_BUDGET - PROFIT_RECOGNITION_REVISED_COST_BUDGET)                      as PROFIT_RECOGNITION_REVISED_MARGIN_BUDGET
        --,DIV0(PROFIT_RECOGNITION_REVISED_MARGIN_BUDGET, PROFIT_RECOGNITION_REVISED_REVENUE_BUDGET)                 as PROFIT_RECOGNITION_REVISED_MARGIN_BUDGET_PERCENT       
        
        -- Projected Values
        ,abs(pr.PROJECTED_FINAL_REVENUE_ADJUSTED)                                                                   as PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE
        ,pr.PROJECTED_FINAL_COST_ADJUSTED                                                                           as PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_COST
        ,(PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE - PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_COST)   as PROFIT_RECOGNITION_PROJECTED_MARGIN
        ,DIV0(PROFIT_RECOGNITION_PROJECTED_MARGIN,PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE)              as PROFIT_RECOGNITION_PROJECTED_MARGIN_PERCENT
        
        -- earned values 
        ,abs(pr.EARNED_JOB_TO_DATE_REVENUE)                                                                         as PROFIT_RECOGNITION_EARNED_REVENUE
        ,pr.EARNED_JOB_TO_DATE_COST                                                                                 as PROFIT_RECOGNITION_EARNED_COST
        ,(PROFIT_RECOGNITION_EARNED_REVENUE- PROFIT_RECOGNITION_EARNED_COST)                                        as PROFIT_RECOGNITION_EARNED_MARGIN
        ,DIV0(PROFIT_RECOGNITION_EARNED_MARGIN,PROFIT_RECOGNITION_EARNED_REVENUE)                                   as PROFIT_RECOGNITION_EARNED_MARGIN_PERCENT
        
        -- Additional Metrics (use a combination of values)
        ,(PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE - pr.ACTUAL_REVENUE_TO_DATE)                          as PROFIT_RECOGNITION_BACKLOG
        ,pr.ACTUAL_COST_TO_DATE                                                                                     as PROFIT_RECOGNITION_ACTUAL_COST
        ,DIV0(pr.ACTUAL_COST_TO_DATE, pr.PROJECTED_FINAL_COST_ADJUSTED)                                             as PROFIT_RECOGNITION_PERCENT_COMPLETE
        ,(PROFIT_RECOGNITION_PERCENT_COMPLETE * PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE)                as PROFIT_RECOGNITION_COST_OF_REVENUE
        ,pr.ACTUAL_REVENUE_TO_DATE                                                                                  as PROFIT_RECOGNITION_ACTUAL_REVENUE -- AKA Billed To Date
        ,(PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_REVENUE - PROFIT_RECOGNITION_ACTUAL_REVENUE)                  as PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_REVENUE
        ,(PROFIT_RECOGNITION_ADJUSTED_PROJECTED_FINAL_COST - PROFIT_RECOGNITION_ACTUAL_COST)                        as PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_COST
        ,(PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_REVENUE - PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_COST)         as PROFIT_RECOGNITION_REMAINING_TO_COMPLETE_MARGIN
        
        -- Flag fields (need measures defined for these. Take absolute value of Projected Final Revenue - Projected Final Cost)
        ,CASE WHEN PROFIT_RECOGNITION_PROJECTED_MARGIN < 0 THEN 1 ELSE 0 END                                        as PROFIT_RECOGNITION_ACCRUED_LOSS        
    FROM AccountBalancesUnpivotCTE abu
     LEFT JOIN FACT_PROFIT_RECOGNITION pr ON 
             abu.BUSINESS_UNIT_CODE = pr.BUSINESS_UNIT_CODE
         AND abu.FISCAL_YEAR        = pr.YearEFF
         AND abu.FISCAL_MONTH       = pr.MonthEFF
)
SELECT a.*
FROM UnpivotWithAdditionalColumnsCTE a
where left(YEAR_MONTH_ID,4) <= year(getdate()) -- Return only records 2024 and prior 

;