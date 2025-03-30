create database if not exists customer_360;
use database customer_360;
use schema public;


CREATE OR REPLACE WAREHOUSE investment_cortex_wh WITH WAREHOUSE_SIZE='MEDIUM'
AUTO_RESUME = TRUE
AUTO_SUSPEND = 360;

use warehouse investment_cortex_wh;

-- Step 1: Create Tables
CREATE OR REPLACE TABLE CUSTOMER_PROFILE (
    CUSTOMER_ID STRING,
    NAME STRING,
    AGE INT,
    INCOME FLOAT,
    NET_WORTH FLOAT,
    ACCOUNT_TYPE STRING,
    TENURE_YEARS FLOAT
);

CREATE OR REPLACE TABLE TRANSACTION_HISTORY (
    TRANSACTION_ID STRING,
    CUSTOMER_ID STRING,
    TRANSACTION_DATE DATE,
    TRANSACTION_AMOUNT FLOAT,
    TRANSACTION_TYPE STRING,
    MERCHANT_CATEGORY STRING
);

CREATE OR REPLACE TABLE PRODUCT_USAGE (
    CUSTOMER_ID STRING,
    PRODUCT_TYPE STRING,
    USAGE_FREQUENCY FLOAT,
    LAST_USED_DATE DATE
);



-- Step 2: Insert Sample Data
--       CUSTOMER_ID ,NAME ,AGE ,INCOME ,NET_WORTH ,ACCOUNT_TYPE ,TENURE_YEARS 

INSERT INTO CUSTOMER_PROFILE VALUES
('CUST001', 'Alice Johnson', 34, 75000, 150000, 'Savings', 5.2),
('CUST002', 'Bob Smith', 45, 120000, 450000, 'Checking', 8.5),
('CUST003', 'Carol Lee', 28, 54000, 80000, 'Savings', 3.1),
('CUST004', 'David Kim', 60, 98000, 350000, 'Investment', 12.3),
('CUST005', 'Sarah Williams', 42, 95000, 280000, 'Premium', 7.5);


--     TRANSACTION_ID ,CUSTOMER_ID ,TRANSACTION_DATE ,TRANSACTION_AMOUNT ,TRANSACTION_TYPE ,MERCHANT_CATEGORY 
INSERT INTO TRANSACTION_HISTORY VALUES
('TX001', 'CUST001', '2025-01-05', 150.75, 'Purchase', 'Grocery'),
('TX002', 'CUST001', '2025-02-10', 600.00, 'Payment', 'Credit Card'),
('TX003', 'CUST002', '2025-01-20', 2000.00, 'Deposit', 'Salary'),
('TX004', 'CUST003', '2025-03-15', 75.00, 'Withdrawal', 'ATM'),
('TX005', 'CUST005', '2025-03-01', 1200.00, 'Purchase', 'Retail'),
('TX006', 'CUST005', '2025-03-10', 2500.00, 'Deposit', 'Salary'),
('TX007', 'CUST005', '2025-03-15', 500.00, 'Payment', 'Utility'),
('TX008', 'CUST005', '2025-03-20', 1500.00, 'Purchase', 'Shopping');


--    CUSTOMER_ID, PRODUCT_TYPE ,USAGE_FREQUENCY ,LAST_USED_DATE    
INSERT INTO PRODUCT_USAGE VALUES
('CUST001', 'Credit Card', 15.2, '2025-02-10'),
('CUST002', 'Mortgage', 1.1, '2025-01-25'),
('CUST003', 'Mobile Banking', 8.5, '2025-03-10'),
('CUST004', 'Investment Account', 2.0, '2025-01-30'),
('CUST005', 'Mobile Banking', 25.0, CURRENT_DATE()-1),
('CUST005', 'Credit Card', 20.5, CURRENT_DATE()),
('CUST005', 'Investment Account', 8.0, CURRENT_DATE()-1);


-- Step 3: Prepare Views for Analysis
CREATE OR REPLACE VIEW CUSTOMER_360 AS
SELECT 
    CP.CUSTOMER_ID, 
    CP.NAME, 
    CP.AGE, 
    CP.INCOME, 
    CP.NET_WORTH, 
    CP.ACCOUNT_TYPE, 
    CP.TENURE_YEARS,
    TH.TRANSACTION_AMOUNT, 
    TH.TRANSACTION_TYPE, 
    PU.PRODUCT_TYPE, 
    PU.USAGE_FREQUENCY
FROM CUSTOMER_PROFILE CP
LEFT JOIN TRANSACTION_HISTORY TH ON CP.CUSTOMER_ID = TH.CUSTOMER_ID
LEFT JOIN PRODUCT_USAGE PU ON CP.CUSTOMER_ID = PU.CUSTOMER_ID;

select * from CUSTOMER_360;


-- **************************************************************************************************************
-- Demo for Churn Risk using LLM
-- Query to analyze churn risk at the customer level by aggregating all product information first:
--      Created a CTE to aggregate all product and transaction data at the customer level
--
--      Added TOTAL_PRODUCTS count to show product relationship depth
-- 
--      Calculated average usage frequency across all products
-- 
--      Used MAX_DAYS_SINCE_LAST_USAGE to identify the longest period of inactivity
--
--      Modified the prompt to specifically request a holistic assessment based on the complete customer relationship

WITH CUSTOMER_PRODUCT_SUMMARY AS (
    SELECT 
        CP.CUSTOMER_ID,
        CP.NAME,
        CP.AGE,
        CP.INCOME,
        CP.ACCOUNT_TYPE,
        CP.TENURE_YEARS,
        COUNT(DISTINCT PU.PRODUCT_TYPE) as TOTAL_PRODUCTS,
        AVG(PU.USAGE_FREQUENCY) as AVG_USAGE_FREQUENCY,
        MAX(DATEDIFF('day', PU.LAST_USED_DATE, CURRENT_DATE())) as MAX_DAYS_SINCE_LAST_USAGE,
        COUNT(TH.TRANSACTION_ID) as TOTAL_TRANSACTIONS,
        AVG(TH.TRANSACTION_AMOUNT) as AVG_TRANSACTION_AMOUNT
    FROM CUSTOMER_PROFILE CP
    LEFT JOIN PRODUCT_USAGE PU ON CP.CUSTOMER_ID = PU.CUSTOMER_ID
    LEFT JOIN TRANSACTION_HISTORY TH ON CP.CUSTOMER_ID = TH.CUSTOMER_ID
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    CUSTOMER_ID,
    NAME,
    SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
        'Analyze the overall churn risk (High, Medium, or Low) for this customer based on their complete relationship\\\: ' ||
        'Customer Profile\\\: ' ||
        'Age\\\: ' || AGE || ', ' ||
        'Income\\\: &dollar;' || INCOME || ', ' ||
        'Account Type\\\: ' || ACCOUNT_TYPE || ', ' ||
        'Tenure\\\: ' || TENURE_YEARS || ' years, ' ||
        'Total Products\\\: ' || TOTAL_PRODUCTS || ', ' ||
        'Total Transactions\\\: ' || TOTAL_TRANSACTIONS || ', ' ||
        'Average Transaction\\\: &dollar;' || AVG_TRANSACTION_AMOUNT || ', ' ||
        'Average Product Usage Frequency\\\: ' || AVG_USAGE_FREQUENCY || ' times per month, ' ||
        'Maximum Days Since Last Product Usage\\\: ' || MAX_DAYS_SINCE_LAST_USAGE || 
        '. Consider all products and provide a holistic risk assessment.'
    ) as OVERALL_CHURN_RISK_ANALYSIS
FROM CUSTOMER_PRODUCT_SUMMARY
ORDER BY CUSTOMER_ID;


-- **********************************************************************
-- Demo for Sentiment and Generate Customer Feedback
-- Create a table for customer feedback/interactions

CREATE OR REPLACE TABLE CUSTOMER_INTERACTIONS (
    INTERACTION_ID STRING,
    CUSTOMER_ID STRING,
    INTERACTION_DATE DATE,
    FEEDBACK_TEXT STRING
);

-- Insert sample data
INSERT INTO CUSTOMER_INTERACTIONS VALUES
('INT001', 'CUST001', '2025-02-01', 'Having trouble with mobile banking login'),
('INT002', 'CUST002', '2025-02-02', 'Love the new credit card rewards program'),
('INT003', 'CUST003', '2025-02-03', 'Wait times for customer service are too long');

-- Use Cortex Complete for sentiment analysis and response generation
-- Using proper COMPLETE function syntax
SELECT 
    CI.CUSTOMER_ID,
    CP.NAME,
    CI.FEEDBACK_TEXT,
    -- Analyze sentiment
    SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', 
        'Analyze if this feedback is positive, negative, or neutral\: ' || CI.FEEDBACK_TEXT
    ) as SENTIMENT_ANALYSIS,
    -- Generate personalized response
    SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat',
        'Write a professional customer service response to this feedback\: ' || 
        'Customer Name\: ' || CP.NAME || 
        ', Account Type\: ' || CP.ACCOUNT_TYPE || 
        ', Tenure\: ' || CP.TENURE_YEARS || ' years' ||
        ', Feedback\: ' || CI.FEEDBACK_TEXT
    ) as SUGGESTED_RESPONSE
FROM CUSTOMER_INTERACTIONS CI
JOIN CUSTOMER_PROFILE CP ON CI.CUSTOMER_ID = CP.CUSTOMER_ID;


