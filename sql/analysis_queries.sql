-- ============================================
-- Medicare Claims Analysis Queries
-- ============================================

-- Average Medicare payment by state
-- Shows which states have the highest average Medicare reimbursements
-- and total volume of services billed
SELECT 
    provider_state,
    COUNT(*) as total_records,
    ROUND(AVG(avg_medicare_payment), 2) as avg_medicare_payment,
    ROUND(SUM(total_services), 0) as total_services
FROM fact_claims
GROUP BY provider_state
ORDER BY avg_medicare_payment DESC
LIMIT 20;

-- Top 20 most billed procedures
-- Shows the most frequently billed HCPCS procedure codes across all providers
-- Joins to dim_procedure to get human readable descriptions
SELECT 
    f.hcpcs_code,
    d.hcpcs_description,
    ROUND(SUM(f.total_services), 0) as total_services,
    ROUND(AVG(f.avg_medicare_payment), 2) as avg_medicare_payment
FROM fact_claims f
JOIN dim_procedure d ON f.hcpcs_code = d.hcpcs_code
GROUP BY f.hcpcs_code, d.hcpcs_description
ORDER BY total_services DESC
LIMIT 20;

-- Most expensive provider specialties
-- Compares average Medicare payment vs submitted charge by specialty
-- The payment gap reveals how much providers overbill above what Medicare approves
SELECT 
    p.provider_type,
    COUNT(*) as total_records,
    ROUND(AVG(f.avg_medicare_payment), 2) as avg_medicare_payment,
    ROUND(AVG(f.avg_submitted_charge), 2) as avg_submitted_charge,
    ROUND(AVG(f.avg_submitted_charge - f.avg_medicare_payment), 2) as avg_payment_gap
FROM fact_claims f
JOIN dim_provider p ON f.provider_npi = p.provider_npi
GROUP BY p.provider_type
ORDER BY avg_medicare_payment DESC
LIMIT 20;

-- Payment rates by place of service
-- Compares Medicare reimbursement between office and facility settings
-- Medicare intentionally pays less for facility-based care since 
-- the facility receives a separate payment
SELECT 
    place_of_service,
    COUNT(*) as total_records,
    ROUND(AVG(avg_medicare_payment), 2) as avg_medicare_payment,
    ROUND(AVG(avg_submitted_charge), 2) as avg_submitted_charge,
    ROUND(SUM(total_services), 0) as total_services
FROM fact_claims
GROUP BY place_of_service
ORDER BY avg_medicare_payment DESC;

-- Top 20 providers by total services billed
-- Identifies the highest volume providers in the Medicare system
-- Joins to dim_provider to get provider name and specialty
SELECT 
    f.provider_npi,
    p.provider_last_org_name,
    p.provider_first_name,
    p.provider_type,
    p.provider_state,
    ROUND(SUM(f.total_services), 0) as total_services,
    ROUND(AVG(f.avg_medicare_payment), 2) as avg_medicare_payment
FROM fact_claims f
JOIN dim_provider p ON f.provider_npi = p.provider_npi
GROUP BY f.provider_npi, p.provider_last_org_name, p.provider_first_name, p.provider_type, p.provider_state
ORDER BY total_services DESC
LIMIT 20;

