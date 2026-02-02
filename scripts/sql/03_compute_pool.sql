-- =============================================================================
-- Flux Data Forge - 03: Compute Pool
-- =============================================================================
-- Creates the SPCS compute pool for running the service.
--
-- Prerequisites:
--   - Run 01_database_schema.sql first
--   - Account must have SPCS enabled
--
-- Note: Compute pools can take 1-2 minutes to reach ACTIVE state.
-- =============================================================================

-- Configuration
SET compute_pool_name = 'FLUX_DATA_FORGE_POOL';

-- Create compute pool
-- Options for INSTANCE_FAMILY:
--   CPU_X64_XS  - Extra small (1 vCPU, 4GB RAM)
--   CPU_X64_S   - Small (2 vCPU, 8GB RAM) - Recommended for Data Forge
--   CPU_X64_M   - Medium (4 vCPU, 32GB RAM)
--   CPU_X64_L   - Large (8 vCPU, 64GB RAM)
--   GPU_NV_S    - GPU Small (for ML workloads)

CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($compute_pool_name)
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Flux Data Forge SPCS service';

-- Check compute pool status
-- Status should be: STARTING -> IDLE -> ACTIVE (when service runs)
DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);

-- Wait for pool to be ready before creating service
-- You can re-run DESCRIBE to check status
SELECT 
    'Compute Pool' as STEP,
    $compute_pool_name as POOL_NAME,
    'Wait for IDLE or ACTIVE status before proceeding' as NEXT_ACTION,
    'SUCCESS' as STATUS;
