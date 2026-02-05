-- =============================================================================
-- Flux Data Forge - 03: Compute Pool
-- =============================================================================
-- Creates the SPCS compute pool for running the service.
--
-- Variables (Jinja2 syntax for Snow CLI):
--   <% compute_pool %>     - Compute pool name
--   <% instance_family %>  - Instance type (default: CPU_X64_S)
--   <% min_nodes %>        - Minimum nodes (default: 1)
--   <% max_nodes %>        - Maximum nodes (default: 2)
--
-- Usage:
--   snow sql -f scripts/sql/03_compute_pool.sql \
--       -D "compute_pool=FLUX_DATA_FORGE_POOL" \
--       -D "instance_family=CPU_X64_S" \
--       -D "min_nodes=1" \
--       -D "max_nodes=2" \
--       -c your_connection_name
--
-- Note: Compute pools can take 1-2 minutes to reach ACTIVE state.
-- =============================================================================

-- Create compute pool
-- Options for INSTANCE_FAMILY:
--   CPU_X64_XS  - Extra small (1 vCPU, 4GB RAM)
--   CPU_X64_S   - Small (2 vCPU, 8GB RAM) - Recommended for Data Forge
--   CPU_X64_M   - Medium (4 vCPU, 32GB RAM)
--   CPU_X64_L   - Large (8 vCPU, 64GB RAM)
--   GPU_NV_S    - GPU Small (for ML workloads)

CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER('<% compute_pool %>')
    MIN_NODES = <% min_nodes %>
    MAX_NODES = <% max_nodes %>
    INSTANCE_FAMILY = <% instance_family %>
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Flux Data Forge SPCS service';

-- Check compute pool status
-- Status should be: STARTING -> IDLE -> ACTIVE (when service runs)
DESCRIBE COMPUTE POOL IDENTIFIER('<% compute_pool %>');

-- Wait for pool to be ready before creating service
-- You can re-run DESCRIBE to check status
SELECT 
    'Compute Pool' as STEP,
    '<% compute_pool %>' as POOL_NAME,
    'Wait for IDLE or ACTIVE status before proceeding' as NEXT_ACTION,
    'SUCCESS' as STATUS;
