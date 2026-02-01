# Troubleshooting Guide

Common issues and solutions for Flux Data Forge deployment and operation.

## Deployment Issues

### Service Won't Start

**Symptoms:**
- `SYSTEM$GET_SERVICE_STATUS()` returns `PENDING` or `FAILED`
- Service never reaches `READY` state

**Solutions:**

1. **Check compute pool status:**
   ```sql
   DESCRIBE COMPUTE POOL <your_pool>;
   -- Must be ACTIVE or IDLE, not SUSPENDED or STARTING
   ```

2. **Check service logs:**
   ```sql
   CALL SYSTEM$GET_SERVICE_LOGS('FLUX_DATA_FORGE_SERVICE', '0', 'flux-data-forge', 100);
   ```

3. **Verify image exists:**
   ```sql
   SHOW IMAGES IN IMAGE REPOSITORY <your_repo>;
   -- Your image tag should be listed
   ```

4. **Common log errors:**
   - `ImagePullBackOff` → Image not found in repository. Re-run `build_and_push.sh`
   - `CrashLoopBackOff` → Application crashes on startup. Check Python errors in logs
   - `OOMKilled` → Out of memory. Increase `resources.limits.memory` in service spec

---

### Image Push Fails

**Symptoms:**
- `docker push` returns authentication errors
- "unauthorized" or "access denied" messages

**Solutions:**

1. **Re-authenticate to registry:**
   ```bash
   docker logout <org>-<account>.registry.snowflakecomputing.com
   docker login <org>-<account>.registry.snowflakecomputing.com
   # Use your Snowflake username and password
   ```

2. **Verify registry URL format:**
   ```
   <org>-<account>.registry.snowflakecomputing.com
   ```
   - `org` = Your Snowflake organization (lowercase)
   - `account` = Your account locator (lowercase)

3. **Check image repository exists:**
   ```sql
   SHOW IMAGE REPOSITORIES;
   ```

---

### Permission Errors

**Symptoms:**
- "Insufficient privileges" errors
- "Access denied" on service creation

**Solutions:**

1. **Required privileges for deployment:**
   ```sql
   -- Grant to your role
   GRANT CREATE SERVICE ON SCHEMA <db>.<schema> TO ROLE <your_role>;
   GRANT USAGE ON COMPUTE POOL <pool> TO ROLE <your_role>;
   GRANT READ ON IMAGE REPOSITORY <repo> TO ROLE <your_role>;
   ```

2. **For full setup (ACCOUNTADMIN or equivalent):**
   ```sql
   GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE <your_role>;
   GRANT CREATE IMAGE REPOSITORY ON SCHEMA <db>.<schema> TO ROLE <your_role>;
   ```

---

## Runtime Issues

### OAuth Token Expired

**Symptoms:**
- Application shows "Token expired" or error code `390114`
- Queries fail after service runs for a while

**Solution:**
The application automatically refreshes tokens. If you see persistent token errors:

1. Check service logs for refresh failures
2. Restart the service:
   ```sql
   ALTER SERVICE FLUX_DATA_FORGE_SERVICE SUSPEND;
   ALTER SERVICE FLUX_DATA_FORGE_SERVICE RESUME;
   ```

---

### No Data in Target Table

**Symptoms:**
- Streaming job shows "Running" but table is empty
- `SELECT COUNT(*) FROM AMI_STREAMING_READINGS` returns 0

**Solutions:**

1. **Check warehouse is running:**
   ```sql
   SHOW WAREHOUSES LIKE '<your_warehouse>';
   -- AUTO_RESUME should be TRUE
   ```

2. **Verify table exists in correct location:**
   ```sql
   SHOW TABLES LIKE 'AMI_STREAMING_READINGS' IN SCHEMA <db>.<schema>;
   ```

3. **Check service logs for insert errors:**
   ```sql
   CALL SYSTEM$GET_SERVICE_LOGS('FLUX_DATA_FORGE_SERVICE', '0', 'flux-data-forge', 100);
   ```

4. **Verify role has INSERT privilege:**
   ```sql
   SHOW GRANTS ON TABLE AMI_STREAMING_READINGS;
   ```

---

### S3 Streaming Not Working

**Symptoms:**
- "Stage Landing" data flow fails
- No files appearing in S3 bucket

**Solutions:**

1. **Verify AWS credentials are set:**
   - Check `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in service spec secrets

2. **Check IAM permissions:**
   - Role needs `s3:PutObject` on the target bucket/prefix

3. **Verify bucket exists and is accessible:**
   ```bash
   aws s3 ls s3://your-bucket/your-prefix/
   ```

4. **Check for EXTERNAL ACCESS INTEGRATION:**
   ```sql
   -- Service needs external network access for S3
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION s3_access
       ALLOWED_NETWORK_RULES = (...)
       ENABLED = TRUE;
   
   -- Then add to service:
   ALTER SERVICE FLUX_DATA_FORGE_SERVICE 
       SET EXTERNAL_ACCESS_INTEGRATIONS = (s3_access);
   ```

---

### Postgres Dual-Write Fails

**Symptoms:**
- "Dual Write" data flow shows errors
- Snowflake receives data but Postgres doesn't

**Solutions:**

1. **Verify Postgres credentials:**
   - Check `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD` in service spec

2. **Check network connectivity:**
   - SPCS services need EXTERNAL ACCESS INTEGRATION to reach Postgres

3. **Verify Postgres table exists:**
   ```sql
   -- Create matching table in Postgres
   CREATE TABLE ami_streaming_readings (
       meter_id VARCHAR(50),
       reading_timestamp TIMESTAMP,
       usage_kwh FLOAT,
       ...
   );
   ```

---

## Performance Issues

### Slow Data Generation

**Symptoms:**
- Generation takes longer than expected
- Low rows/second throughput

**Solutions:**

1. **Increase compute resources:**
   ```yaml
   # In service_spec.yaml
   resources:
     requests:
       cpu: 2
       memory: 4Gi
     limits:
       cpu: 4
       memory: 8Gi
   ```

2. **Use smaller batch sizes:**
   - Large batches (>10,000 rows) may cause memory pressure

3. **Check warehouse size:**
   - For batch inserts, larger warehouse = faster inserts

---

### High Memory Usage

**Symptoms:**
- Service restarts unexpectedly (OOMKilled)
- Slow response times

**Solutions:**

1. **Reduce concurrent streaming jobs:**
   - Stop unused streaming jobs from the UI

2. **Lower batch sizes in streaming config**

3. **Increase memory limits in service spec**

---

## Getting Help

If you're still stuck:

1. **Gather diagnostic info:**
   ```sql
   -- Service status
   SELECT SYSTEM$GET_SERVICE_STATUS('FLUX_DATA_FORGE_SERVICE');
   
   -- Recent logs
   CALL SYSTEM$GET_SERVICE_LOGS('FLUX_DATA_FORGE_SERVICE', '0', 'flux-data-forge', 500);
   
   -- Compute pool status
   DESCRIBE COMPUTE POOL <your_pool>;
   ```

2. **Check Snowflake documentation:**
   - [SPCS Overview](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
   - [Troubleshooting SPCS](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/troubleshooting)
   - [Service Logs](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services#getting-the-status-of-a-service)
   - [Compute Pools](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-compute-pool)

3. **Contact support:**
   - Open an issue in this repository with diagnostic output
   - Include: service logs, compute pool status, and steps to reproduce

---

## Quick Reference: Snowflake Documentation

| Topic | Link |
|-------|------|
| SPCS Overview | [docs.snowflake.com/.../overview](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview) |
| Image Registry | [docs.snowflake.com/.../working-with-registry-repository](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository) |
| Compute Pools | [docs.snowflake.com/.../working-with-compute-pool](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-compute-pool) |
| Service Specification | [docs.snowflake.com/.../specification-reference](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference) |
| External Access | [docs.snowflake.com/.../external-network-access-overview](https://docs.snowflake.com/en/developer-guide/external-network-access/external-network-access-overview) |
| Secrets | [docs.snowflake.com/.../create-secret](https://docs.snowflake.com/en/sql-reference/sql/create-secret) |
| Snowpipe Streaming | [docs.snowflake.com/.../data-load-snowpipe-streaming-overview](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview) |
| Storage Integration | [docs.snowflake.com/.../create-storage-integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) |
