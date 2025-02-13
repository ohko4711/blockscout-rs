#!/bin/bash

# pg dump sql 
# pg_dump -U ddl -h localhost -d ddl-test -p 5432 -Fc --schema sgd1   -v  > sgd1.dump

# VAR
DUMP_FILE="sgd1.dump"
CONTAINER_NAME="gn-postgres-dev"
PG_USER="graph-node"
PG_DB="graph-node"
SCHEMA_NAME="sgd1"
TABLE_NAME="domain"

# Create the 'ddl' role if it doesn't exist and drop existing objects
docker exec -i ${CONTAINER_NAME} psql -U ${PG_USER} -d ${PG_DB} -c "
DO \$\$
BEGIN
    -- Create ddl role if not exists
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ddl') THEN
        CREATE ROLE ddl;
    END IF;
   Create schema if not exists sgd1;
END
\$\$;
"

# 使用 pg_restore 导入自定义格式的 dump 文件
docker exec -i ${CONTAINER_NAME} pg_restore  -U ${PG_USER} -d ${PG_DB}  -a  -v < "${DUMP_FILE}"

# cp from public.domain to sgd1.domain
# docker exec -i ${CONTAINER_NAME} psql -U ${PG_USER} -d ${PG_DB} -c "
# CREATE TABLE sgd1.domain (LIKE public.domains INCLUDING ALL);
# INSERT INTO sgd1.domain SELECT * FROM public.domains;
# "