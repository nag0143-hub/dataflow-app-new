#!/bin/bash
sleep 10
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -P 'DataFlow#2026!' -C -i /docker-entrypoint-initdb.d/setup.sql
echo "SQL Server sample data loaded."
