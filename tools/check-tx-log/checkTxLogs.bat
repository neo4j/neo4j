@echo off

call mvn dependency:copy-dependencies

call java -cp "target\dependency\*;target\classes" org.neo4j.tools.txlog.CheckTxLogs %*
