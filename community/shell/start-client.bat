@echo off

call mvn dependency:copy-dependencies
call java -cp target\classes;target\dependency\* org.neo4j.shell.StartClient %*
