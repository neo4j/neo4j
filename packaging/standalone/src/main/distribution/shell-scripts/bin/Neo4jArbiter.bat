@echo off
rem Copyright (c) 2002-2015 "Neo Technology,"
rem Network Engine for Objects in Lund AB [http://neotechnology.com]
rem
rem This file is part of Neo4j.
rem
rem Neo4j is free software: you can redistribute it and/or modify
rem it under the terms of the GNU General Public License as published by
rem the Free Software Foundation, either version 3 of the License, or
rem (at your option) any later version.
rem
rem This program is distributed in the hope that it will be useful,
rem but WITHOUT ANY WARRANTY; without even the implied warranty of
rem MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem GNU General Public License for more details.
rem
rem You should have received a copy of the GNU General Public License
rem along with this program.  If not, see <http://www.gnu.org/licenses/>.

set serviceName="Neo4jArbiter"
set serviceDisplayName="Neo4j-HA-Arbiter"
set serviceStartType=auto
set classpath="-DserverClasspath=lib/*.jar;system/lib/*.jar"
set mainclass="-DserverMainClass=org.neo4j.server.enterprise.StandaloneClusterClient"
set configFile="conf\arbiter-wrapper.conf"

call "%~dps0base.bat" %1 %2 %3 %4 %5"
