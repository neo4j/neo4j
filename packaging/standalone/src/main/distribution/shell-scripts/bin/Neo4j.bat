@echo off
rem Copyright (c) 2002-2014 "Neo Technology,"
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

echo  +=~~~~+
echo +~~~~~~~~ OOO
echo =~~~~~~~~  O OOO                                                          O
echo +~~~~~~~~      ++=~~~~=+      OOOO  OOOOOO                        OO     OOO
echo  =~~~~~=     ++~~~~~~~~~~+     OOOO   OO                         OOO      O
echo             ++~~~~~~~~~~~~+    OOOOO  OO    OOOOO    OOOOO      OOO    OOOOO
echo   OO        +~~~~~~~~~~~~~~    OO OOO OO   OO  OOO  OOO OOO    OOO OO  OOOOO
echo             +~~~~~~~~~~~~~~+   OO  OO OO  OOO   OO  OO   OOO  OOO  OO    OOO
echo   OO        +~~~~~~~~~~~~~~+   OO   OOOO  OOOOOOOO  OO   OOO OOOOOOOOOO  OOO
echo   OO        +~~~~~~~~~~~~~=    OO   OOOO  OOO       OO   OOO OOOOOOOOOO  OOO
echo    O         +~~~~~~~~~~~+    OOOOO  OOO   OOOOOOO  OOOOOOO        OO    OOO
echo    OO          +~~~~~~~=+                     O        OO          OO    OOO
echo     OO            +++                                                    OOO
echo     OO  ++         OO                                                 OOOOO
echo      +~~~~~~~+     OO
echo     +~~~~~~~~~~  OO
echo    +~~~~~~~~~~~+
echo    +~~~~~~~~~~~=
echo    +~~~~~~~~~~~+
echo    +~~~~~~~~~~~
echo      =~~~~~~~+
echo        ++=++


set serviceName=Neo4j-Server
set serviceDisplayName=Neo4j-Server
set serviceStartType=auto
set classpath="-DserverClasspath=lib/*.jar;system/lib/*.jar;plugins/**/*.jar;./conf*"
set mainclass="-DserverMainClass=org.neo4j.server.Bootstrapper"
set configFile="conf\neo4j-wrapper.conf"

call "%~dps0base.bat" %1 %2 %3 %4 %5
