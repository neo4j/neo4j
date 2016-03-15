@ECHO OFF
rem Copyright (c) 2002-2016 "Neo Technology,"
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

SETLOCAL

Powershell -NoProfile -NonInteractive -NoLogo -ExecutionPolicy Bypass -Command "try { Unblock-File -Path '%~dp0Neo4j-Management\*.*' -ErrorAction 'SilentlyContinue' } catch {};Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Invoke-Neo4j %*)"
EXIT /B %ERRORLEVEL%