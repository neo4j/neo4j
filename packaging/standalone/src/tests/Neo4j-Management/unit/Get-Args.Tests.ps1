# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.",".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
.$common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Args" {

    It "should return Verbose=false when args is empty" {
      $args = Get-Args

      $args.Verbose | Should Be $false
      $args.Args.Count | Should Be 0
      $args.ArgsAsStr | Should Be ''
    }

    It "should return Verbose=true when -v is passed" {
      $args = Get-Args @('-v','some','other','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=true when -V is passed" {
      $args = Get-Args @('some','-V','other','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -verbose is passed" {
      $args = Get-Args @('some','other','-verbose','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -Verbose is passed" {
      $args = Get-Args @('some','other','-Verbose','args')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return verbose=true when -VeRbOsE is passed" {
      $args = Get-Args @('some','other','args','-VeRbOsE')

      $args.Verbose | Should Be $true
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=false when no verbose argument is passed" {
      $args = Get-Args @('some','other','args')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','args')
      $args.ArgsAsStr | Should Be 'some other args'
    }

    It "should return Verbose=false when -verb argument is passed" {
      $args = Get-Args @('some','other','args','-verb')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','args','-verb')
      $args.ArgsAsStr | Should Be 'some other args -verb'
    }

    It "should return Verbose=false when no-verbose argument is passed" {
      $args = Get-Args @('some','other','no-verbose','args')

      $args.Verbose | Should Be $false
      $args.Args | Should Be @('some','other','no-verbose','args')
      $args.ArgsAsStr | Should Be 'some other no-verbose args'
    }
  }
}
