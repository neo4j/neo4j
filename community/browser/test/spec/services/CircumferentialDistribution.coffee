###!
Copyright (c) 2002-2015 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

'use strict'

describe 'Service: CircumferentialDistribution', () ->

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Distribution = {}
  beforeEach inject (_CircumferentialDistribution_) ->
    Distribution = _CircumferentialDistribution_

  it 'should leave arrows alone that are far enough apart', () ->
    arrowsThatAreAlreadyFarEnoughApart =
        0: 0
        1: 120
        2: 240
    result = Distribution.distribute(
      floating: arrowsThatAreAlreadyFarEnoughApart
      fixed: {}
    , 20)
    expect(result).toEqual(arrowsThatAreAlreadyFarEnoughApart)

  it 'should spread out arrows that are too close together', () ->
    arrowsThatAreTooCloseTogether =
      0: 160
      1: 170
      2: 180
      3: 190
      4: 200
    result = Distribution.distribute(
      floating: arrowsThatAreTooCloseTogether
      fixed: {}
    , 20)
    expect(result).toEqual(
      0: 140
      1: 160
      2: 180
      3: 200
      4: 220
    )

  it 'should spread out arrows that are too close together, wrapping across 0 degrees', () ->
    arrowsThatAreTooCloseTogether =
      0: 340
      1: 350
      2: 0
      3: 10
      4: 20
    result = Distribution.distribute(
      floating: arrowsThatAreTooCloseTogether
      fixed: {}
    , 20)
    expect(result).toEqual(
      0: 320
      1: 340
      2: 0
      3: 20
      4: 40
    )

  it 'should leave arrows alone whose positions have already been fixed, and distribute between them', () ->