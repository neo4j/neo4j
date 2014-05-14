###!
Copyright (c) 2002-2014 "Neo Technology,"
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

'use strict';

angular.module('neo4jApp.filters')
  .filter 'duration', [() ->
    (duration) ->
      milliseconds = parseInt((duration%1000)/100)
      seconds = parseInt((duration/1000)%60)
      minutes = parseInt((duration/(1000*60))%60)
      hours = parseInt((duration/(1000*60*60))%24)

      hours = if (hours < 10) then "0" + hours else hours
      minutes = if (minutes < 10) then "0" + minutes else minutes
      seconds = if (seconds < 10) then "0" + seconds else seconds

      hours + ":" + minutes + ":" + seconds + "." + milliseconds

  ]
