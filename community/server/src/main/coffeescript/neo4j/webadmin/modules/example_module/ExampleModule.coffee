###
  Copyright (c) 2002-2012 "Neo Technology,"
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
###
To get code to run within webadmin, 
create some object with an init method like
the one below.

Then import your object in the webadmin.coffee file,
and add it to the list of modules to load there.

See for instance the databrowser module for examples
of how to incorporate url routing, views, models and
module-specific settings.
###
define( 
  [],
  () ->

    class ExampleModule

      ###
      The init method will get passed an instance of
      ApplicationState, which gives you access to 
      settings and a GraphDatabase instance.
      ###
      init : (applicationState) ->
        # Do things.

)
