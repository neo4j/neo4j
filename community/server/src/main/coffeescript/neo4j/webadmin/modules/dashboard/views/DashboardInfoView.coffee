###
Copyright (c) 2002-2018 "Neo Technology,"
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

define( 
  ['./info'
   'ribcage/View'
   'ribcage/ui/NumberFormatter'
   'lib/amd/jQuery'], 
  (template, View, NumberFormatter, $ ) ->
  
    class DashboardInfoView extends View
      
      template : template
     
      initialize : (opts) =>
        @primitives = opts.primitives
        @diskUsage = opts.diskUsage
        
        @primitives.bind("change",@render)
        @diskUsage.bind("change",@render)

      render : =>
        $(@el).html @template
          primitives  : @primitives
          diskUsage   : @diskUsage
          fancyNumber : NumberFormatter.fancy
        return this

      remove : =>
        @primitives.unbind("change",@render)
        @diskUsage.unbind("change",@render)
        super()
)
