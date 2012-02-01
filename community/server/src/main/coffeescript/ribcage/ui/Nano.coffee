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

define [], () ->
  # Nano Templates (Tomasz Mazur, Jacek Becela)
  class Nano
    
    @pipes :
      'truncate' : (value, args) ->
        maxlen = Number(args[0])
        if maxlen isnt NaN and value.length > maxlen
          return value[0...maxlen]
        return value
    
    @compile : (template, data) ->
      template.replace /\{([\w\-\,\.\|:]*)}/g, (str, key) ->
        pipes = key.split('|')
        keySets = for path in pipes.shift().split(",")
          path.split(".")
        
        for keys in keySets
          value = data[keys.shift()]
          for key in keys
            if value? and value.hasOwnProperty(key) then value = value[key] 
            else value = null
          
          if value?      
            # Pipe the value through whatever functions was asked for
            for pipe in pipes
              [name, args] = pipe.split ':'
              args = args.split(',')
              value = Nano.pipes[name](value, args)
            
            return value
        return "N/A"

