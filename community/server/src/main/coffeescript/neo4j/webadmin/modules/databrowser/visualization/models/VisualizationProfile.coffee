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

define(
  ['./StyleRules',
   './style',
   'ribcage/LocalModel'], 
  (StyleRules, style, LocalModel) ->

    class VisualizationProfile extends LocalModel
      
      initialize : () ->
        @initNestedModel('styleRules', StyleRules)
        @_defaultNodeStyle = new style.NodeStyle
        @_defaultGroupStyle = new style.GroupStyle
      
      setName : (name) -> @set name:name
      getName : () -> @get "name"
        
      isDefault : () -> @get "builtin"
      
      # Given a visualization node, 
      # apply appropriate style attributes
      styleNode : (visualNode) ->
        
        type = if visualNode.type is "group" then "group" else "node"
        
        switch type
          when "group" then @_defaultGroupStyle.applyTo visualNode
          when "node" then @_defaultNodeStyle.applyTo visualNode
        
        rules = @styleRules.models
        for i in [rules.length-1..0] by -1
          rule = rules[i]
          if rule.appliesTo visualNode, type
            rule.applyStyleTo visualNode
            
        if visualNode.type is "unexplored"
          visualNode.style.shapeStyle.alpha = 0.2
      
)
