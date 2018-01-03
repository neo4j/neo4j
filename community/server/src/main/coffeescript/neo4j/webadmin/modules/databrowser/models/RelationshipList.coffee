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
  ['neo4j/webadmin/utils/ItemUrlResolver'
   './RelationshipProxy'
   'ribcage/Model'], 
  (ItemUrlResolver, RelationshipProxy, Model) ->
  
    class RelationshipList extends Model
      
      initialize : (relationships) =>
        @setRawRelationships(relationships || [])

      setRawRelationships : (relationships) =>
        @set "rawRelationships" : relationships        
        rels = []
        propertyKeyMap = {}
        for rel in relationships
          for key, value of rel.getProperties()
            propertyKeyMap[key] = true
          rels.push( new RelationshipProxy(rel) )

        propertyKeys = for key, value of propertyKeyMap
          key

        @set "propertyKeys" : propertyKeys
        @set "relationships" : rels

      getPropertyKeys : () =>
        @get "propertyKeys"

      getRelationships : () =>
        @get "relationships"

      getRawRelationships : () =>
        @get "rawRelationships"

)
