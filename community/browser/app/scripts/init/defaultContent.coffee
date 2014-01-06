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

angular.module('neo4jApp')
.run([
  '$rootScope'
  'Document'
  'Folder'
  ($rootScope, Document, Folder) ->
    general_scripts = [
      {
        folder: 'general'
        content: """
// Create a node
CREATE (n {name:"World"}) RETURN "hello", n.name
        """
      }
      {
        folder: 'general'
        content: """
// Get some data
MATCH (n) RETURN n LIMIT 100
        """
      }
      {
        folder: 'general'
        content: """
// What is related, and how
MATCH (a)-[r]->(b)
WHERE labels(a) <> [] AND labels(b) <> []
RETURN DISTINCT head(labels(a)) AS This, type(r) as To, head(labels(b)) AS That
LIMIT 10
        """
      }
      {
        folder: 'general'
        content: """
// REST API
:GET /db/data
        """
      }
    ]

    node_scripts = [
      {
        folder: 'nodes'
        content: """
// Count nodes
// Warning: may take a long time.
MATCH (n)
RETURN count(n)
        """
      }
      {
        folder: 'nodes'
        content: """
// Create index
// Replace:
//   'LabelName' with label to index
//   'propertyKey' with property to be indexed
CREATE INDEX ON :LabelName(propertyKey)
        """
      }
      {
        folder: 'nodes'
        content: """
// Create indexed node
// Replace:
//   'LabelName' with label to apply to new node
//   'propertyKey' with property to add
//   'property_value' with value of the added property
CREATE (n:LabelName { propertyKey:"property_value" }) RETURN n
        """
      }
      {
        folder: 'nodes'
        content: """
// Create node
CREATE (n) RETURN n
        """
      }
      {
        folder: 'nodes'
        content: """
// Delete a node
// Replace:
//   'LabelName' with label of node to delete
//   'propertyKey' with property to find
//   'expected_value' with value of property
START n=node(*) 
MATCH (n:LabelName)-[r?]-()
WHERE n.propertyKey = "expected_value"
DELETE n,r
        """
      }
      {
        folder: 'nodes'
        content: """
// Drop index
// Replace:
//   'LabelName' with label index
//   'propertyKey' with indexed property
DROP INDEX ON :LabelName(propertyKey)
        """
      }
      {
        folder: 'nodes'
        content: """
// Find a node
MATCH (n{{':'+label-name}})
WHERE n.{{property-name}} = "{{property-value}}" RETURN n
        """
      }
    ]

    relationship_scripts = [
      {
        folder: 'relationships'
        content: """
// Isolate node
// Description: Delete some relationships to a particular node
// Replace:
//   'RELATIONSHIP' with relationship type to match (or remove for all)
//   'propertyKey' with property by which to find the node
//   'expected_value' with the property value to find
MATCH (a)-[r:RELATIONSHIP]-()
WHERE a.propertyKey = "expected_value"
DELETE r
        """
      }
      {
        folder: 'relationships'
        content: """
// Relate nodes
// Replace:
//   'propertyKey' with property to evaluate on either node
//   'expected_value_a' with property value to find node a
//   'expected_value_b' with property value to find node b
//   'RELATIONSHP' with type of new relationship between a and b
MATCH (a),(b)
WHERE a.propertyKey = "expected_value_a"
AND b.propertyKey = "expected_value_b"
CREATE (a)-[r:RELATIONSHIP]->(b)
RETURN a,r,b
        """
      }
      {
        folder: 'relationships'
        content: """
// Shortest path
// Replace:
//   'propertyKey' with property to evaluate on either node
//   'expected_value_a' with property value to find node a
//   'expected_value_b' with property value to find node b
MATCH p = shortestPath( (a)-[*..4]->(b) )
WHERE a.propertyKey='expected_value_a' AND b.propertyKey='expected_value_b'
RETURN p
        """
      }
      {
        folder: 'relationships'
        content: """
// Whats related
// Description: find a random sample of nodes, revealing how they are related
MATCH (a)-[r]-(b)
RETURN DISTINCT head(labels(a)), type(r), head(labels(b)) LIMIT 100
        """
      }
    ]

    system_scripts = [
      {
        folder: 'system'
        content: """
// Server configuration
:GET /db/manage/server/jmx/domain/org.neo4j/instance%3Dkernel%230%2Cname%3DConfiguration
        """
      }
      {
        folder: 'system'
        content: """
// Kernel information
:GET /db/manage/server/jmx/domain/org.neo4j/instance%3Dkernel%230%2Cname%3DKernel
        """
      }
      {
        folder: 'system'
        content: """
// ID Allocation
:GET /db/manage/server/jmx/domain/org.neo4j/instance%3Dkernel%230%2Cname%3DPrimitive%20count
        """
      }
      {
        folder: 'system'
        content: """
// Store file sizes
:GET /db/manage/server/jmx/domain/org.neo4j/instance%3Dkernel%230%2Cname%3DStore%20file%20sizes
        """
      }
      {
        folder: 'system'
        content: """
// Extensions
:GET /db/data/ext
        """
      }
      
    ]

    folders = [
      {
        id: "general"
        name: "General"
        expanded: yes
      }
      # {
      #   id: "nodes"
      #   name: "Nodes"
      #   expanded: no
      # }
      # {
      #   id: "relationships"
      #   name: "Relationships"
      #   expanded: no
      # }
      {
        id: "system"
        name: "System"
        expanded: no
      }
    ]

    # Restore default content if empty
    if Document.length is 0
      Document.add(general_scripts.concat(system_scripts)).save()
      Folder.add(folders).save()

    # Find and restore orphan folders
    for doc in Document.all()
      continue unless doc.folder
      if not Folder.get(doc.folder)
        Folder.create(id: doc.folder)

])
