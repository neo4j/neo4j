/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.docgen

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.junit.Test
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class CreateUniqueTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  
  override def graphDescription = List(
    "root X A",
    "root X B",
    "root X C",
    "A KNOWS C"
  )

  def section = "Create Unique"

  @Test def create_relationship_when_missing() {
    testQuery(
      title = "Create relationship if it is missing",
      text = "+CREATE UNIQUE+ is used to describe the pattern that should be found or created.",
      queryText = "match (lft {name: 'A'}), (rgt) where rgt.name in ['B','C'] create unique (lft)-[r:KNOWS]->(rgt) return r",
      optionalResultExplanation = "The left node is matched agains the two right nodes. One relationship already exists and can be " +
        "matched, and the other relationship is created before it is returned.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1))
  }

  @Test def create_node_if_it_is_missing() {
    testQuery(
      title = "Create node if missing",
      text = "If the pattern described needs a node, and it can't be matched, a new node will be created.",
      queryText = "match (root {name: 'root'}) create unique (root)-[:LOVES]-(someone) return someone",
      optionalResultExplanation = "The root node doesn't have any `LOVES` relationships, and so a node is created, and also a relationship " +
        "to that node.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1, nodesCreated = 1))
  }

  @Test def create_node_with_values() {
    testQuery(
      title = "Create nodes with values",
      text = "The pattern described can also contain values on the node. These are given using the following syntax: `prop : <expression>`.",
      queryText = "match (root {name: 'root'}) create unique (root)-[:X]-(leaf {name:'D'} ) return leaf",
      optionalResultExplanation = "No node connected with the root node has the name +D+, and so a new node is created to " +
        "match the pattern.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1))
  }

  @Test def create_relationship_with_values() {
    testQuery(
      title = "Create relationship with values",
      text = "Relationships to be created can also be matched on values.",
      queryText = "match (root {name: 'root'}) create unique (root)-[r:X {since:'forever'}]-() return r",
      optionalResultExplanation = "In this example, we want the relationship to have a value, and since no such relationship can be found," +
        " a new node and relationship are created. Note that since we are not interested in the created node, we don't " +
        "name it.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1))
  }

  @Test def commad_separated_pattern() {
    testQuery(
      title = "Describe complex pattern",
      text = "The pattern described by +CREATE UNIQUE+ can be separated by commas, just like in +MATCH+ and +CREATE+.",
      queryText = "match (root {name: 'root'}) create unique (root)-[:FOO]->(x), (root)-[:BAR]->(x) return x",
      optionalResultExplanation = "This example pattern uses two paths, separated by a comma.",
      assertions = (p) => assertStats(p, relationshipsCreated = 2, nodesCreated = 1))
  }

  @Test def create_labeled_node_if_labels_missing() {
    testQuery(
      title = "Create labeled node if missing",
      text = "If the pattern described needs a labeled node and there is none with the given labels, " +
             "Cypher will create a new one.",
      queryText = "match (a {name: 'A'}) create unique (a)-[:KNOWS]-(c:blue) return c",
      optionalResultExplanation = "The A node is connected in a `KNOWS` relationship to the c node, but since C doesn't have " +
                "the `:blue` label, a new node labeled as `:blue` is created along with a `KNOWS` relationship "+
                "from A to it.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1, nodesCreated = 1, labelsAdded = 1))
  }
}
