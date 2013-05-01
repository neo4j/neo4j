/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.StatisticsChecker
import org.junit.Test

class MergeTest extends DocumentingTestBase with StatisticsChecker {
  def section = "Merge"

  def graphDescription = List(
    "Charlie:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN WallStreet:Movie",
    "Michael:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN TheAmericanPresident:Movie",
    "Michael:Person ACTED_IN TheAmericanPresident:Movie",
    "Oliver:Person DIRECTED WallStreet:Movie",
    "Rob:Person DIRECTED TheAmericanPresident:Movie",
    "Charlie:Person FATHER Martin:Person")

  override val properties = Map(
    "Charlie" -> Map("name" -> "Charlie Sheen"),
    "Oliver" -> Map("name" -> "Oliver Stone"),
    "Michael" -> Map("name" -> "Michael Douglas"),
    "Rob" -> Map("name" -> "Rob Reiner"),
    "Martin" -> Map("name" -> "Martin Sheen"),
    "WallStreet" -> Map("title" -> "Wall Street"),
    "TheAmericanPresident" -> Map("title" -> "The American President")
  )

  @Test def merge_single_node_with_label() {
    testQuery(
      title = "Merge single node with a label",
      text = "Merging a single node with a given label.",
      queryText = "merge (robert:Critic)\nreturn robert, labels(robert)",
      returns = "Because there are no nodes labeled Critic in the database, a new node is created.",
      assertions = (p) => assertStats(p, nodesCreated = 1, labelsAdded = 1)
    )
  }

  @Test def merge_single_node_with_properties() {
    testQuery(
      title = "Merge single node with properties",
      text = "Merging a single node with properties where not all matches any existing node.",
      queryText = "merge (charlie {name:'Charlie Sheen', age:10})\nreturn charlie",
      returns = "A new node with the name Charlie Sheen will be created since not all properties " +
        "matched the existing Charlie Sheen node.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2)
    )
  }

  @Test def merge_single_node_with_label_and_property() {
    testQuery(
      title = "Merge single node specifying both label and property",
      text = "Merging a single node with both label and property matching an existing node.",
      queryText = "merge (michael:Person {name:'Michael Douglas'})\nreturn michael",
      returns = "Michael Douglas will be matched and returned",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesSet = 0)
    )
  }

  @Test def merge_node_and_set_property_on_creation() {
    testQuery(
      title = "Merge with ON CREATE",
      text = "Merge a node and set properties if the node needs to be created.",
      queryText = """merge (keanu:Person {name:'Keanu Reeves'})
on create keanu set keanu.created = timestamp()
return keanu""",
      returns = "Creates the Keanu node, and sets a timestamp on creation time.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
    )
  }

  @Test def merge_node_and_set_property_on_match() {
    testQuery(
      title = "Merge with ON MATCH",
      text = "Merging nodes and setting properties on found nodes.",
      queryText = "merge (person:Person)\non match person set person.found = true\nreturn person",
      returns = "Finds all the :Person nodes, sets a property on them, and returns them.",
      assertions = (p) => assertStats(p, propertiesSet = 5)
    )
  }

  @Test def merge_node_and_set_property_on_creation_or_update_prop() {
    testQuery(
      title = "Merge with ON CREATE and ON MATCH",
      text = "Merge a node and set properties if the node needs to be created.",
      queryText =
        """merge (keanu:Person {name:'Keanu Reeves'})
on create keanu set keanu.created = timestamp()
on match keanu set keanu.lastSeen = timestamp()
return keanu""",
      returns = "Creates the Keanu node, and sets a timestamp on creation time. If Keanu already existed, a " +
        "different property would be set",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
    )
  }


}