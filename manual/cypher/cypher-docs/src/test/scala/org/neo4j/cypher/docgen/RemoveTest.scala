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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class RemoveTest extends DocumentingTestBase with SoftReset {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override def graphDescription = List(
    "Andres:Swedish KNOWS Tobias:Swedish",
    "Andres KNOWS Peter:German:Swedish"
  )

  override val properties = Map(
    "Andres" -> Map("age" -> 36l),
    "Tobias" -> Map("age" -> 25l),
    "Peter" -> Map("age" -> 34l)
  )

  def section = "Remove"

  @Test def remove_property() {
    testQuery(
      title = "Remove a property",
      text = "Neo4j doesn't allow storing +null+ in properties. Instead, if no value exists, the property is " +
        "just not there. So, to remove a property value on a node or a relationship, is also done with +REMOVE+.",
      queryText = "match (andres {name: 'Andres'}) remove andres.age return andres",
      optionalResultExplanation = "The node is returned, and no property `age` exists on it.",
      assertions = (p) => assertFalse("Property was not removed as expected.", node("Andres").hasProperty("age")) )
  }

  @Test def remove_a_label_from_a_node() {
    testQuery(
      title = "Remove a label from a node",
      text = "To remove labels, you use +REMOVE+.",
      queryText = "match (n {name: 'Peter'}) remove n:German return n",
      optionalResultExplanation = "",
      assertions = (p) => assert(getLabelsFromNode(p) === List("Swedish"))
    )
  }

  @Test def remove_multiple_labels_from_a_node() {
    testQuery(
      title = "Removing multiple labels",
      text = "To remove multiple labels, you use +REMOVE+.",
      queryText = "match (n {name: 'Peter'}) remove n:German:Swedish return n",
      optionalResultExplanation = "",
      assertions = (p) => assert(getLabelsFromNode(p).isEmpty)
    )
  }
}
