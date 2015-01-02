/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.QueryStatisticsTestSupport

class SetTest extends DocumentingTestBase with QueryStatisticsTestSupport {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override def graphDescription = List(
    "Andres:Swedish KNOWS Peter",
    "Stefan KNOWS Andres",
    "Emil KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l, "awesome" -> true),
    "Peter" -> Map("age" -> 34l))

  def section = "Set"

  @Test def set_property_on_node() {
    testQuery(
      title = "Set a property",
      text = "To set a property on a node or relationship, use +SET+.",
      queryText = "match (n {name: 'Andres'}) set n.surname = 'Taylor' return n",
      optionalResultExplanation = "The newly changed node is returned by the query.",
      assertions = (p) => assert(node("Andres").getProperty("surname") === "Taylor"))
  }

  @Test def set_property_to_null() {
    testQuery(
      title = "Remove a property",
      text = """Normally you remove a property by using +<<query-remove,REMOVE>>+, but it's sometimes handy to do
it using the +SET+ command. One example is if the property comes from a parameter.""",
      queryText = "match (n {name: 'Andres'}) set n.name = null return n",
      optionalResultExplanation = "The node is returned by the query, and the name property is now missing.",
      assertions = (p) => assertFalse(node("Andres").hasProperty("name")))
  }

  @Test def set_properties_from_other_graph_element() {
    testQuery(
      title = "Copying properties between nodes and relationships",
      text =
        """You can also use +SET+ to copy all properties from one graph element to another. Remember that doing this
will remove all other properties on the receiving graph element.""".stripMargin,
      queryText = "match (at {name: 'Andres'}), (pn {name: 'Peter'}) set at = pn return at, pn",
      optionalResultExplanation = "The Andres node has had all it's properties replaced by the properties in the Peter node.",
      assertions = (p) => {
        assert(node("Andres").getProperty("name") === "Peter")
        assertFalse("Didn't expect the Andres node to have an awesome property", node("Andres").hasProperty("awesome"))
      })
  }

  @Test def set_a_property_using_a_parameter() {
    prepareAndTestQuery(
      title = "Set a property using a parameter",
      text = """
Use a parameter to give the value of a property.
""",
      prepare = setParameters(Map("surname" -> "Taylor")),
      queryText = "match (n {name: 'Andres'}) set n.surname = {surname} return n",
      optionalResultExplanation = "The Andres node has got an surname added.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesSet = 1))
  }

  @Test def set_all_properties_using_a_parameter() {
    prepareAndTestQuery(
      title = "Set all properties using a parameter",
      text = """
This will replace all existing properties on the node with the new set provided by the parameter.
""",
      prepare = setParameters(Map("props" -> Map("name" -> "Andres", "position" -> "Developer"))),
      queryText = "match (n {name: 'Andres'}) set n = {props} return n",
      optionalResultExplanation = "The Andres node has had all it's properties replaced by the properties in the +props+ parameter.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesSet = 4))
  }

  @Test def set_single_label_on_a_node() {
    testQuery(
      title = "Set a label on a node",
      text = "To set a label on a node, use +SET+.",
      queryText = "match (n {name: 'Stefan'}) set n :German return n",
      optionalResultExplanation = "The newly labeled node is returned by the query.",
      assertions = (p) => assert(getLabelsFromNode(p) === List("German")))
  }

  @Test def set_multiple_labels_on_a_node() {
    testQuery(
      title = "Set multiple labels on a node",
      text = "To set multiple labels on a node, use +SET+ and separate the different labels using +:+.",
      queryText = "match (n {name: 'Emil'}) set n :Swedish:Bossman return n",
      optionalResultExplanation = "The newly labeled node is returned by the query.",
      assertions = (p) => assert(getLabelsFromNode(p) === List("Swedish", "Bossman")))
  }

}
