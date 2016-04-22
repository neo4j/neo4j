/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class SetTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override def graphDescription = List(
    "Andres:Swedish KNOWS Peter",
    "Stefan KNOWS Andres",
    "Emil KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map[String, Any]("age" -> 36l, "hungry" -> true),
    "Peter" -> Map[String, Any]("age" -> 34l))

  def section = "Set"

  @Test def set_property_on_node() {
    testQuery(
      title = "Set a property",
      text = "To set a property on a node or relationship, use `SET`.",
      queryText = "MATCH (n {name: 'Andres'}) SET n.surname = 'Taylor' RETURN n",
      optionalResultExplanation = "The newly changed node is returned by the query.",
      assertions = (p) => assert(node("Andres").getProperty("surname") === "Taylor"))
  }

  @Test def set_property_to_null() {
    testQuery(
      title = "Remove a property",
      text = """Normally you remove a property by using `<<query-remove,REMOVE>>`, but it's sometimes handy to do
it using the `SET` command. One example is if the property comes from a parameter.""",
      queryText = "MATCH (n {name: 'Andres'}) SET n.name = null RETURN n",
      optionalResultExplanation = "The node is returned by the query, and the name property is now missing.",
      assertions = (p) => assertFalse(node("Andres").hasProperty("name")))
  }

  @Test def set_properties_from_other_graph_element() {
    testQuery(
      title = "Copying properties between nodes and relationships",
      text =
        """You can also use `SET` to copy all properties from one graph element to another. Remember that doing this
will remove all other properties on the receiving graph element.""".stripMargin,
      queryText = "MATCH (at {name: 'Andres'}), (pn {name: 'Peter'}) SET at = pn RETURN at, pn",
      optionalResultExplanation = "The 'Andres' node has had all of its properties replaced by the properties in the 'Peter' node.",
      assertions = (p) => {
        assert(node("Andres").getProperty("name") === "Peter")
        assertFalse("Didn't expect the Andres node to have a hungry property", node("Andres").hasProperty("hungry"))
      })
  }

  @Test def inclusive_set_properties_from_map() {
    testQuery(
      title = "Adding properties from maps",
      text =
        """When setting properties from a map (literal, paremeter, or graph element), you can use the `+=` form of `SET`
          |to only add properties, and not remove any of the existing properties on the graph element.
        """.stripMargin,
      queryText = "MATCH (peter {name: 'Peter'}) SET peter += { hungry: true, position: 'Entrepreneur' }",
      optionalResultExplanation = "",
      assertions = (p) => {
        assert(node("Peter").getProperty("name") === "Peter")
        assert(node("Peter").getProperty("hungry") === true)
        assert(node("Peter").getProperty("position") === "Entrepreneur")
      })
  }

  @Test def set_a_property_using_a_parameter() {
    testQuery(
      title = "Set a property using a parameter",
      text = """
Use a parameter to give the value of a property.
""",
      parameters = Map("surname" -> "Taylor"),
      queryText = "MATCH (n {name: 'Andres'}) SET n.surname = {surname} RETURN n",
      optionalResultExplanation = "The Andres node has got a surname added.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesWritten = 1))
  }

  @Test def set_all_properties_using_a_parameter() {
    testQuery(
      title = "Set all properties using a parameter",
      text = """
This will replace all existing properties on the node with the new set provided by the parameter.
""",
      parameters = Map("props" -> Map("name" -> "Andres", "position" -> "Developer")),
      queryText = "MATCH (n {name: 'Andres'}) SET n = {props} RETURN n",
      optionalResultExplanation = "The Andres node has had all of its properties replaced by the properties in the `props` parameter.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesWritten = 4))
  }

  @Test def set_multiple_properties_in_one_set_clause() {
    testQuery(
      title = "Set multiple properties using one SET clause",
      text = "If you want to set multiple properties in one go, simply separate them with a comma.",
      queryText = "MATCH (n {name: 'Andres'}) SET n.position = 'Developer', n.surname = 'Taylor'",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesWritten = 2))
  }

  @Test def set_single_label_on_a_node() {
    testQuery(
      title = "Set a label on a node",
      text = "To set a label on a node, use `SET`.",
      queryText = "MATCH (n {name: 'Stefan'}) SET n :German RETURN n",
      optionalResultExplanation = "The newly labeled node is returned by the query.",
      assertions = (p) => assert(getLabelsFromNode(p) === List("German")))
  }

  @Test def set_multiple_labels_on_a_node() {
    testQuery(
      title = "Set multiple labels on a node",
      text = "To set multiple labels on a node, use `SET` and separate the different labels using +:+.",
      queryText = "MATCH (n {name: 'Emil'}) SET n :Swedish:Bossman RETURN n",
      optionalResultExplanation = "The newly labeled node is returned by the query.",
      assertions = (p) => assert(getLabelsFromNode(p) === List("Swedish", "Bossman")))
  }

}
