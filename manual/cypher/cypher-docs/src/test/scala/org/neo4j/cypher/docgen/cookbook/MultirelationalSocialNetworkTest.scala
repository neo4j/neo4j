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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}


class MultirelationalSocialNetworkTest extends DocumentingTestBase {
  override def graphDescription = List("Joe FOLLOWS Sara",
            "Sara FOLLOWS Joe",
            "Joe LOVES Maria",
            "Maria LOVES Joe",
            "Joe FOLLOWS Maria",
            "Maria FOLLOWS Joe",
            "Sara FOLLOWS Ben",
            "Joe LIKES bikes",
            "Joe LIKES nature",
            "Sara LIKES bikes",
            "Sara LIKES cars",
            "Sara LIKES cats",
            "Maria LIKES cars")

  def section = "cookbook"
  override val graphvizOptions = "graph [layout=circo]"
  override val noTitle = true;

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  @Test def followBack() {
    testQuery(
      title = "Who FOLLOWS or LOVES me back",
      text = """This example shows a multi-relational network between persons and things they like.
        A multi-relational graph is a graph with more than one kind of relationship between nodes.""",
      queryText = "MATCH (me {name: 'Joe'})-[r1:FOLLOWS|:LOVES]->(other)-[r2]->(me) WHERE type(r1)=type(r2) RETURN other.name, type(r1)",
      optionalResultExplanation = "The query returns people that +FOLLOWS+ or +LOVES+ +Joe+ back.",
      assertions = (p) => assertEquals(Set(Map("other.name" -> "Sara", "type(r1)" -> "FOLLOWS"),
          Map("other.name" -> "Maria", "type(r1)" -> "FOLLOWS"),
          Map("other.name" -> "Maria", "type(r1)" -> "LOVES")),p.toSet))
  }
}
