package org.neo4j.cypher.docgen.cookbook

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.junit.Ignore


class MultirelationalSocialNetworkTest extends DocumentingTestBase {
  def graphDescription = List("Joe FOLLOWS Sara", 
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

  @Test def followBack() {
    testQuery(
      title = "Who FOLLOWS or LOVES me back",
      text = """This example shows a multi-relational
     * network between persons and things they like.
     * A multi-relational graph is a graph with more than
     * one kind of relationship between nodes.""",
      queryText = "START me=node:node_auto_index(name = 'Joe') " +
                "MATCH me-[r1]->other-[r2]->me WHERE type(r1)=type(r2) AND type(r1) =~ /FOLLOWS|LOVES/ RETURN other.name, type(r1)",
      returns = "People that +FOLLOWS+ or +LOVES+ +Joe+ back.",
      (p) => assertEquals(List(Map("other.name" -> "Sara", "TYPE(r1)" -> "FOLLOWS"),
          Map("other.name" -> "Maria", "TYPE(r1)" -> "FOLLOWS"),
          Map("other.name" -> "Maria", "TYPE(r1)" -> "LOVES")),p.toList))
  } 
}