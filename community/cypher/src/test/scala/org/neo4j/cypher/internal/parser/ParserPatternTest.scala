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
package org.neo4j.cypher.internal.parser

import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.neo4j.graphdb.Direction
import v2_0._
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.commands.values.{KeyToken, TokenType}

class ParserPatternTest extends ParserPattern with ParserTest with Expressions {

  @Test def label_literal_list_parsing() {
    implicit val parserToTest = labelShortForm

    parsing(":FOO") shouldGive
      Seq(KeyToken.Unresolved("FOO", TokenType.Label))

    parsing(":FOO:BAR") shouldGive
      Seq(KeyToken.Unresolved("FOO", TokenType.Label), KeyToken.Unresolved("BAR", TokenType.Label))

    assertFails("[:foo, :bar]")
  }

  @Test def node_forms() {
    implicit val parserToTest = node

    parsing("n") shouldGive
      ParsedEntity("n", Identifier("n"), Map.empty, Seq.empty, true)

    parsing("(n)") shouldGive
      ParsedEntity("n", Identifier("n"), Map.empty, Seq.empty, true)

    parsing("n {name:'Andres'}") shouldGive
      ParsedEntity("n", Identifier("n"), Map("name" -> Literal("Andres")), Seq.empty, false)
  }

  @Test def paths() {
    implicit val parserToTest = path

    parsing("a-[r:TYPE]->b") shouldGive
      List(ParsedRelation("r", Map.empty, bareNode("a"), bareNode("b"), Seq("TYPE"), Direction.OUTGOING, false))

    parsing("a-[r]->b") shouldGive
      List(ParsedRelation("r", Map.empty, bareNode("a"), bareNode("b"), Seq.empty, Direction.OUTGOING, false))
  }


  def bareNode(id: String): ParsedEntity = {
    ParsedEntity(id, Identifier(id), Map.empty, Seq.empty, true)
  }

  def matchTranslator(abstractPattern: AbstractPattern) = ???
}
