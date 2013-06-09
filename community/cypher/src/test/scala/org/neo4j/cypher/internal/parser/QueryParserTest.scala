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

import v2_0.QueryParser
import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.mutation.CreateNode
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.expressions.Multiply
import org.neo4j.cypher.internal.commands.CreateNodeStartItem
import org.neo4j.cypher.internal.commands.expressions.Property

class QueryParserTest extends QueryParser with ParserTest {

  @Test
  def shouldParseCreate() {
    implicit val parserToTest = afterWith

    parsing("create (b {age : a.age * 2}) ") shouldGive
      QueryStart.empty.copy(startItems=Seq(
        CreateNodeStartItem(
          CreateNode("b",
            Map("age" -> Multiply(Property(Identifier("a"), "age"), Literal(2.0))), Seq.empty, bare = false))))
  }


  @Test
  def shouldParseAfterWithWithWhere() {
    implicit val parserToTest = afterWith

    parsing("where ID(n) = 1") shouldGive
      QueryStart.empty.copy(predicate=Equals(IdFunction(Identifier("n")), Literal(1)))
  }
}