/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.mutation

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.commands.Query
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.symbols
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

class UpdateActionEffectfulTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture.parser

  test("correctly computes MergeNodeAction's effects for node property write") {
    val inner = PropertySetAction(Property(Identifier("a"), PropertyKey("x")), Literal(1))
    val given = MergeNodeAction("a", Map.empty, Seq.empty, Seq.empty, Seq(inner), Seq.empty, None)

    given.effects(SymbolTable(Map("a" -> symbols.CTNode))) should equal(Effects(ReadsNodes, WritesNodes, WritesNodeProperty("x")))
  }

  test("correctly computes MergeNodeAction's effects for relationship property write") {
    val inner = PropertySetAction(Property(Identifier("a"), PropertyKey("x")), Literal(1))
    val given = MergeNodeAction("b", Map.empty, Seq.empty, Seq.empty, Seq(inner), Seq.empty, None)

    given.effects(SymbolTable(Map("a" -> symbols.CTRelationship))) should equal(Effects(ReadsNodes, WritesNodes, WritesRelationshipProperty("x")))
  }

  test("correctly computes MergeNodeAction's effects when inside Foreach") {
    val inner = PropertySetAction(Property(Identifier("a"), PropertyKey("x")), Literal(1))
    val merge = MergeNodeAction("a", Map.empty, Seq.empty, Seq.empty, Seq(inner), Seq.empty, None)
    val given = ForeachAction(Literal(Seq.empty), "k", Seq(merge))

    given.effects(SymbolTable(Map("a" -> symbols.CTNode))) should equal(Effects(ReadsNodes, WritesNodes, WritesNodeProperty("x")))
  }

  test("correctly computes CreateNode's effects when inside Foreach") {
    val propertySet = PropertySetAction(Property(Identifier("a"), PropertyKey("x")), Literal(1))
    val create = CreateNode("a", Map.empty, Seq.empty)
    val given = ForeachAction(Literal(Seq.empty), "k", Seq(create, propertySet))

    given.effects(SymbolTable(Map("a" -> symbols.CTNode))) should equal(Effects(WritesNodes, WritesNodeProperty("x")))
  }

  test("MATCH (a) SET a:Foo RETURN a") {
    val statement = parser.parse("MATCH (a) SET a:Foo RETURN a")
    val commandQuery = statement.asQuery
    commandQuery match {
      case query: Query =>
        query.tail.get.updatedCommands match {
          case Seq(setAction) =>
            setAction.effects(SymbolTable(Map("a" -> symbols.CTNode))) should equal(Effects(WritesLabel("Foo")))
        }
    }
  }
}
