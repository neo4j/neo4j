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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{MergeAst, expressions}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.PropertySetAction
import org.neo4j.cypher.internal.compiler.v2_3.{AbstractPattern, On, OnAction, ParsedEntity}
import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.parser.{Expressions, ParserTest, Query}
import org.parboiled.scala._

class MergeTest extends ParserTest[ast.Merge, MergeAst] with Query with Expressions {
  implicit val parserToTest = Merge ~ EOI

  test("tests") {
    val node = "nodeName"
    val A = "a"
    val B = "b"
    val labelName = KeyToken.Unresolved("Label", TokenType.Label)
    def setProperty(id: String) = PropertySetAction(
      expressions.Property(expressions.Identifier(id), PropertyKey("property")), expressions.TimestampFunction())

    parsing("MERGE (nodeName)") shouldGiveMergeAst
      Seq(ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq.empty))

    parsing("MERGE (nodeName {prop:42})") shouldGiveMergeAst
      Seq(ParsedEntity(node, expressions.Identifier(node), Map("prop" -> expressions.Literal(42)), Seq.empty))

    parsing("MERGE ({prop:42})") shouldGiveMergeAst
      Seq(ParsedEntity("  UNNAMED6", expressions.Identifier("  UNNAMED6"), Map("prop" -> expressions.Literal(42)), Seq.empty))

    parsing("MERGE (nodeName:Label)") shouldGiveMergeAst
      Seq(ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName)))

    parsing("MERGE (nodeName:Label) ON CREATE SET nodeName.property = timestamp()") shouldGiveMergeAst
      (Seq(
        ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName))),
        Seq(OnAction(On.Create, Seq(setProperty(node)))))

    parsing("MERGE (nodeName:Label) ON MATCH SET nodeName.property = timestamp()") shouldGiveMergeAst
      (Seq(
        ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName))),
        Seq(OnAction(On.Match, Seq(setProperty(node)))))

    parsing(
      """MERGE (a:Label)
ON MATCH SET a.property = timestamp()
ON CREATE SET a.property = timestamp()""") shouldGiveMergeAst
      (Seq(
        ParsedEntity(A, expressions.Identifier(A), Map.empty, Seq(labelName))),
        Seq(
          OnAction(On.Match, Seq(setProperty(A))),
          OnAction(On.Create, Seq(setProperty(A)))
        ))
  }

  def convert(astNode: ast.Merge): MergeAst = astNode.toCommand

  implicit class RichResultCheck(inner: ResultCheck) {
    def shouldGiveMergeAst(patterns: Seq[AbstractPattern] = Seq.empty,
                           onActions: Seq[OnAction] = Seq.empty) = {
      val resultParams = inner.actuals.flatMap(_.patterns)
      assert(resultParams.toList === patterns.toList)
      assert(onActions.toList === inner.actuals.flatMap(_.onActions))
    }
  }

}
