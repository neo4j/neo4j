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
import org.neo4j.cypher.internal.commands.expressions
import org.neo4j.cypher.internal.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.commands.MergeAst
import org.neo4j.cypher.internal.parser.experimental.ast
import org.neo4j.cypher.internal.parser.experimental.rules.{Expressions, Query}
import org.neo4j.cypher.internal.mutation.PropertySetAction


class MergeTest extends ParserExperimentalTest[ast.Merge, MergeAst] with Query with Expressions {

  @Test def tests() {
    implicit val parserToTest = Merge
    val node = "nodeName"
    val nodeOther = "fooName"
    val A = "a"
    val B = "b"
    val NO_PATHS = Seq.empty
    val labelName = KeyToken.Unresolved("Label", TokenType.Label)
    val labelOther = KeyToken.Unresolved("Other", TokenType.Label)
    def setProperty(id: String) = PropertySetAction(
      expressions.Property(expressions.Identifier(id), "property"), expressions.TimestampFunction())

    parsing("MERGE (nodeName)") shouldGive
      MergeAst(Seq(
        ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq.empty, bare = true)),
        Seq.empty)

    parsing("MERGE (nodeName {prop:42})") shouldGive
        MergeAst(Seq(
          ParsedEntity(node, expressions.Identifier(node), Map("prop" -> expressions.Literal(42)), Seq.empty, bare = false)),
          Seq.empty)

    parsing("MERGE ({prop:42})") shouldGive
        MergeAst(Seq(
          ParsedEntity("  UNNAMED7", expressions.Identifier("  UNNAMED7"), Map("prop" -> expressions.Literal(42)), Seq.empty, bare = true)),
          Seq.empty)

    parsing("MERGE (nodeName:Label)") shouldGive
        MergeAst(Seq(
          ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq.empty)

    parsing("MERGE (nodeName:Label) MERGE (fooName:Other)") shouldGive
      MergeAst(Seq(
        ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName), bare = false),
        ParsedEntity(nodeOther, expressions.Identifier(nodeOther), Map.empty, Seq(labelOther), bare = false)),
        Seq.empty)


    parsing("MERGE (nodeName:Label) ON CREATE nodeName SET nodeName.property = timestamp()") shouldGive
        MergeAst(Seq(
          ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq(OnAction(On.Create, node, Seq(setProperty(node)))))

    parsing("MERGE (nodeName:Label) ON MATCH nodeName SET nodeName.property = timestamp()") shouldGive
        MergeAst(Seq(
          ParsedEntity(node, expressions.Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq(OnAction(On.Match, node, Seq(setProperty(node)))))

    parsing(
      """MERGE (a:Label)
MERGE (b:Label)
ON MATCH a SET a.property = timestamp()
ON CREATE a SET a.property = timestamp()
ON CREATE b SET b.property = timestamp()
ON MATCH b SET b.property = timestamp()""") shouldGive
        MergeAst(Seq(
          ParsedEntity(A, expressions.Identifier(A), Map.empty, Seq(labelName), bare = false),
          ParsedEntity(B, expressions.Identifier(B), Map.empty, Seq(labelName), bare = false)),
          Seq(
            OnAction(On.Match, A, Seq(setProperty(A))),
            OnAction(On.Create, A, Seq(setProperty(A))),
            OnAction(On.Create, B, Seq(setProperty(B))),
            OnAction(On.Match, B, Seq(setProperty(B)))
          ))
  }

  def convert(astNode: ast.Merge): MergeAst = astNode.toCommand
}