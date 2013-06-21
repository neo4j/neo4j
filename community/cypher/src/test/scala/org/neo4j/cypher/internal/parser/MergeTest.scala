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
import org.neo4j.cypher.internal.commands._
import expressions._
import org.neo4j.cypher.internal.parser.v2_0.{Updates, StartAndCreateClause, MatchClause}
import org.neo4j.cypher.internal.mutation.PropertySetAction
import org.neo4j.cypher.internal.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.commands.values.TokenType.PropertyKey


class MergeTest extends StartAndCreateClause with MatchClause with Updates with ParserTest {
  @Test def tests() {
    implicit val parserToTest = createStart
    val node = "nodeName"
    val A = "a"
    val B = "b"
    val NO_PATHS = Seq.empty
    val labelName = KeyToken.Unresolved("Label", TokenType.Label)
    def setProperty(id: String) = PropertySetAction(Property(Identifier(id), PropertyKey("property")), TimestampFunction())

    parsing("MERGE (nodeName)") shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(node, Identifier(node), Map.empty, Seq.empty, bare = true)),
          Seq.empty)), NO_PATHS)


    parsing("MERGE (nodeName {prop:42})") shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(node, Identifier(node), Map("prop" -> Literal(42)), Seq.empty, bare = false)),
          Seq.empty)), NO_PATHS)


    parsing("MERGE (nodeName:Label)") shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(node, Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq.empty)), NO_PATHS)


    parsing("MERGE (nodeName:Label) ON CREATE nodeName SET nodeName.property = timestamp()") shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(node, Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq(OnAction(On.Create, node, Seq(setProperty(node)))))), NO_PATHS)


    parsing("MERGE (nodeName:Label) ON MATCH nodeName SET nodeName.property = timestamp()") shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(node, Identifier(node), Map.empty, Seq(labelName), bare = false)),
          Seq(OnAction(On.Match, node, Seq(setProperty(node)))))), NO_PATHS)


    parsing(
      """MERGE (a:Label)
MERGE (b:Label)
ON MATCH a SET a.property = timestamp()
ON CREATE a SET a.property = timestamp()
ON CREATE b SET b.property = timestamp()
ON MATCH b SET b.property = timestamp()
      """) shouldGive
      (Seq(
        MergeAst(Seq(
          ParsedEntity(A, Identifier(A), Map.empty, Seq(labelName), bare = false),
          ParsedEntity(B, Identifier(B), Map.empty, Seq(labelName), bare = false)),
          Seq(
            OnAction(On.Match, A, Seq(setProperty(A))),
            OnAction(On.Create, A, Seq(setProperty(A))),
            OnAction(On.Create, B, Seq(setProperty(B))),
            OnAction(On.Match, B, Seq(setProperty(B)))
          ))), NO_PATHS)
  }

  def createProperty(entity: String, propName: String): Expression = Property(Identifier(entity), PropertyKey(propName))
}