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

import v2_0.{MatchClause, Expressions}
import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions.{Identifier, Collection, Literal}
import org.neo4j.cypher.internal.commands.values.LabelName
import org.neo4j.cypher.internal.commands.{And, HasLabel, RelatedTo, PathExpression}
import org.neo4j.graphdb.Direction

class ExpressionsTest extends Expressions with MatchClause with ParserTest {

  @Test def label_literals() {
    implicit val parserToTest = expression

    parsing(":swedish") shouldGive
      Literal(LabelName("swedish"))

    parsing("[:swedish, :argentinian]") shouldGive
      Collection(Literal(LabelName("swedish")), Literal(LabelName("argentinian")))
  }

  @Test def pattern_expressions() {
    implicit val parserToTest = pathExpression

    parsing("a-->(:Foo)") shouldGive
      PathExpression(Seq(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("  UNNAMED1"), Literal(Seq(LabelName("Foo")))))

    parsing("a-->(n:Foo)") shouldGive
      PathExpression(Seq(RelatedTo("a", "n", "  UNNAMED3", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("n"), Literal(Seq(LabelName("Foo")))))

    parsing("a-->(:Bar:Foo)") shouldGive
      PathExpression(Seq(RelatedTo("a", "  UNNAMED6", "  UNNAMED7", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("  UNNAMED6"), Literal(Seq(LabelName("Bar"), LabelName("Foo")))))

    val patterns = Seq(
      RelatedTo("a", "  UNNAMED8", "  UNNAMED10", Seq.empty, Direction.OUTGOING, false),
      RelatedTo("  UNNAMED8", "  UNNAMED9", "  UNNAMED11", Seq.empty, Direction.OUTGOING, false))

    val predicate = And(
      HasLabel(Identifier("  UNNAMED8"), Literal(Seq(LabelName("First")))),
      HasLabel(Identifier("  UNNAMED9"), Literal(Seq(LabelName("Second")))))

    parsing("a-->(:First)-->(:Second)") shouldGive
      PathExpression(patterns, predicate)
  }

  def createProperty(entity: String, propName: String) = ???
}
