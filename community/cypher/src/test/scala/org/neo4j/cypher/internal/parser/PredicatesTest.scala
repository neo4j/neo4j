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
import org.neo4j.graphdb.Direction
import expressions.{Property, Identifier}
import v2_0.{Predicates, MatchClause, Expressions}
import org.neo4j.cypher.internal.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.commands.PatternPredicate
import org.neo4j.cypher.internal.commands.HasLabel

class PredicatesTest extends Predicates with MatchClause with ParserTest with Expressions {

  @Test def pattern_predicates() {
    implicit val parserToTest = patternPredicate

    parsing("a-->(:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "  UNNAMED5", "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("  UNNAMED5"), KeyToken.Unresolved("Foo", TokenType.Label)))

    parsing("a-->(n:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "n", "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)), HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label)))

    parsing("a-->(:Bar:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo("a", "  UNNAMED5", "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)), And(HasLabel(Identifier("  UNNAMED5"), KeyToken.Unresolved("Bar", TokenType.Label)), HasLabel(Identifier("  UNNAMED5"),KeyToken.Unresolved("Foo", TokenType.Label))))

    val patterns = Seq(
      RelatedTo("a", "  UNNAMED5", "  UNNAMED1", Seq.empty, Direction.OUTGOING, false),
      RelatedTo("  UNNAMED5", "  UNNAMED16", "  UNNAMED12", Seq.empty, Direction.OUTGOING, false))

    val predicate = And(
      HasLabel(Identifier("  UNNAMED5"), KeyToken.Unresolved("First", TokenType.Label)),
      HasLabel(Identifier("  UNNAMED16"), KeyToken.Unresolved("Second", TokenType.Label)))

    parsing("a-->(:First)-->(:Second)") shouldGive
      PatternPredicate(patterns, predicate)
  }

  @Test
  def test_predicates_with_true_false() {
    implicit val parserToTest = predicate

    parsing("true") shouldGive
      True()

    parsing("node.prop = true") shouldGive
      Equals(Property(Identifier("node"), "prop"), True())

    parsing("true = node.prop") shouldGive
      Equals(True(), Property(Identifier("node"), "prop"))

    parsing("true = true") shouldGive
      Equals(True(), True())
  }
}