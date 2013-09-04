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
package org.neo4j.cypher.internal.parser.legacy

import org.junit.Test
import org.neo4j.cypher.internal.commands._
import org.neo4j.graphdb.Direction
import expressions.{Property, Identifier}
import org.neo4j.cypher.internal.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.commands.PatternPredicate
import org.neo4j.cypher.internal.commands.values.TokenType.PropertyKey

class PredicatesTest extends Predicates with MatchClause with ParserTest with Expressions {

  @Test def pattern_predicates() {
    implicit val parserToTest = patternPredicate

    parsing("a-->(:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED5", Seq(UnresolvedLabel("Foo"))), "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)))

    parsing("a-->(n:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo(SingleNode("a"), SingleNode("n", Seq(UnresolvedLabel("Foo"))), "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)))

    parsing("a-->(:Bar:Foo)") shouldGive
      PatternPredicate(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED5", Seq(UnresolvedLabel("Bar"), UnresolvedLabel("Foo"))), "  UNNAMED1", Seq.empty, Direction.OUTGOING, false)))

    val patterns = Seq(
      RelatedTo(SingleNode("a"), SingleNode("  UNNAMED5", Seq(UnresolvedLabel("First"))), "  UNNAMED1", Seq.empty, Direction.OUTGOING, false),
      RelatedTo(SingleNode("  UNNAMED5", Seq(UnresolvedLabel("First"))), SingleNode("  UNNAMED16", Seq(UnresolvedLabel("Second"))), "  UNNAMED12", Seq.empty, Direction.OUTGOING, false))

    parsing("a-->(:First)-->(:Second)") shouldGive
      PatternPredicate(patterns)
  }

  @Test
  def test_predicates_with_true_false() {
    implicit val parserToTest = predicate

    parsing("true") shouldGive
      True()

    parsing("node.prop = true") shouldGive
      Equals(Property(Identifier("node"), PropertyKey("prop")), True())

    parsing("true = node.prop") shouldGive
      Equals(True(), Property(Identifier("node"), PropertyKey("prop")))

    parsing("true = true") shouldGive
      Equals(True(), True())
  }
}
