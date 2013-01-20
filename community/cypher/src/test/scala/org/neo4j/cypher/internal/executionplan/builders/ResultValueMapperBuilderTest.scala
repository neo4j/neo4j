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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.commands.{AllIdentifiers, ReturnItem}
import org.neo4j.cypher.internal.commands.values.LabelName
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.pipes.FakePipe
import org.neo4j.cypher.internal.symbols.{StringType, LabelType}

class ResultValueMapperBuilderTest extends BuilderTest {

  val builder = new ResultValueMapperBuilder

  @Test
  def offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(LabelName("foo")), ":foo"))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def does_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(LabelName("foo")), ":foo"))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def no_labeltype_no_offer() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), ":foo"))))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def offers_even_when_using_allidentifiers() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(AllIdentifiers())))

    val fake = new FakePipe(Seq(), "x" -> LabelType())

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(fake, q)))
  }

  @Test
  def not_offers_even_when_using_allidentifiers_and_no_matching_type_is_used() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(AllIdentifiers())))

    val fake = new FakePipe(Seq(), "x" -> StringType())

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(fake, q)))
  }

}