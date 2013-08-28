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

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.junit.Test
import org.neo4j.cypher.internal.pipes.OptionalsBindingPipe
import org.junit.Assert._

class OptionalsBinderBuilderTest extends BuilderTest {

  def builder: PlanBuilder = new OptionalsBinderBuilder

  @Test
  def should_want_to_insert_optionals_binding_insert_into_plan_without_it() {
    // given
    val query     = PartiallySolvedQuery()
    val queryPlan = plan(query)

    // when
    assertTrue( "should want to insert binding", builder.canWorkWith(queryPlan, null) )
  }

  @Test
  def should_not_want_to_insert_optionals_binding_insert_into_plan_that_has_it() {
    // given
    val query     = PartiallySolvedQuery().copy( bound = true )
    val queryPlan = plan( query )

    // when
    assertFalse( "should not want to insert pipe", builder.canWorkWith(queryPlan, null) )

  }

  @Test
  def should_insert_pipe() {
    // given
    val query     = PartiallySolvedQuery()
    val queryPlan = plan(query)

    // when
    val resultPlan = builder.apply(queryPlan, null)

    // then
    assertTrue( "must insert OptionalsBindingPipe", resultPlan.pipe.isInstanceOf[OptionalsBindingPipe] )
  }
}
