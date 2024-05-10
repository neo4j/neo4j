/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ChainerTest extends CypherFunSuite {

  private val BB = new Transformer[BaseContext, BaseState, BaseState] {
    override def transform(from: BaseState, context: BaseContext): BaseState = from
    override def name: String = "BB"
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }

  private val BL = new Transformer[BaseContext, BaseState, LogicalPlanState] {
    override def transform(from: BaseState, context: BaseContext): LogicalPlanState = LogicalPlanState(from)
    override def name: String = "BL"
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }

  private val LL = new Transformer[BaseContext, LogicalPlanState, LogicalPlanState] {
    override def transform(from: LogicalPlanState, context: BaseContext): LogicalPlanState = from
    override def name: String = "LL"
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }

  private def mockContext = {
    val m = mock[BaseContext]
    when(m.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    m
  }

  test("legal chain") {
    val init = InitialState(
      "Q",
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )
    val r = Chainer
      .chainTransformers(Seq(BB, BL, LL))
      .asInstanceOf[Transformer[BaseContext, BaseState, LogicalPlanState]]
      .transform(init, mockContext)
    r.queryText should be("Q")
  }

  test("illegal chain") {
    val init = InitialState(
      "Q",
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )
    a[ClassCastException] should be thrownBy {
      Chainer
        .chainTransformers(Seq(BB, LL, BL))
        .asInstanceOf[Transformer[BaseContext, BaseState, LogicalPlanState]]
        .transform(init, mockContext)
    }
  }
}
