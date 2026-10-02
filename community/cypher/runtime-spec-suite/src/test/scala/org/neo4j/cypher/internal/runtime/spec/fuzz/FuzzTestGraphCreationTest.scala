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
package org.neo4j.cypher.internal.runtime.spec.fuzz

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Column
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeSpecSuiteTestSuite
import org.neo4j.cypher.internal.runtime.spec.fuzz.FuzzTestGraphCreation.GraphConfig
import org.neo4j.cypher.internal.runtime.spec.fuzz.FuzzTestGraphCreation.GraphType
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.scalacheck.rng.Seed

class FuzzTestGraphCreationTest extends RuntimeSpecSuiteTestSuite {

  private def emit(plan: ProduceResult): String =
    FuzzTestGraphCreation.generateReproducibleTestCode(
      GraphConfig(GraphType.TwoNodes, seed = 42L),
      plan,
      morselSize = 4,
      parameters = Map.empty[String, Any],
      seed = Seed(42L)
    )

  test("repro for a plan containing an updating plan builds with readOnly = false") {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    val create = Create(AllNodesScan(v"x", Set.empty), Seq(CreateNode(v"m", Set.empty, Set.empty, None)))
    val produce = ProduceResult(create, Seq(Column(v"m", Set.empty)))

    emit(produce) should include(".build(readOnly = false)")
  }

  test("repro for a read-only plan keeps the default build()") {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    val produce = ProduceResult(AllNodesScan(v"x", Set.empty), Seq(Column(v"x", Set.empty)))

    val repro = emit(produce)
    repro should include(".build()")
    (repro should not).include(".build(readOnly = false)")
  }
}
