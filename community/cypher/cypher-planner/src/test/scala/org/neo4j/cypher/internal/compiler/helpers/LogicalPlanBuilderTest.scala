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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.neo4j.graphdb.schema.IndexType

class LogicalPlanBuilderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .input") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "r", "v")
      .filter("n:N", "r:R", "v:V")
      .input(nodes = Seq("n"), relationships = Seq("r"), variables = Seq("v"))
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n"), Seq(LabelName("N"))),
              HasTypes(Variable("r"), Seq(RelTypeName("R"))),
              HasLabelsOrTypes(Variable("v"), Seq(LabelOrRelTypeName("V")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("Label filters should produce hasAnyLabel instead of OR as label disjunctions are planned this way") {
    val planWithFilter = new LogicalPlanBuilder()
      .produceResults("anon_3")
      .filter("anon_3:B|C")
      .input(nodes = Seq("anon_3"))
      .build()

    planWithFilter should beLike {
      case ProduceResult(
          Selection(
            Ands(
              SetExtractor(
                HasAnyLabel(
                  Variable("anon_3"),
                  Seq(
                    LabelName("B"),
                    LabelName("C")
                  )
                )
              )
            ),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .allNodeScan") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n:N")
      .allNodeScan("n")
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n"), Seq(LabelName("N")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .expandAll") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n:N", "r:R", "m:M")
      .expandAll("(n)-[r:X]->(m)")
      .allNodeScan("n")
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n"), Seq(LabelName("N"))),
              HasTypes(Variable("r"), Seq(RelTypeName("R"))),
              HasLabels(Variable("m"), Seq(LabelName("M")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .nodeIndexOperator") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n:N")
      .nodeIndexOperator("n:X(prop=1)", indexType = IndexType.RANGE)
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n"), Seq(LabelName("N")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .relationshipIndexOperator") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n:N", "r:R", "m:M")
      .relationshipIndexOperator("(n)-[r:X(prop=1)]->(m)", indexType = IndexType.RANGE)
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n"), Seq(LabelName("N"))),
              HasTypes(Variable("r"), Seq(RelTypeName("R"))),
              HasLabels(Variable("m"), Seq(LabelName("M")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter after .argument") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "r", "v")
      .apply()
      .|.filter("n:N", "r:R", "v:V")
      .|.argument("n, r, v")
      .input(nodes = Seq("n"), relationships = Seq("r"), variables = Seq("v"))
      .build()

    plan should beLike {
      case ProduceResult(
          Apply(
            _,
            Selection(
              Ands(SetExtractor(
                HasLabels(Variable("n"), Seq(LabelName("N"))),
                HasTypes(Variable("r"), Seq(RelTypeName("R"))),
                HasLabelsOrTypes(Variable("v"), Seq(LabelOrRelTypeName("V")))
              )),
              _
            )
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter for aliased vars") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n2:N", "r2:R", "v2:V")
      .projection("n AS n2", "r AS r2", "v AS v2")
      .input(nodes = Seq("n"), relationships = Seq("r"), variables = Seq("v"))
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              HasLabels(Variable("n2"), Seq(LabelName("N"))),
              HasTypes(Variable("r2"), Seq(RelTypeName("R"))),
              HasLabelsOrTypes(Variable("v2"), Seq(LabelOrRelTypeName("V")))
            )),
            _
          ),
          _
        ) =>
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .projection") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n2", "r2", "v2")
      .projection("n:N AS n2", "r:R AS r2", "v:V AS v2")
      .input(nodes = Seq("n"), relationships = Seq("r"), variables = Seq("v"))
      .build()

    plan should beLike {
      case ProduceResult(Projection(_, projections), _) =>
        projections shouldEqual Map(
          v"n2" -> hasLabels("n", "N"),
          v"r2" -> hasTypes("r", "R"),
          v"v2" -> hasLabelsOrTypes("v", "V")
        )
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .projection with shadowing") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "r", "v")
      .projection("n:N AS n", "r:R AS r", "v:V AS v")
      .input(nodes = Seq("n"), relationships = Seq("r"), variables = Seq("v"))
      .build()

    plan should beLike {
      // TODO
      case ProduceResult(Projection(_, projections), _) =>
        projections shouldEqual Map(
          v"n" -> hasLabels("n", "N"),
          v"r" -> hasTypes("r", "R"),
          v"v" -> hasLabelsOrTypes("v", "V")
        )
    }
  }

  test("should correctly insert HasLabels/HasTypes/HasLabelsOrTypes in .filter in nested expression") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n:N OR true")
      .allNodeScan("n")
      .build()

    plan should beLike {
      case ProduceResult(
          Selection(
            Ands(SetExtractor(
              Ors(SetExtractor(HasLabels(Variable("n"), Seq(LabelName("N"))), True()))
            )),
            _
          ),
          _
        ) =>
    }
  }

}
