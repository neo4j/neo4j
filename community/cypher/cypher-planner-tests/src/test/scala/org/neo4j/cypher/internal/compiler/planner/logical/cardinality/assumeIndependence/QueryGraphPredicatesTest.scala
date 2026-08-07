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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.QueryGraphPredicates.PredicatesWithDisjunctiveLabelInfos
import org.neo4j.cypher.internal.ir.Predicate

class QueryGraphPredicatesTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  test("orLeavedSubPredicates on empty predicates should result in single entry") {
    QueryGraphPredicates.empty.distributeLabelDisjunctionAsLabelInfo.predicatesWithLabelDisjunctionsDistributed should equal(
      Seq(QueryGraphPredicates.empty)
    )
  }

  test("orLeavedSubPredicates on one disjunction") {
    val predicates =
      Set(
        Predicate(
          Set(v"n"),
          ors(
            hasLabels("n", "A"),
            hasLabels("n", "B")
          )
        )
      )

    QueryGraphPredicates.empty.copy(otherPredicates = predicates)
      .distributeLabelDisjunctionAsLabelInfo.predicatesWithLabelDisjunctionsDistributed.toSet should equal(
      Set(
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"))))
        ),
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"))))
        )
      )
    )
  }

  test("orLeavedSubPredicates on conjunction of two disjunctions") {
    val predicates =
      Set(
        Predicate(
          Set(v"n"),
          ors(
            hasLabels("n", "A"),
            hasLabels("n", "B")
          )
        ),
        Predicate(
          Set(v"n"),
          ors(
            hasLabels("n", "C"),
            hasLabels("n", "D")
          )
        )
      )

    QueryGraphPredicates.empty.copy(otherPredicates = predicates)
      .distributeLabelDisjunctionAsLabelInfo.predicatesWithLabelDisjunctionsDistributed.toSet should equal(
      Set(
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("C")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("C")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("C"))))
        ),
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("C")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("C")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("C"))))
        ),
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("D")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("D")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("A"), labelName("D"))))
        ),
        QueryGraphPredicates.empty.copy(
          localLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("D")))),
          localOnlyLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("D")))),
          allLabelInfo = LabelInfo.from(Seq(v"n" -> Set(labelName("B"), labelName("D"))))
        )
      )
    )
  }

  test("should not distribute overly large disjunctions") {
    val predicates = {
      val labelPredicates =
        for (i <- 0 to QueryGraphPredicates.DISTRIBUTE_LABEL_DISJUNCTION_LIMIT)
          yield hasLabels("n", s"A_$i")

      Set(Predicate(Set(v"n"), ors(labelPredicates: _*)))
    }

    val qgp = QueryGraphPredicates.empty.copy(otherPredicates = predicates)
    qgp.distributeLabelDisjunctionAsLabelInfo shouldEqual PredicatesWithDisjunctiveLabelInfos(qgp, Seq(qgp))
  }
}
