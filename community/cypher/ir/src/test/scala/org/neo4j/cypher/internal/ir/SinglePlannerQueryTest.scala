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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SinglePlannerQueryTest extends CypherFunSuite with AstConstructionTestSupport {

  test("flattenForeach leaves non-foreach untouched") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        mutatingPatterns = IndexedSeq(
          SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty)
        )
      )
    )

    query.flattenForeach should equal(query)
  }

  test("flattenForeach flattens foreach in head") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        mutatingPatterns = IndexedSeq(
          ForeachPattern(
            varFor("x"),
            listOfInt(1, 2),
            RegularSinglePlannerQuery(
              QueryGraph(
                mutatingPatterns = IndexedSeq(
                  SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty)
                )
              )
            )
          ),
          SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
        )
      )
    )

    query.flattenForeach should equal(
      RegularSinglePlannerQuery(
        QueryGraph(
          mutatingPatterns = IndexedSeq(
            SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty),
            SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
          )
        )
      )
    )
  }

  test("flattenForeach flattens foreach in tail") {
    val query = RegularSinglePlannerQuery(
      tail = Some(RegularSinglePlannerQuery(
        QueryGraph(
          mutatingPatterns = IndexedSeq(
            ForeachPattern(
              varFor("x"),
              listOfInt(1, 2),
              RegularSinglePlannerQuery(
                QueryGraph(
                  mutatingPatterns = IndexedSeq(
                    SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty)
                  )
                )
              )
            ),
            SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
          )
        )
      ))
    )

    query.flattenForeach should equal(
      RegularSinglePlannerQuery(
        tail = Some(RegularSinglePlannerQuery(
          QueryGraph(
            mutatingPatterns = IndexedSeq(
              SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty),
              SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
            )
          )
        ))
      )
    )
  }

  test("flattenForeach flattens nested foreach") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        mutatingPatterns = IndexedSeq(
          ForeachPattern(
            varFor("x"),
            listOfInt(1, 2),
            RegularSinglePlannerQuery(
              QueryGraph(
                mutatingPatterns = IndexedSeq(
                  ForeachPattern(
                    varFor("y"),
                    listOfInt(3, 4),
                    RegularSinglePlannerQuery(
                      QueryGraph(
                        mutatingPatterns = IndexedSeq(
                          SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty)
                        )
                      )
                    )
                  )
                )
              )
            )
          ),
          SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
        )
      )
    )

    query.flattenForeach should equal(
      RegularSinglePlannerQuery(
        QueryGraph(
          mutatingPatterns = IndexedSeq(
            SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty),
            SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
          )
        )
      )
    )
  }

  test("flattenForeach flattens foreach in CALL subquery") {
    val query = RegularSinglePlannerQuery(
      horizon = CallSubqueryHorizon(
        RegularSinglePlannerQuery(
          QueryGraph(
            mutatingPatterns = IndexedSeq(
              ForeachPattern(
                varFor("x"),
                listOfInt(1, 2),
                RegularSinglePlannerQuery(
                  QueryGraph(
                    mutatingPatterns = IndexedSeq(
                      SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty)
                    )
                  )
                )
              ),
              SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
            )
          )
        ),
        correlated = true,
        yielding = true,
        inTransactionsParameters = None
      )
    )

    query.flattenForeach should equal(
      RegularSinglePlannerQuery(
        horizon = CallSubqueryHorizon(
          RegularSinglePlannerQuery(
            QueryGraph(
              mutatingPatterns = IndexedSeq(
                SetLabelPattern(varFor("a"), Seq(LabelName("A")(pos)), Seq.empty),
                SetLabelPattern(varFor("a"), Seq(LabelName("B")(pos)), Seq.empty)
              )
            )
          ),
          correlated = true,
          yielding = true,
          inTransactionsParameters = None
        )
      )
    )
  }
}
