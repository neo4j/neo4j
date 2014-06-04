/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.{uniqueIndexSeekLeafPlanner, indexSeekLeafPlanner}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{QueryGraphSolvingContext, Cardinality}
import org.neo4j.cypher.internal.compiler.v2_1.planner.BeLikeMatcher._

class IndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val idName = IdName("n")
  val hasLabels = HasLabels(ident("n"), Seq(LabelName("Awesome") _)) _
  val equalsValue = Equals(
    Property(ident("n"), PropertyKeyName("prop") _) _,
    SignedIntegerLiteral("42") _
  )_

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(equalsValue, hasLabels)

      cardinality = mapCardinality {
        case _: AllNodesScan => 1000
        case _: NodeByLabelScan => 100
        case _ => Double.MaxValue
      }

      indexOn("Awesome", "prop")

      withQueryGraphSolvingContext { (ctx: QueryGraphSolvingContext) =>
        // when
        val resultPlans = indexSeekLeafPlanner(qg)(ctx)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexSeek(`idName`, _, _, SignedIntegerLiteral("42"))) => ()
        }
      }
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(equalsValue, hasLabels)

      cardinality = mapCardinality {
        case _: AllNodesScan => 1000
        case _: NodeByLabelScan => 100
        case _ => Double.MaxValue
      }

      uniqueIndexOn("Awesome", "prop")

      withQueryGraphSolvingContext { (ctx: QueryGraphSolvingContext) =>
        // when
        val resultPlans = uniqueIndexSeekLeafPlanner(qg)(ctx)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexUniqueSeek(`idName`, _, _, SignedIntegerLiteral("42"))) => ()
        }
      }
    }
  }

  private def queryGraph(predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(idName), _)).toSet),
      patternNodes = Set(idName)
    )
}
