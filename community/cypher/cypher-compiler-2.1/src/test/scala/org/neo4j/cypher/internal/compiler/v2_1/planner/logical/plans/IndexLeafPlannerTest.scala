/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v2_1.commands.ManyQueryExpression

class IndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val idName = IdName("n")
  val hasLabels = HasLabels(ident("n"), Seq(LabelName("Awesome") _)) _
  val property = Property(ident("n"), PropertyKeyName("prop") _)_
  val lit42 = SignedDecimalIntegerLiteral("42") _
  val lit6 = SignedDecimalIntegerLiteral("6") _

  val inCollectionValue = In(property, Collection(Seq(lit42))_)_

  test("does not plan index seek when no index exist") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      withLogicalPlanningContext { (ctx: LogicalPlanningContext, table: Map[PatternExpression, QueryGraph]) =>
        // when
        val resultPlans = indexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans shouldBe empty
      }
    }
  }
  test("does not plan index seek when no unique index exist") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = uniqueIndexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans shouldBe empty
      }
    }
  }

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      indexOn("Awesome", "prop")

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = indexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexSeek(`idName`, _, _, ManyQueryExpression(Collection(Seq(lit42))))) => ()
        }
      }
    }
  }

  test("index scan when there is an index on the property for IN queries") {
    new given {
      qg = queryGraph(In(property, Collection(Seq(lit42))_)_, hasLabels)

      indexOn("Awesome", "prop")

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = indexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexSeek(`idName`, _, _, ManyQueryExpression(Collection(Seq(lit42))))) => ()
        }
      }
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      uniqueIndexOn("Awesome", "prop")

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = uniqueIndexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexUniqueSeek(`idName`, _, _, ManyQueryExpression(Collection(Seq(lit42))))) => ()
        }
      }
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Awesome")_, ident("prop"))_

    new given {
      qg = queryGraph(inCollectionValue, hasLabels).addHints(Some(hint))

      indexOn("Awesome", "prop")

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = indexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexSeek(`idName`, _, _, ManyQueryExpression(Collection(Seq(lit42))))) => ()
        }

        resultPlans.plans.map(_.solved.graph) should beLike {
          case (Seq(plannedQG: QueryGraph)) if plannedQG.hints == Set(hint) => ()
        }
      }
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Awesome")_, ident("prop"))_

    new given {
      qg = queryGraph(inCollectionValue, hasLabels).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")

      withLogicalPlanningContext { (ctx, table) =>
        // when
        val resultPlans = uniqueIndexSeekLeafPlanner(qg)(ctx, table)

        // then
        resultPlans.plans.map(_.plan) should beLike {
          case Seq(NodeIndexUniqueSeek(`idName`, _, _, ManyQueryExpression(Collection(Seq(lit42))))) => ()
        }

        resultPlans.plans.map(_.solved.graph) should beLike {
          case (Seq(plannedQG: QueryGraph)) if plannedQG.hints == Set(hint) => ()
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
