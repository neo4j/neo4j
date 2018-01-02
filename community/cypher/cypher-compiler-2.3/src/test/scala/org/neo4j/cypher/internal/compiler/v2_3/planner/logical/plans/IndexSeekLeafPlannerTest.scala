/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.commands.SingleQueryExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.{indexSeekLeafPlanner, uniqueIndexSeekLeafPlanner}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.language.reflectiveCalls

class IndexSeekLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val idName = IdName("n")
  val hasLabels: Expression = HasLabels(ident("n"), Seq(LabelName("Awesome") _)) _
  val property: Expression = Property(ident("n"), PropertyKeyName("prop") _)_
  val lit42: Expression = SignedDecimalIntegerLiteral("42") _
  val lit6: Expression = SignedDecimalIntegerLiteral("6") _

  val inCollectionValue = In(property, Collection(Seq(lit42))_)_

  test("does not plan index seek when no index exist") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index seek when there is a matching unique index") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index seek when no unique index exist") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = uniqueIndexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, _, _, SingleQueryExpression(`lit42`), _)) =>  ()
      }
    }
  }

  test("plans index seeks when identifier exists as an argument") {
    new given {
      // GIVEN 42 as x MATCH a WHERE a.prop IN [x]
      val x = ident("x")
      qg = queryGraph(In(property, Collection(Seq(x)) _) _, hasLabels).addArgumentIds(Seq(IdName("x")))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val x = cfg.x
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, _, _, SingleQueryExpression(`x`), _)) => ()
      }
    }
  }

  test("does not plan an index seek when the RHS expression does not have its dependencies in scope") {
    new given { // MATCH a, x WHERE a.prop IN [x]
       val x = ident("x")
      qg = queryGraph(In(property, Collection(Seq(x))_)_, hasLabels)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(inCollectionValue, hasLabels)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = uniqueIndexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeUniqueIndexSeek(`idName`, _, _, SingleQueryExpression(`lit42`), _)) => ()
      }
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Awesome")_, PropertyKeyName("prop")(pos))_

    new given {
      qg = queryGraph(inCollectionValue, hasLabels).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, _, _, SingleQueryExpression(`lit42`), _)) => ()
      }

      resultPlans.map(_.solved.graph) should beLike {
        case (Seq(plannedQG: QueryGraph)) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Awesome")_, PropertyKeyName("prop")(pos))_

    new given {
      qg = queryGraph(inCollectionValue, hasLabels).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = uniqueIndexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeUniqueIndexSeek(`idName`, _, _, SingleQueryExpression(`lit42`), _)) => ()
      }

      resultPlans.map(_.solved.graph) should beLike {
        case (Seq(plannedQG: QueryGraph)) if plannedQG.hints == Set(hint) => ()
      }
    }

  }

  private def queryGraph(predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(idName), _)).toSet),
      patternNodes = Set(idName)
    )
}
