/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_2.commands.{CompositeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.{indexSeekLeafPlanner, uniqueIndexSeekLeafPlanner}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.{IdName, Predicate, QueryGraph, Selections}

import scala.language.reflectiveCalls

class IndexSeekLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val idName = IdName("n")
  val hasLabels: Expression = HasLabels(varFor("n"), Seq(LabelName("Awesome") _)) _
  val property: Expression = Property(varFor("n"), PropertyKeyName("prop") _)_
  val property2: Expression = Property(varFor("n"), PropertyKeyName("prop2") _)_
  val lit42: Expression = SignedDecimalIntegerLiteral("42") _
  val lit6: Expression = SignedDecimalIntegerLiteral("6") _

  val inCollectionValue = In(property, ListLiteral(Seq(lit42))_)_

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

  test("index scan when there is a composite index on two properties") {
    new given {
      val inCollectionValue2 = In(property2, ListLiteral(Seq(lit6))_)_
      qg = queryGraph(inCollectionValue, inCollectionValue2, hasLabels)

      indexOn("Awesome", "prop", "prop2")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, LabelToken("Awesome", _),
        Seq(PropertyKeyToken("prop", _), PropertyKeyToken("prop2", _)),
        CompositeQueryExpression(Seq(`lit42`, `lit6`)), _)) => ()
      }
    }
  }

  test("index scan when there is a composite index on two properties in the presence of other nodes, labels and properties") {
    new given {
      val litFoo: Expression = StringLiteral("foo") _

      // MATCH (n:Awesome:Sauce), (m:Awesome)
      // WHERE n.prop = 42 AND n.prop2 = 6 AND n.prop3 = "foo" AND m.prop = "foo"
      qg = queryGraph(
        // node 'n'
        HasLabels(varFor("n"), Seq(LabelName("Awesome") _)) _,
        HasLabels(varFor("n"), Seq(LabelName("Sauce") _)) _,
        In(Property(varFor("n"), PropertyKeyName("prop") _) _, ListLiteral(Seq(lit42)) _) _,
        In(Property(varFor("n"), PropertyKeyName("prop2") _) _, ListLiteral(Seq(lit6)) _) _,
        In(Property(varFor("n"), PropertyKeyName("prop3") _) _, ListLiteral(Seq(litFoo)) _) _,
        // node 'm'
        HasLabels(varFor("m"), Seq(LabelName("Awesome") _)) _,
        In(Property(varFor("m"), PropertyKeyName("prop") _) _, ListLiteral(Seq(litFoo)) _) _
      )

      // CREATE INDEX ON :Awesome(prop,prop2)
      indexOn("Awesome", "prop", "prop2")

    }.withLogicalPlanningContext { (cfg, ctx) =>

      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, LabelToken("Awesome", _),
        Seq(PropertyKeyToken("prop", _), PropertyKeyToken("prop2", _)),
        CompositeQueryExpression(Seq(`lit42`, `lit6`)), _)) => ()
      }
    }
  }

  test("index scan when there is a composite index on many properties") {
    val propertyNames: Seq[String] = (0 to 10).map(n => s"prop$n")
    val properties: Seq[Expression] = propertyNames.map { n =>
      val prop: Expression = Property(varFor("n"), PropertyKeyName(n) _) _
      prop
    }
    val values: Seq[Expression] = (0 to 10).map { n =>
      val lit: Expression = SignedDecimalIntegerLiteral((n * 10 + 2).toString) _
      lit
    }
    val predicates = properties.zip(values).map{ pair =>
      val predicate = In(pair._1, ListLiteral(Seq(pair._2))_ )_
      Predicate(Set(idName), predicate)
    }

    new given {
      qg = QueryGraph(
        selections = Selections(predicates.toSet + Predicate(Set(idName), hasLabels)),
        patternNodes = Set(idName)
      )

      indexOn("Awesome", propertyNames: _*)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexSeekLeafPlanner(cfg.qg)(ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexSeek(`idName`, LabelToken("Awesome", _),
        props@Seq(_*),
        CompositeQueryExpression(vals@Seq(_*)), _))
          if assertPropsAndValuesMatch(propertyNames, values, props, vals.flatMap(_.expressions)) => ()
      }
    }
  }

  private def assertPropsAndValuesMatch(expectedProps: Seq[String], expectedVals: Seq[Expression], foundProps: Seq[PropertyKeyToken], foundVals: Seq[Expression]) = {
    val expected: Map[String, Expression] = expectedProps.zip(expectedVals).toMap
    val found: Map[String, Expression] = foundProps.map(_.name).zip(foundVals).toMap
    found.equals(expected)
  }

  test("plans index seeks when variable exists as an argument") {
    new given {
      // GIVEN 42 as x MATCH a WHERE a.prop IN [x]
      val x = varFor("x")
      qg = queryGraph(In(property, ListLiteral(Seq(x)) _) _, hasLabels).addArgumentIds(Seq(IdName("x")))

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
       val x = varFor("x")
      qg = queryGraph(In(property, ListLiteral(Seq(x))_)_, hasLabels)

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
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome")_, Seq(PropertyKeyName("prop")(pos)))_

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

      resultPlans.map(_.solved.queryGraph) should beLike {
        case (Seq(plannedQG: QueryGraph)) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome")_, Seq(PropertyKeyName("prop")(pos)))_

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

      resultPlans.map(_.solved.queryGraph) should beLike {
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
