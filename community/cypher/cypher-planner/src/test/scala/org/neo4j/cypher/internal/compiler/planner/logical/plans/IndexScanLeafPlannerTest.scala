/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.steps.indexScanLeafPlanner
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast.UsingIndexHint
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, PartialPredicate, PropertyKeyName}
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class IndexScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val idName = "n"
  private val hasLabelsPredicate = super.hasLabels("n", "Awesome")
  private val existsPredicate = function(Exists.name, prop("n", "prop"))
  private val startsWithPredicate = startsWith(prop("n", "prop"), literalString(""))
  private val ltPredicate = propLessThan("n", "prop", 12)
  private val neqPredicate = notEquals(prop("n", "prop"), literalInt(12))
  private val eqPredicate = propEquality("n", "prop", 12)
  private val regexPredicate = regex(prop("n", "prop"), literalString("Johnny"))
  private val stringLiteral = literalString("apa")
  private val containsPredicate = contains(prop("n", "prop"), stringLiteral)
  private val endsWithPredicate = endsWith(prop("n", "prop"), stringLiteral)

  test("does not plan index scan when no index exist") {
    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) => ()
      }
    }
  }

  test("index scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) => ()
      }
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) => ()
      }
    }
  }

  test("unique index scan with values when there is an unique index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate)

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) => ()
      }
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, _, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(existsPredicate, hasLabelsPredicate).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, _, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("plans index scans for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(startsWithPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, startsWithPredicate)))
          }
      }
    }
  }


  test("plans index scans with value for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(startsWithPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, startsWithPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop < <value>") {
    new given {
      qg = queryGraph(ltPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, ltPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop < <value>") {
    new given {
      qg = queryGraph(ltPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, ltPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop <> <value>") {
    new given {
      qg = queryGraph(neqPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, neqPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop <> <value>") {
    new given {
      qg = queryGraph(neqPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, neqPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <value>") {
    new given {
      qg = queryGraph(eqPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, eqPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <value>") {
    new given {
      qg = queryGraph(eqPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, eqPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <pattern>") {
    new given {
      qg = queryGraph(regexPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, DoNotGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, regexPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <pattern>") {
    new given {
      qg = queryGraph(regexPredicate, hasLabelsPredicate)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, Seq(IndexedProperty(_, CanGetValue)), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, regexPredicate)))
          }
      }
    }
  }

  test("does not plan index contains scan when no index exist") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index contains scan when there is an index on the property") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, IndexedProperty(_, DoNotGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("index contains scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, IndexedProperty(_, CanGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("unique index contains scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, IndexedProperty(_, DoNotGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("unique index contains scan with values when there is an unique index on the property") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, IndexedProperty(_, CanGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("plans index contains scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, _, `stringLiteral`, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("plans unique index contains scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, _, `stringLiteral`, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("index ends with scan when there is an index on the property") {
    new given {
      qg = queryGraph(endsWithPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexEndsWithScan(`idName`, _, IndexedProperty(_, DoNotGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("index ends with scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(endsWithPredicate, hasLabelsPredicate)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexEndsWithScan(`idName`, _, IndexedProperty(_, CanGetValue), `stringLiteral`, _, _)) => ()
      }
    }
  }

  test("does not plan index scans for arguments for: n.prop = <value>") {
    new given {
      qg = queryGraph(eqPredicate, hasLabelsPredicate)
        .copy(argumentIds = Set(idName))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index contains scan for arguments") {
    new given {
      qg = queryGraph(containsPredicate, hasLabelsPredicate)
        .copy(argumentIds = Set(idName))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index ends with scan for arguments") {
    new given {
      qg = queryGraph(endsWithPredicate, hasLabelsPredicate)
        .copy(argumentIds = Set(idName))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  private def queryGraph(predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(idName), _)).toSet),
      patternNodes = Set(idName)
    )
}
