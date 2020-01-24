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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.indexScanLeafPlanner
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.UsingIndexHint
import org.neo4j.cypher.internal.v3_5.expressions.{functions, _}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class IndexScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val idName = "n"
  val hasLabels: Expression = HasLabels(varFor("n"), Seq(LabelName("Awesome") _)) _
  val property: Expression = Property(varFor("n"), PropertyKeyName("prop") _) _

  val existsPredicate: Expression = FunctionInvocation(FunctionName(functions.Exists.name) _, property) _
  val startsWithPredicate: Expression = StartsWith(property, StringLiteral("") _) _
  val ltPredicate: Expression = LessThan(property, SignedDecimalIntegerLiteral("12") _) _
  val neqPredicate: Expression = NotEquals(property, SignedDecimalIntegerLiteral("12") _) _
  val eqPredicate: Expression = Equals(property, SignedDecimalIntegerLiteral("12") _) _
  val regexPredicate: Expression = RegexMatch(property, StringLiteral("Johnny") _) _
  val stringLiteral: Expression = StringLiteral("apa") _
  val containsPredicate: Expression = Contains(property, stringLiteral) _
  val endsWithPredicate: Expression = EndsWith(property, stringLiteral) _

  test("does not plan index scan when no index exist") {
    new given {
      qg = queryGraph(existsPredicate, hasLabels)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabels)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) => ()
      }
    }
  }

  test("index scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabels)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) => ()
      }
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabels)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) => ()
      }
    }
  }

  test("unique index scan with values when there is an unique index on the property") {
    new given {
      qg = queryGraph(existsPredicate, hasLabels)

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) => ()
      }
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome") _, Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(existsPredicate, hasLabels).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, _, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Seq(hint) => ()
      }
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome") _, Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(existsPredicate, hasLabels).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexScan(`idName`, _, _, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Seq(hint) => ()
      }
    }
  }

  test("plans index scans for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(startsWithPredicate, hasLabels)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, startsWithPredicate)))
          }
      }
    }
  }


  test("plans index scans with value for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(startsWithPredicate, hasLabels)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, startsWithPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop < <value>") {
    new given {
      qg = queryGraph(ltPredicate, hasLabels)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, ltPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop < <value>") {
    new given {
      qg = queryGraph(ltPredicate, hasLabels)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, ltPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop <> <value>") {
    new given {
      qg = queryGraph(neqPredicate, hasLabels)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, neqPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop <> <value>") {
    new given {
      qg = queryGraph(neqPredicate, hasLabels)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, neqPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <value>") {
    new given {
      qg = queryGraph(eqPredicate, hasLabels)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, eqPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <value>") {
    new given {
      qg = queryGraph(eqPredicate, hasLabels)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, eqPredicate)))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <pattern>") {
    new given {
      qg = queryGraph(regexPredicate, hasLabels)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, DoNotGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, regexPredicate)))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <pattern>") {
    new given {
      qg = queryGraph(regexPredicate, hasLabels)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(plan@NodeIndexScan(`idName`, _, IndexedProperty(_, CanGetValue), _, _)) =>
          ctx.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularPlannerQuery(scanQG, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(PartialPredicate(existsPredicate, regexPredicate)))
          }
      }
    }
  }

  test("does not plan index contains scan when no index exist") {
    new given {
      qg = queryGraph(containsPredicate, hasLabels)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index contains scan when there is an index on the property") {
    new given {
      qg = queryGraph(containsPredicate, hasLabels)

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
      qg = queryGraph(containsPredicate, hasLabels)

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
      qg = queryGraph(containsPredicate, hasLabels)

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
      qg = queryGraph(containsPredicate, hasLabels)

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
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome") _, Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(containsPredicate, hasLabels).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, _, `stringLiteral`, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Seq(hint) => ()
      }
    }
  }

  test("plans unique index contains scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Awesome") _, Seq(PropertyKeyName("prop")(pos))) _

    new given {
      qg = queryGraph(containsPredicate, hasLabels).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = indexScanLeafPlanner(cfg.qg, InterestingOrder.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(NodeIndexContainsScan(`idName`, _, _, `stringLiteral`, _, _)) => ()
      }

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Seq(hint) => ()
      }
    }
  }

  test("index ends with scan when there is an index on the property") {
    new given {
      qg = queryGraph(endsWithPredicate, hasLabels)

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
      qg = queryGraph(endsWithPredicate, hasLabels)

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
      qg = queryGraph(eqPredicate, hasLabels)
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
      qg = queryGraph(containsPredicate, hasLabels)
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
      qg = queryGraph(endsWithPredicate, hasLabels)
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
