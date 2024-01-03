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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.neo4j.graphdb.schema.IndexType

class NodeIndexScanLeafPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val n = v"n"
  private val dependency = v"dep"
  private val lit12 = literalInt(12)
  private val litApa = literalString("Apa")
  private val litBepa = literalString("Bepa")
  private val hasLabelAwesome = super.hasLabels("n", "Awesome")
  private val dependencyProp = prop(dependency, "prop")
  private val propIsNotNull = isNotNull(prop("n", "prop"))
  private val propStartsWithEmpty = startsWith(prop("n", "prop"), literalString(""))
  private val propLessThan12 = lessThan(prop("n", "prop"), lit12)
  private val propNotEquals12 = not(equals(prop("n", "prop"), lit12))
  private val propEquals12 = equals(prop("n", "prop"), lit12)
  private val propRegexMatchJohnny = regex(prop("n", "prop"), literalString("Johnny"))
  private val propContainsApa = contains(prop("n", "prop"), litApa)
  private val propContainsDependencyProp = contains(prop("n", "prop"), dependencyProp)
  private val propContainsBepa = contains(prop("n", "prop"), litBepa)
  private val propEndsWithApa = endsWith(prop("n", "prop"), litApa)
  private val propEndsWithDependencyProp = endsWith(prop("n", "prop"), dependencyProp)
  private val propEndsWithBepa = endsWith(prop("n", "prop"), litBepa)

  private val fooIsNotNull = isNotNull(prop("n", "foo"))
  private val fooContainsApa = contains(prop("n", "foo"), litApa)
  private val fooEndsWithApa = endsWith(prop("n", "foo"), litApa)
  private val barIsNotNull = isNotNull(prop("n", "bar"))
  private val bazIsNotNull = isNotNull(prop("n", "baz"))
  private val bazEquals12 = equals(prop("n", "baz"), lit12)

  private val awesomeLabelToken = LabelToken("Awesome", LabelId(0))
  private val propPropertyKeyToken = PropertyKeyToken("prop", PropertyKeyId(0))

  private def nodeIndexScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    NodeIndexLeafPlanner(Seq(nodeIndexScanPlanProvider), restrictions)

  private def nodeIndexSearchStringScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    NodeIndexLeafPlanner(Seq(nodeIndexStringSearchScanPlanProvider), restrictions)

  private def allNodeScansLeafPlanner(restrictions: LeafPlanRestrictions) =
    NodeIndexLeafPlanner(Seq(nodeIndexScanPlanProvider, nodeIndexStringSearchScanPlanProvider), restrictions)

  test("does not plan index scan when no index exist") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("no index scan when there is an index on the property but node variable is restricted") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(n, Set(dependency)))(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should be(empty)
    }
  }

  test("index contains scan when there is an index on the property but node variable is restricted") {
    new givenConfig {
      qg = queryGraph(propContainsDependencyProp, hasLabelAwesome)
      textIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(n, Set(dependency)))(
          cfg.qg,
          InterestingOrderConfig.empty,
          ctx
        )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .nodeIndexOperator(
            v"$n:Awesome(prop CONTAINS ???)".name,
            paramExpr = Seq(dependencyProp),
            argumentIds = Set(dependency.name),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
    }
  }

  test("index ends with scan when there is an index on the property but node variable is restricted") {
    new givenConfig {
      qg = queryGraph(propEndsWithDependencyProp, hasLabelAwesome)
      textIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(n, Set(dependency)))(
          cfg.qg,
          InterestingOrderConfig.empty,
          ctx
        )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .nodeIndexOperator(
            v"$n:Awesome(prop ENDS WITH ???)".name,
            paramExpr = Seq(dependencyProp),
            argumentIds = Set(dependency.name),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
    }
  }

  test("index scan solves is not null") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) =>
          solvedPredicates(plan, ctx) should equal(Set(
            propIsNotNull,
            hasLabelAwesome
          ))
      }
    }
  }

  test("index scan for equality solves is not null") {
    new givenConfig {
      qg = queryGraph(propEquals12, propIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) =>
          solvedPredicates(plan, ctx) should equal(Set(
            propIsNotNull,
            hasLabelAwesome
          ))
      }
    }
  }

  private def solvedPredicates(plan: LogicalPlan, ctx: LogicalPlanningContext) =
    ctx.staticComponents.planningAttributes.solveds.get(
      plan.id
    ).asSinglePlannerQuery.queryGraph.selections.predicates.map(_.expr)

  test("index scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("unique index scan when there is an unique index on the property") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("unique index scan with values when there is an unique index on the property") {
    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome)

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("plans unique index scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("index scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(propIsNotNull, hasLabelAwesome).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldBe empty
    }
  }

  test("plans index scans for: n.prop STARTS WITH <pattern>") {
    new givenConfig {
      qg = queryGraph(propStartsWithEmpty, hasLabelAwesome)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propStartsWithEmpty),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans with value for: n.prop STARTS WITH <pattern>") {
    new givenConfig {
      qg = queryGraph(propStartsWithEmpty, hasLabelAwesome)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propStartsWithEmpty),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans for: n.prop < <value>") {
    new givenConfig {
      qg = queryGraph(propLessThan12, hasLabelAwesome)
      addTypeToSemanticTable(lit12, CTInteger.invariant)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propLessThan12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop < <value>") {
    new givenConfig {
      qg = queryGraph(propLessThan12, hasLabelAwesome)
      addTypeToSemanticTable(lit12, CTInteger.invariant)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propLessThan12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans for: n.prop <> <value>") {
    new givenConfig {
      qg = queryGraph(propNotEquals12, hasLabelAwesome)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propNotEquals12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop <> <value>") {
    new givenConfig {
      qg = queryGraph(propNotEquals12, hasLabelAwesome)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propNotEquals12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <value>") {
    new givenConfig {
      qg = queryGraph(propEquals12, hasLabelAwesome)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propEquals12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <value>") {
    new givenConfig {
      qg = queryGraph(propEquals12, hasLabelAwesome)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propEquals12),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans for: n.prop = <pattern>") {
    new givenConfig {
      qg = queryGraph(propRegexMatchJohnny, hasLabelAwesome)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propRegexMatchJohnny),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans index scans with values for: n.prop = <pattern>") {
    new givenConfig {
      qg = queryGraph(propRegexMatchJohnny, hasLabelAwesome)
      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(
            LogicalVariable(n),
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _
          )) =>
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propRegexMatchJohnny),
                hasLabels("n", "Awesome")
              ))
          }
      }
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates") {
    new givenConfig {
      qg = queryGraph(propContainsApa, fooContainsApa, barIsNotNull, bazEquals12, hasLabelAwesome)

      indexOn("Awesome", "foo", "prop", "bar", "baz").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(LogicalVariable(n), _, properties, _, _, _)) =>
          properties should beLike {
            case Seq(
                IndexedProperty(PropertyKeyToken("foo", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("prop", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("bar", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("baz", _), CanGetValue, NODE_TYPE)
              ) => ()
          }
          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propContainsApa),
                PartialPredicate(fooIsNotNull, fooContainsApa),
                barIsNotNull,
                PartialPredicate(bazIsNotNull, bazEquals12),
                hasLabelAwesome
              ))
          }
      }
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates on the same property") {
    new givenConfig {
      qg = queryGraph(propContainsApa, propLessThan12, fooIsNotNull, barIsNotNull, bazEquals12, hasLabelAwesome)
      addTypeToSemanticTable(lit12, CTInteger.invariant)
      indexOn("Awesome", "foo", "prop", "bar", "baz").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(plan @ NodeIndexScan(LogicalVariable(n), _, properties, _, _, _)) =>
          properties should beLike {
            case Seq(
                IndexedProperty(PropertyKeyToken("foo", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("prop", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("bar", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("baz", _), CanGetValue, NODE_TYPE)
              ) => ()
          }

          ctx.staticComponents.planningAttributes.solveds.get(plan.id) should beLike {
            case RegularSinglePlannerQuery(scanQG, _, _, _, _) =>
              // Two combinations of predicates lead to identical plans, that semantically solve the same thing
              scanQG.selections.predicates.map(_.expr) should equal(Set(
                PartialPredicate(propIsNotNull, propContainsApa),
                PartialPredicate(propIsNotNull, propLessThan12),
                fooIsNotNull,
                barIsNotNull,
                PartialPredicate(bazIsNotNull, bazEquals12),
                hasLabelAwesome
              ))
          }
      }
    }
  }

  test("plans no composite index scans when there is a composite index but not enough predicates") {
    new givenConfig {
      qg = queryGraph(propContainsApa, fooContainsApa, barIsNotNull, hasLabelAwesome)

      indexOn("Awesome", "foo", "prop", "bar", "baz").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor() => ()
      }
    }
  }

  test("does not plan index contains scan when no index exist") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("index contains scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, DoNotGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("index contains scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, CanGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("multiple index contains scans with values when there is an index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(propContainsApa, propContainsBepa, hasLabelAwesome)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldEqual Set(
        NodeIndexContainsScan(
          n,
          awesomeLabelToken,
          IndexedProperty(propPropertyKeyToken, CanGetValue, NODE_TYPE),
          `litApa`,
          Set(dependency),
          IndexOrderNone,
          IndexType.RANGE
        ),
        NodeIndexContainsScan(
          n,
          awesomeLabelToken,
          IndexedProperty(propPropertyKeyToken, CanGetValue, NODE_TYPE),
          `litBepa`,
          Set(dependency),
          IndexOrderNone,
          IndexType.RANGE
        )
      )
    }
  }

  test("no index contains scans with values when there is a composite index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(propContainsApa, fooContainsApa, hasLabelAwesome)

      indexOn("Awesome", "prop", "foo").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("unique index contains scan when there is an unique index on the property") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, DoNotGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("unique index contains scan with values when there is an unique index on the property") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, CanGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("plans index contains scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(LogicalVariable(n), _, _, `litApa`, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("index contains scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(LogicalVariable(n), _, _, `litApa`, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldBe empty
    }
  }

  test("plans unique index contains scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexContainsScan(LogicalVariable(n), _, _, `litApa`, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("index ends with scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propEndsWithApa, hasLabelAwesome)

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexEndsWithScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, DoNotGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("index ends with scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(propEndsWithApa, hasLabelAwesome)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexEndsWithScan(
            LogicalVariable(n),
            _,
            IndexedProperty(_, CanGetValue, NODE_TYPE),
            `litApa`,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("multiple index ends with scans with values when there is an index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(propEndsWithApa, propEndsWithBepa, hasLabelAwesome)

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldEqual Set(
        NodeIndexEndsWithScan(
          n,
          awesomeLabelToken,
          IndexedProperty(propPropertyKeyToken, CanGetValue, NODE_TYPE),
          `litApa`,
          Set(dependency),
          IndexOrderNone,
          IndexType.RANGE
        ),
        NodeIndexEndsWithScan(
          n,
          awesomeLabelToken,
          IndexedProperty(propPropertyKeyToken, CanGetValue, NODE_TYPE),
          `litBepa`,
          Set(dependency),
          IndexOrderNone,
          IndexType.RANGE
        )
      )
    }
  }

  test("no index ends with scans with values when there is a composite index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(propEndsWithApa, fooEndsWithApa, hasLabelAwesome)

      indexOn("Awesome", "prop", "foo").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor() => ()
      }
    }
  }

  test("plans index ends with scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      qg = queryGraph(propEndsWithApa, hasLabelAwesome).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(p @ NodeIndexEndsWithScan(LogicalVariable(n), _, _, `litApa`, _, _, _)) =>
          val plannedQG = ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
          plannedQG.hints shouldEqual Set(hint)
      }
    }
  }

  test("index ends with scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(varFor("n"), labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(propEndsWithApa, hasLabelAwesome).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should beLike {
        case SetExtractor(p @ NodeIndexEndsWithScan(LogicalVariable(n), _, _, `litApa`, _, _, _)) =>
          val plannedQG = ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
          plannedQG.hints shouldEqual Set()
      }
    }
  }

  test("does not plan index scans for arguments for: n.prop = <value>") {
    new givenConfig {
      qg = queryGraph(propEquals12, hasLabelAwesome)
        .withArgumentIds(Set(n))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        allNodeScansLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index contains scan for arguments") {
    new givenConfig {
      qg = queryGraph(propContainsApa, hasLabelAwesome)
        .withArgumentIds(Set(n))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        allNodeScansLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index ends with scan for arguments") {
    new givenConfig {
      qg = queryGraph(propEndsWithApa, hasLabelAwesome)
        .withArgumentIds(Set(n))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        allNodeScansLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index scan on argument for (n:Label) with existence constraint") {
    new givenConfig {
      qg = queryGraph(hasLabelAwesome)
        .withArgumentIds(Set(n))

      indexOn("Awesome", "prop")
      nodePropertyExistenceConstraintOn("Awesome", Set("prop"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        allNodeScansLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index scan on argument for (n:Label) with aggregation") {
    new givenConfig {
      qg = queryGraph(hasLabelAwesome)
        .withArgumentIds(Set(n))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val ctxWithAggregation =
        ctx.withModifiedPlannerState(_.withAggregationProperties(Set(PropertyAccess(n, "prop"))))
      val resultPlans = allNodeScansLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctxWithAggregation
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test(
    "does plan scans when seeks are found for single property indexes, when used together with nodeIndexSeekPlanProvider"
  ) {
    new givenConfig {
      indexOn("Awesome", "prop")
      addTypeToSemanticTable(lit12, CTInteger.invariant)

      qg = queryGraph(propLessThan12, hasLabelAwesome)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        nodeIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexScan(LogicalVariable(n), _, _, _, _, _)) => ()
      }
    }
  }

  test("does not plan EndsWithScan for composite indexes") {
    new givenConfig {
      indexOn("Awesome", "prop", "foo")

      qg = queryGraph(propEndsWithApa, fooEndsWithApa, hasLabelAwesome)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = nodeIndexSearchStringScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  private def queryGraph(predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(n), _)).toSet),
      patternNodes = Set(n),
      argumentIds = Set(dependency)
    )
}
