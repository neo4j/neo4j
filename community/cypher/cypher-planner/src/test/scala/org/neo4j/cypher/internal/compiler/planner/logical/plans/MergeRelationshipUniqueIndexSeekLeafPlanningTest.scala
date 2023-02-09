/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.mergeRelationshipUniqueIndexSeekLeafPlanner
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class MergeRelationshipUniqueIndexSeekLeafPlanningTest extends CypherFunSuite
    with LogicalPlanningTestSupport2
    with AstConstructionTestSupport {
  private val relName = "r"
  private val startNodeName = "start"
  private val endNodeName = "end"
  private val prop = "prop"
  private val prop2 = "prop2"
  private val prop3 = "prop3"
  private val relTypeName = "REL"

  private val rProp = prop(relName, prop)
  private val rProp2 = prop(relName, prop2)
  private val rProp3 = prop(relName, prop3)
  private val lit42 = literalInt(42)
  private val lit6 = literalInt(6)
  private val litFoo = literalString("Foo")

  private val rPropInLit42 = in(rProp, listOf(lit42))
  private val rProp2InLit6 = in(rProp2, listOf(lit6))
  private val rProp3InLitFoo = in(rProp3, listOf(litFoo))

  private def queryGraph(types: Seq[String], semanticDirection: SemanticDirection, predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(relName), _)).toSet),
      patternRelationships = Set(PatternRelationship(
        relName,
        (startNodeName, endNodeName),
        semanticDirection,
        types.map(super[AstConstructionTestSupport].relTypeName(_)),
        SimplePatternLength
      ))
    )

  test("does not plan index seek when planning relationship unique index seek for merger is disabled") {
    new given().withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = false))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does not plan index seek when no index is present") {
    new given {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(Seq(relTypeName), SemanticDirection.OUTGOING, rPropInLit42)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = true))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      // then
      resultPlans shouldBe empty
    }
  }

  test("plans an index seek on a single property") {
    new given {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(Seq(relTypeName), SemanticDirection.OUTGOING, rPropInLit42)
      uniqueRelationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = true))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      // then
      resultPlans shouldEqual Set(DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(IndexedProperty(
          PropertyKeyToken(prop, cfg.semanticTable.resolvedPropertyKeyNames(prop)),
          CanGetValue,
          RELATIONSHIP_TYPE
        )),
        valueExpr = SingleQueryExpression(lit42),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      ))
    }
  }

  test("plans two index seek, with an assert same relationship, when querying two properties with two different indexes") {
    new given {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(Seq(relTypeName), SemanticDirection.OUTGOING, rPropInLit42, rProp2InLit6)
      uniqueRelationshipIndexOn(relTypeName, prop)
      uniqueRelationshipIndexOn(relTypeName, prop2)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = true))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      val lhs = DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(IndexedProperty(
          PropertyKeyToken(prop, cfg.semanticTable.resolvedPropertyKeyNames(prop)),
          CanGetValue,
          RELATIONSHIP_TYPE
        )),
        valueExpr = SingleQueryExpression(lit42),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      )

      val rhs = DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(IndexedProperty(
          PropertyKeyToken(prop2, cfg.semanticTable.resolvedPropertyKeyNames(prop2)),
          CanGetValue,
          RELATIONSHIP_TYPE
        )),
        valueExpr = SingleQueryExpression(lit6),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      )

      // then
      resultPlans shouldEqual Set(AssertSameRelationship(idName = relName, left = lhs, right = rhs))
    }
  }

  test("plans a single index seek when querying two properties with a composite index") {
    new given {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(Seq(relTypeName), SemanticDirection.OUTGOING, rPropInLit42, rProp2InLit6)
      uniqueRelationshipIndexOn(relTypeName, prop, prop2)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = true))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      // then
      resultPlans shouldEqual Set(DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(
          IndexedProperty(
            PropertyKeyToken(prop, cfg.semanticTable.resolvedPropertyKeyNames(prop)),
            CanGetValue,
            RELATIONSHIP_TYPE
          ),
          IndexedProperty(
            PropertyKeyToken(prop2, cfg.semanticTable.resolvedPropertyKeyNames(prop2)),
            CanGetValue,
            RELATIONSHIP_TYPE
          )
        ),
        valueExpr = CompositeQueryExpression(List(SingleQueryExpression(lit42), SingleQueryExpression(lit6))),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      ))
    }
  }

  test("plans a single index seek and a composite one under an assert same relationship") {
    new given {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(Seq(relTypeName), SemanticDirection.OUTGOING, rPropInLit42, rProp2InLit6, rProp3InLitFoo)
      uniqueRelationshipIndexOn(relTypeName, prop, prop2)
      uniqueRelationshipIndexOn(relTypeName, prop3)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val ctx2 = ctx.withModifiedSettings(_.copy(planningMergeRelationshipUniqueIndexSeekEnabled = true))
      // when
      val resultPlans =
        mergeRelationshipUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx2)

      val lhs = DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(
          IndexedProperty(
            PropertyKeyToken(prop, cfg.semanticTable.resolvedPropertyKeyNames(prop)),
            CanGetValue,
            RELATIONSHIP_TYPE
          ),
          IndexedProperty(
            PropertyKeyToken(prop2, cfg.semanticTable.resolvedPropertyKeyNames(prop2)),
            CanGetValue,
            RELATIONSHIP_TYPE
          )
        ),
        valueExpr = CompositeQueryExpression(List(SingleQueryExpression(lit42), SingleQueryExpression(lit6))),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      )

      val rhs = DirectedRelationshipUniqueIndexSeek(
        idName = relName,
        startNode = startNodeName,
        endNode = endNodeName,
        typeToken = RelationshipTypeToken(relTypeName, cfg.semanticTable.resolvedRelTypeNames(relTypeName)),
        properties = Seq(IndexedProperty(
          PropertyKeyToken(prop3, cfg.semanticTable.resolvedPropertyKeyNames(prop3)),
          CanGetValue,
          RELATIONSHIP_TYPE
        )),
        valueExpr = SingleQueryExpression(litFoo),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        indexType = IndexType.RANGE
      )

      // then
      resultPlans shouldEqual Set(AssertSameRelationship(idName = relName, left = lhs, right = rhs))
    }
  }
}
