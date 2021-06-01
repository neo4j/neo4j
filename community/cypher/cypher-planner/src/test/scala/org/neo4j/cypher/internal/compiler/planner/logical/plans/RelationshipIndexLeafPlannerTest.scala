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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def relationshipIndexLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(Seq(
      RelationshipIndexScanPlanProvider,
      RelationshipIndexSeekPlanProvider,
      RelationshipIndexStringSearchScanPlanProvider,
    ), restrictions)

  test("finds all types of index plans") {

    val lit42: Expression = literalInt(42)
    val lit6: Expression = literalInt(6)

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val oProp = prop("o", "prop")
    val oFoo = prop("o", "foo")
    val oBar = prop("o", "bar")
    val oAaa = prop("o", "aaa")
    val oBbb = prop("o", "bbb")
    val oCcc = prop("o", "ccc")
    val xProp = prop("x", "prop")

    val nPropEqualsXProp = Equals(nProp, xProp)(pos)
    val nPropEqualsLit42 = equals(lit42, nProp)
    val nPropInLit6Lit42 = in(nProp, listOf(lit6, lit42))
    val nPropLessThanLit6 = lessThan(nProp, lit6)
    val litFoo = literalString("foo")
    val nPropStartsWithLitFoo = startsWith(nProp, litFoo)
    val nPropEndsWithLitFoo = endsWith(nProp, litFoo)
    val nPropContainsLitFoo = contains(nProp, litFoo)
    val point = function("point", mapOfInt("x" -> 1, "y" -> 2))
    val nPropDistance = greaterThan(lit42, function("distance", nProp, point))
    val mPropEqualsXProp = Equals(mProp, xProp)(pos)
    val mPropIsNotNull = isNotNull(mProp)
    val oPropIsNotNull = isNotNull(oProp)
    val oPropExists = exists(oProp)
    val oFooExists = exists(oFoo)
    val oFooEqualsLit6 = equals(oFoo, lit6)
    val oBarEqualsLit42 = equals(oBar, lit42)
    val oAaaEqualsLit42 = equals(oAaa, lit42)
    val oBbbLessThan6 = lessThan(oBbb, lit6)
    val oCccLessThan6 = lessThan(oCcc, lit6)
    new given {
      addTypeToSemanticTable(nProp, CTInteger.invariant)
      addTypeToSemanticTable(mProp, CTInteger.invariant)
      addTypeToSemanticTable(oProp, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      val predicates: Set[Expression] = Set(
        nPropInLit6Lit42,
        nPropLessThanLit6,
        nPropEqualsLit42,
        nPropStartsWithLitFoo,
        nPropEndsWithLitFoo,
        nPropContainsLitFoo,
        nPropDistance,
        nPropEqualsXProp,
        mPropEqualsXProp,
        mPropIsNotNull,
        oPropIsNotNull,
        oPropExists,
        oFooExists,
        oFooEqualsLit6,
        oBarEqualsLit42,
        oAaaEqualsLit42,
        oBbbLessThan6,
        oCccLessThan6,
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternRelationships = Set(
          PatternRelationship("n", ("n1", "n2"), OUTGOING, Seq(RelTypeName("Awesome")(InputPosition.NONE)), SimplePatternLength),
          PatternRelationship("m", ("m1", "m2"), BOTH, Seq(RelTypeName("Awesome")(InputPosition.NONE)), SimplePatternLength),
          PatternRelationship("o", ("o1", "o2"), OUTGOING, Seq(RelTypeName("Awesome")(InputPosition.NONE)), SimplePatternLength),
          PatternRelationship("x", ("x1", "x2"), OUTGOING, Seq(RelTypeName("Awesome")(InputPosition.NONE)), SimplePatternLength)
        ),
        argumentIds = Set("x")
      )

      relationshipIndexOn("Awesome", "prop")
      relationshipIndexOn("Awesome", "foo", "bar")
      relationshipIndexOn("Awesome", "aaa", "bbb", "ccc")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor("m", Set("x"))
      val resultPlans = relationshipIndexLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val relationshipTypeToken = RelationshipTypeToken("Awesome", RelTypeId(0))
      val propToken = PropertyKeyToken("prop", PropertyKeyId(0))
      val fooToken = PropertyKeyToken("foo", PropertyKeyId(1))
      val barToken = PropertyKeyToken("bar", PropertyKeyId(2))
      val aaaToken = PropertyKeyToken("aaa", PropertyKeyId(3))
      val bbbToken = PropertyKeyToken("bbb", PropertyKeyId(4))
      val cccToken = PropertyKeyToken("ccc", PropertyKeyId(5))

      resultPlans.toSet shouldEqual Set(
        // nPropInLit6Lit42
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)), ManyQueryExpression(listOf(lit6, lit42)), Set("x"), IndexOrderNone),
        // nPropLessThanLit6
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)), RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)), Set("x"), IndexOrderNone),
        // nPropEqualsLit42
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)), SingleQueryExpression(lit42), Set("x"), IndexOrderNone),
        // nPropStartsWithLitFoo
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)), RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(litFoo))(pos)), Set("x"), IndexOrderNone),
        // nPropDistance
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)), RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(point, lit42, inclusive = false))(pos)), Set("x"), IndexOrderNone),
        // nPropEqualsXProp
        DirectedRelationshipIndexSeek("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)), SingleQueryExpression(xProp), Set("x"), IndexOrderNone),
        // mPropEqualsXProp
        UndirectedRelationshipIndexSeek("m", "m1", "m2", relationshipTypeToken, Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)), SingleQueryExpression(xProp), Set("x"), IndexOrderNone),
        // oFooEqualsLit6, oBarEqualsLit42
        DirectedRelationshipIndexSeek("o", "o1", "o2", relationshipTypeToken, Seq(IndexedProperty(fooToken, CanGetValue, RELATIONSHIP_TYPE), IndexedProperty(barToken, CanGetValue, RELATIONSHIP_TYPE)), CompositeQueryExpression(Seq(SingleQueryExpression(lit6), SingleQueryExpression(lit42))), Set("x"), IndexOrderNone),
        // oAaaEqualsLit42, oBbbLessThan6, oCccLessThan6
        DirectedRelationshipIndexSeek("o", "o1", "o2", relationshipTypeToken, Seq(IndexedProperty(aaaToken, CanGetValue, RELATIONSHIP_TYPE), IndexedProperty(bbbToken, DoNotGetValue, RELATIONSHIP_TYPE), IndexedProperty(cccToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          CompositeQueryExpression(Seq(SingleQueryExpression(lit42), RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)), ExistenceQueryExpression())), Set("x"), IndexOrderNone),
        // nPropContainsLitFoo
        DirectedRelationshipIndexContainsScan("n", "n1", "n2", relationshipTypeToken, IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE), litFoo, Set("x"), IndexOrderNone),
        // nPropEndsWithLitFoo
        DirectedRelationshipIndexEndsWithScan("n", "n1", "n2", relationshipTypeToken, IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE), litFoo, Set("x"), IndexOrderNone),
        // ..several..
        DirectedRelationshipIndexScan("n", "n1", "n2", relationshipTypeToken, Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)), Set("x"), IndexOrderNone),
        // oPropIsNotNull, oPropExists
        DirectedRelationshipIndexScan("o", "o1", "o2", relationshipTypeToken, Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)), Set("x"), IndexOrderNone),
        // oFooExists, oFooEqualsLit6, oBarEqualsLit42,
        DirectedRelationshipIndexScan("o", "o1", "o2", relationshipTypeToken, Seq(IndexedProperty(fooToken, DoNotGetValue, RELATIONSHIP_TYPE), IndexedProperty(barToken, DoNotGetValue, RELATIONSHIP_TYPE)), Set("x"), IndexOrderNone),
        // oAaaEqualsLit42, oBbbLessThan6, oCccLessThan6
        DirectedRelationshipIndexScan("o", "o1", "o2", relationshipTypeToken, Seq(IndexedProperty(aaaToken, DoNotGetValue, RELATIONSHIP_TYPE), IndexedProperty(bbbToken, DoNotGetValue, RELATIONSHIP_TYPE), IndexedProperty(cccToken, DoNotGetValue, RELATIONSHIP_TYPE)), Set("x"), IndexOrderNone),
      )

      // DirectedRelationshipIndexScan(o, o1, o2, RelationshipTypeToken(Awesome,RelTypeId(0)), List(IndexedProperty(PropertyKeyToken(aaa,PropertyKeyId(3)),DoNotGetValue,RELATIONSHIP_TYPE), IndexedProperty(PropertyKeyToken(bbb,PropertyKeyId(4)),DoNotGetValue,RELATIONSHIP_TYPE), IndexedProperty(PropertyKeyToken(ccc,PropertyKeyId(5)),DoNotGetValue,RELATIONSHIP_TYPE)), Set(x), IndexOrderNone) {},
    }
  }
}
