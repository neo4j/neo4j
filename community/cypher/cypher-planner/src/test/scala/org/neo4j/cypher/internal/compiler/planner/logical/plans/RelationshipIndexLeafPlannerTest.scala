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
import org.neo4j.graphdb.schema.IndexType

class RelationshipIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def relationshipIndexLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(
      Seq(
        RelationshipIndexScanPlanProvider,
        RelationshipIndexSeekPlanProvider,
        RelationshipIndexStringSearchScanPlanProvider
      ),
      restrictions
    )

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
    val nPropDistance = greaterThan(lit42, function(List("point"), "distance", nProp, point))
    val mPropEqualsXProp = Equals(mProp, xProp)(pos)
    val mPropIsNotNull = isNotNull(mProp)
    val oPropIsNotNull = isNotNull(oProp)
    val oFooEqualsLit6 = equals(oFoo, lit6)
    val oBarEqualsLit42 = equals(oBar, lit42)
    val oAaaEqualsLit42 = equals(oAaa, lit42)
    val oBbbLessThan6 = lessThan(oBbb, lit6)
    val oCccLessThan6 = lessThan(oCcc, lit6)
    new givenConfig {
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
        oFooEqualsLit6,
        oBarEqualsLit42,
        oAaaEqualsLit42,
        oBbbLessThan6,
        oCccLessThan6
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternRelationships = Set(
          PatternRelationship(
            v"n",
            (v"n1", v"n2"),
            OUTGOING,
            Seq(RelTypeName("Awesome")(InputPosition.NONE)),
            SimplePatternLength
          ),
          PatternRelationship(
            v"m",
            (v"m1", v"m2"),
            BOTH,
            Seq(RelTypeName("Awesome")(InputPosition.NONE)),
            SimplePatternLength
          ),
          PatternRelationship(
            v"o",
            (v"o1", v"o2"),
            OUTGOING,
            Seq(RelTypeName("Awesome")(InputPosition.NONE)),
            SimplePatternLength
          ),
          PatternRelationship(
            v"x",
            (v"x1", v"x2"),
            OUTGOING,
            Seq(RelTypeName("Awesome")(InputPosition.NONE)),
            SimplePatternLength
          )
        ),
        argumentIds = Set(v"x")
      )

      relationshipIndexOn("Awesome", "prop")
      relationshipTextIndexOn("Awesome", "prop")
      relationshipIndexOn("Awesome", "foo", "bar")
      relationshipIndexOn("Awesome", "aaa", "bbb", "ccc")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor(v"m", Set(v"x"))
      val resultPlans = relationshipIndexLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val relationshipTypeToken = RelationshipTypeToken("Awesome", RelTypeId(0))
      val propToken = PropertyKeyToken("prop", PropertyKeyId(0))
      val fooToken = PropertyKeyToken("foo", PropertyKeyId(1))
      val barToken = PropertyKeyToken("bar", PropertyKeyId(2))
      val aaaToken = PropertyKeyToken("aaa", PropertyKeyId(3))
      val bbbToken = PropertyKeyToken("bbb", PropertyKeyId(4))
      val cccToken = PropertyKeyToken("ccc", PropertyKeyId(5))

      resultPlans shouldEqual Set(
        // nPropInLit6Lit42
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)),
          ManyQueryExpression(listOf(lit6, lit42)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        // nPropLessThanLit6
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        // nPropEqualsLit42
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)),
          SingleQueryExpression(lit42),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        // nPropStartsWithLitFoo
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(litFoo))(pos)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(litFoo))(pos)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.TEXT,
          supportPartitionedScan = false
        ),
        // nPropEqualsXProp
        DirectedRelationshipIndexSeek(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)),
          SingleQueryExpression(xProp),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        // mPropEqualsXProp
        UndirectedRelationshipIndexSeek(
          v"m",
          v"m1",
          v"m2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, CanGetValue, RELATIONSHIP_TYPE)),
          SingleQueryExpression(xProp),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        // oFooEqualsLit6, oBarEqualsLit42
        DirectedRelationshipIndexSeek(
          v"o",
          v"o1",
          v"o2",
          relationshipTypeToken,
          Seq(
            IndexedProperty(fooToken, CanGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(barToken, CanGetValue, RELATIONSHIP_TYPE)
          ),
          CompositeQueryExpression(Seq(SingleQueryExpression(lit6), SingleQueryExpression(lit42))),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        // oAaaEqualsLit42, oBbbLessThan6, oCccLessThan6
        DirectedRelationshipIndexSeek(
          v"o",
          v"o1",
          v"o2",
          relationshipTypeToken,
          Seq(
            IndexedProperty(aaaToken, CanGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(bbbToken, DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(cccToken, DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          CompositeQueryExpression(Seq(
            SingleQueryExpression(lit42),
            RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)),
            ExistenceQueryExpression()
          )),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        // nPropContainsLitFoo
        DirectedRelationshipIndexContainsScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE),
          litFoo,
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        ),
        DirectedRelationshipIndexContainsScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE),
          litFoo,
          Set(v"x"),
          IndexOrderNone,
          IndexType.TEXT
        ),
        // nPropEndsWithLitFoo
        DirectedRelationshipIndexEndsWithScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE),
          litFoo,
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        ),
        DirectedRelationshipIndexEndsWithScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE),
          litFoo,
          Set(v"x"),
          IndexOrderNone,
          IndexType.TEXT
        ),
        // ..several..
        DirectedRelationshipIndexScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        ),
        DirectedRelationshipIndexScan(
          v"n",
          v"n1",
          v"n2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.TEXT
        ),
        // oPropIsNotNull
        DirectedRelationshipIndexScan(
          v"o",
          v"o1",
          v"o2",
          relationshipTypeToken,
          Seq(IndexedProperty(propToken, DoNotGetValue, RELATIONSHIP_TYPE)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        ),
        // oFooEqualsLit6, oBarEqualsLit42,
        DirectedRelationshipIndexScan(
          v"o",
          v"o1",
          v"o2",
          relationshipTypeToken,
          Seq(
            IndexedProperty(fooToken, DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(barToken, DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        ),
        // oAaaEqualsLit42, oBbbLessThan6, oCccLessThan6
        DirectedRelationshipIndexScan(
          v"o",
          v"o1",
          v"o2",
          relationshipTypeToken,
          Seq(
            IndexedProperty(aaaToken, DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(bbbToken, DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(cccToken, DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE
        )
      )
    }
  }
}
