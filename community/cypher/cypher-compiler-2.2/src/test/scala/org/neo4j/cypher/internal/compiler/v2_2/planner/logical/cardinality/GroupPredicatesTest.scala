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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{QueryGraphProducer, Selectivity}
import org.neo4j.graphdb.Direction

class GroupPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer {
  val id1 = ident("a")
  val id2 = ident("b")
  val BAR: LabelName = LabelName("BAR") _
  val FOO: LabelName = LabelName("FOO") _
  val BAR2: LabelName = LabelName("BAR2") _

  val lit: Literal = SignedDecimalIntegerLiteral("1") _
  val property: Property = Property(id1, PropertyKeyName("prop") _) _
  val inComparison: Expression = In(property, Collection(Seq(lit)) _) _
  val hasLabelId1: Expression = HasLabels(id1, Seq(BAR)) _
  val hasLabelId1Bis: Expression = HasLabels(id1, Seq(BAR2)) _
  val hasLabelId2: Expression = HasLabels(id2, Seq(FOO)) _
  val pattern = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  val stubbedEstimateSelectivity = (predicate: PredicateCombination) => predicate match {
    case _: PropertyAndLabelPredicate => Selectivity(0.2)
    case RelationshipWithLabels(Some(BAR), _, _, _) => Selectivity(0.35)
    case _: RelationshipWithLabels => Selectivity(0.3)
    case _: SingleExpression => Selectivity(0.8)
  }

  test("empty set of predicates returns empty set of groupings") {
    groupPredicates(stubbedEstimateSelectivity)(Set.empty) should equal(Set.empty)
  }

  test("single property equality is returned in it's own grouping") {
    // MATCH a WHERE a.prop IN ["foo"]
    val predicate = ExpressionPredicate(inComparison)
    groupPredicates(stubbedEstimateSelectivity)(Set(predicate)) should equal(
      Set(
        (SingleExpression(predicate.e), Selectivity(0.8))
      ))
  }

  test("unrelated prop and label predicates are returned in different groupings") {
    // MATCH a, b WHERE a.prop IN ["foo"] AND b:BAR2
    val p1 = ExpressionPredicate(inComparison)
    val p2 = ExpressionPredicate(hasLabelId2)
    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2)) should equal(
      Set(
        (SingleExpression(p1.e), Selectivity(0.8)),
        (SingleExpression(p2.e), Selectivity(0.8))))
  }

  test("related prop and label predicates are returned in one grouping, and each on it's own") {
    // MATCH a WHERE a.prop IN ["foo"] AND a:BAR
    val p1 = ExpressionPredicate(inComparison)
    val p2 = ExpressionPredicate(hasLabelId1)
    val p3 = ExistsPredicate(IdName(id1.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2)) should equal(
      Set(
        (PropertyEqualsAndLabelPredicate(property.propertyKey, 1, BAR, Set(p1, p2, p3)), Selectivity(0.2))
      )
    )
  }

  test("pattern gets passed through alone when it's the only one") {
    // MATCH a-->b
    val p1 = PatternPredicate(pattern)
    val p2 = ExistsPredicate(IdName(id1.name))
    val p3 = ExistsPredicate(IdName(id2.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1)) should equal(
      Set(
        (RelationshipWithLabels(None, p1.p, None, Set(p1, p2, p3)), Selectivity(0.3))
      ))
  }

  test("pattern gets grouped with known labels when they are on the lhs") {
    // MATCH a-->b WHERE a:Bar
    val p1 = PatternPredicate(pattern)
    val p2 = ExpressionPredicate(hasLabelId1)
    val p3 = ExistsPredicate(IdName(id1.name))
    val p4 = ExistsPredicate(IdName(id2.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2)) should equal(
      Set(
        (RelationshipWithLabels(Some(BAR), p1.p, None, Set(p1, p2, p3, p4)), Selectivity(0.35))
      ))
  }

  test("pattern gets grouped with the most selective label if all are on the lhs") {
    // MATCH a-->b WHERE a:Bar AND a:Foo
    val p1 = PatternPredicate(pattern)
    val p2 = ExpressionPredicate(hasLabelId1)
    val p3 = ExpressionPredicate(hasLabelId1Bis)
    val p4 = ExistsPredicate(IdName(id1.name))
    val p5 = ExistsPredicate(IdName(id2.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2, p3)) should equal(
      Set(
        (RelationshipWithLabels(Some(BAR2), pattern, None, Set(p1, p3, p4, p5)), Selectivity(0.3)),
        (SingleExpression(p2.e), Selectivity(0.8))
      ))
  }

  test("pattern gets grouped with known labels when it's on the rhs") {
    // MATCH a-->b WHERE b:Bar
    val p1 = PatternPredicate(pattern)
    val p2 = ExpressionPredicate(hasLabelId2)
    val p3 = ExistsPredicate(IdName(id1.name))
    val p4 = ExistsPredicate(IdName(id2.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2)) should equal(
      Set(
        (RelationshipWithLabels(None, pattern, Some(FOO), Set(p1, p2, p3, p4)), Selectivity(0.3))
      ))
  }

  test("pattern gets grouped with known nodes when it's on both sides") {
    // MATCH a-->b WHERE a:Bar AND b:Foo
    val p1 = PatternPredicate(pattern)
    val p2 = ExpressionPredicate(hasLabelId1)
    val p3 = ExpressionPredicate(hasLabelId2)
    val p4 = ExistsPredicate(IdName(id1.name))
    val p5 = ExistsPredicate(IdName(id2.name))

    groupPredicates(stubbedEstimateSelectivity)(Set(p1, p2, p3)) should equal(
      Set(
        (RelationshipWithLabels(Some(BAR), pattern, Some(FOO), Set(p1, p2, p3, p4, p5)), Selectivity(0.35))
      ))
  }
}

