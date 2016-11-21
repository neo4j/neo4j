/*
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.cardinality

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compiler.v3_2.planner.{Predicate, Selections}
import org.neo4j.cypher.internal.compiler.v3_2.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, LabelId, PropertyKeyId, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, IdName, Selectivity}

class ExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Should consider parameter expressions when calculating index selectivity") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("Page", LabelId(0))
    semanticTable.resolvedPropertyKeyNames.put("title", PropertyKeyId(0))

    implicit val selections = Selections(Set(Predicate(Set(IdName("n")), HasLabels(varFor("n"), Seq(LabelName("Page")_))_)))

    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(None)).thenReturn(1000.0)
    when(stats.indexSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity.of(0.1d).get))

    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(In(Property(varFor("n"), PropertyKeyName("title")_)_, Parameter("titles", CTAny)_)_)

    result.factor should equal (0.92 +- 0.01)
  }

  test("Should peek inside sub predicates") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("Page", LabelId(0))

    implicit val selections = Selections(Set(Predicate(Set(IdName("n")), HasLabels(varFor("n"), Seq(LabelName("Page")_))_)))

    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(None)).thenReturn(2000.0)
    when(stats.nodesWithLabelCardinality(Some(LabelId(0)))).thenReturn(1000.0)
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(PartialPredicate[HasLabels](HasLabels(varFor("n"), Seq(LabelName("Page")_))_, mock[HasLabels]))

    result.factor should equal(0.5)
  }

  test("Should look at range predicates that could benefit from using an index") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("Person", LabelId(0))

    val n_is_Person = Predicate(Set(IdName("n")), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop")_)_
    val n_gt_3_and_lt_4 = Predicate(Set(IdName("n")), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3")_)_,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4")_)_
    )))

    implicit val selections = Selections(Set(n_is_Person, n_gt_3_and_lt_4))

    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(None)).thenReturn(2000.0)
    when(stats.nodesWithLabelCardinality(Some(LabelId(0)))).thenReturn(1000.0)
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(n_gt_3_and_lt_4.expr)

    result.factor should equal(0.06)
  }

  test("Should optimize selectivity with respect to prefix length for STARTS WITH predicates") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("A", LabelId(0))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(0))

    implicit val selections = mock[Selections]
    val label = LabelName("A")(InputPosition.NONE)
    val propKey = PropertyKeyName("prop")(InputPosition.NONE)
    when(selections.labelsOnNode(IdName("a"))).thenReturn(Set(label))

    val stats = mock[GraphStatistics]
    when(stats.indexSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity.of(.01).get))
    when(stats.indexPropertyExistsSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity.ONE))
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val prefixes = Map("p"          -> 0.23384596099184043,
                       "p2"         -> 0.2299568541948447,
                       "p33"        -> 0.22801230079634685,
                       "p5555"      -> 0.22606774739784896,
                       "reallylong" -> 0.22429997158103274)

    prefixes.foreach { case (prefix, selectivity) =>
      val actual = calculator(StartsWith(Property(Variable("a") _, propKey) _, StringLiteral(prefix)(InputPosition.NONE)) _)
      assert( actual.factor === selectivity +- selectivity * 0.000000000000001)
    }
  }

  test("Selectivity should never be worse than corresponding existence selectivity") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelIds.put("A", LabelId(0))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(0))

    implicit val selections = mock[Selections]
    val label = LabelName("A")(InputPosition.NONE)
    val propKey = PropertyKeyName("prop")(InputPosition.NONE)
    when(selections.labelsOnNode(IdName("a"))).thenReturn(Set(label))

    val stats = mock[GraphStatistics]
    when(stats.indexSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity.of(0.01).get))
    val existenceSelectivity = .2285
    when(stats.indexPropertyExistsSelectivity(LabelId(0), PropertyKeyId(0))).thenReturn(Some(Selectivity.of(existenceSelectivity).get))
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val prefixes = Map("p"          -> existenceSelectivity,
                       "p2"         -> existenceSelectivity,
                       "p33"        -> 0.22801230079634685,
                       "p5555"      -> 0.22606774739784896,
                       "reallylong" -> 0.22429997158103274)

    prefixes.foreach { case (prefix, selectivity) =>
      val actual = calculator(StartsWith(Property(Variable("a") _, propKey) _, StringLiteral(prefix)(InputPosition.NONE)) _)
      assert( actual.factor === selectivity +- selectivity * 0.000000000000001)
    }
  }

  test("should default to single cardinality for HasLabels with previously unknown label") {
    val stats = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(10))
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)
    implicit val semanticTable = SemanticTable()
    implicit val selections = mock[Selections]

    val expr = HasLabels(null, Seq(LabelName("Foo")(pos)))(pos)
    calculator(expr) should equal(Selectivity(1.0 / 10.0))
  }
}
