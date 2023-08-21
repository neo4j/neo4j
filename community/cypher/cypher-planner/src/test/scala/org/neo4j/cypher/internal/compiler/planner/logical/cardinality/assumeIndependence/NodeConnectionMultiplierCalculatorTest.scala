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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class NodeConnectionMultiplierCalculatorTest extends CypherFunSuite with AstConstructionTestSupport with Matchers {

  implicit private val cardinalityModel: CardinalityModel = new CardinalityModel {

    override def apply(
      query: PlannerQuery,
      labelInfo: LabelInfo,
      relTypeInfo: RelTypeInfo,
      semanticTable: SemanticTable,
      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
      cardinalityModel: CardinalityModel
    ): Cardinality = Cardinality.SINGLE
  }

  implicit private val indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext =
    IndexCompatiblePredicatesProviderContext.default

  test("should return zero if there are no nodes with the given labels") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.EMPTY)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.EMPTY)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.EMPTY)

    val calculator = NodeConnectionMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.nodeConnectionMultiplier(relationship, Map("a" -> Set(labelName("L"))), Selections.empty)

    result should equal(Multiplier.ZERO)
  }

  test("should not consider label selectivity twice") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.SINGLE)

    val calculator = NodeConnectionMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.nodeConnectionMultiplier(relationship, Map("a" -> Set(labelName("L"))), Selections.empty)

    result should equal(Multiplier.ONE)
  }

  test("handles variable length paths over 32 in length") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.SINGLE)

    val calculator = NodeConnectionMultiplierCalculator(stats, IndependenceCombiner)
    val relationship =
      PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(33, Some(33)))

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.nodeConnectionMultiplier(relationship, Map("a" -> Set(labelName("L"))), Selections.empty)

    // one node which has a single relationship to itself. Given the relationship uniqueness, we should get some result between 0 and 1, but not larger than 1
    result should be >= Multiplier.ZERO
    result should be <= Multiplier.ONE
  }

  test("should be able to produce multipliers larger than 1.0") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality(10))
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = NodeConnectionMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val labels = new mutable.HashMap[String, LabelId]()
    for (i <- 1 to 100) labels.put(i.toString, LabelId(i))
    val labelInfo = Map("a" -> labels.keys.map(labelName(_)).toSet)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = labels)
    val result = calculator.nodeConnectionMultiplier(relationship, labelInfo, Selections.empty)

    result should be >= Multiplier.ONE
  }

  test("relationship cardinality if no relationship exist should be equal with/without existing token") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(Cardinality(0))
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality(0))

    val directions = Seq(SemanticDirection.INCOMING, SemanticDirection.OUTGOING, SemanticDirection.BOTH)
    for (direction <- directions) withClue(direction) {
      val minimizedStats = new MinimumGraphStatistics(stats)
      val calculator = NodeConnectionMultiplierCalculator(minimizedStats, IndependenceCombiner)
      val unknownTypeRel = PatternRelationship(
        "r",
        ("a", "b"),
        direction,
        Seq(RelTypeName("UNKNOWN")(pos)),
        SimplePatternLength
      )
      val knownTypeRel = PatternRelationship(
        "r",
        ("a", "b"),
        direction,
        Seq(RelTypeName("KNOWN")(pos)),
        SimplePatternLength
      )

      implicit val semanticTable: SemanticTable =
        new SemanticTable(resolvedRelTypeNames = mutable.Map("KNOWN" -> RelTypeId(0)))
      val unknownRelCardinality = calculator.nodeConnectionMultiplier(unknownTypeRel, Map.empty, Selections.empty)
      val knownRelCardinality = calculator.nodeConnectionMultiplier(knownTypeRel, Map.empty, Selections.empty)

      unknownRelCardinality should equal(knownRelCardinality)
    }
  }

  test("Calculate uniqueness selectivity correctly for QPPs") {
    import org.neo4j.cypher.internal.compiler.planner.logical.CardinalitySupport.MultiplierEquality

    val graphStatistics = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality(200)

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
        labelId match {
          case Some(LabelId(0)) => Cardinality(50)
          case Some(LabelId(1)) => Cardinality(20)
          case _                => sys.error(s"missing node cardinality: $labelId")
        }

      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality =
        (fromLabel, relTypeId, toLabel) match {
          case (None, Some(RelTypeId(0)), None)             => Cardinality(40)
          case (Some(LabelId(0)), Some(RelTypeId(0)), None) => Cardinality(35)
          case (Some(LabelId(1)), Some(RelTypeId(0)), None) => Cardinality(15)
          case _ => sys.error(s"missing rel cardinality: (:$fromLabel)-[:$relTypeId]->(:$toLabel)")
        }

      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
        sys.error(s"missing value selectivity: $index")

      override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] =
        sys.error(s"missing prop not null selectivity: $index")
    }

    val calculator = NodeConnectionMultiplierCalculator(graphStatistics, IndependenceCombiner)

    val planContext = new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(
          inner = graphStatistics,
          snapshot = new MutableGraphStatisticsSnapshot()
        )

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = Iterator.empty

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

      override def getRelationshipPropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty
    }

    val selectivityCalculator = CompositeExpressionSelectivityCalculator(planContext)

    val cardinalityModel = new StatisticsBackedCardinalityModel(
      queryGraphCardinalityModel =
        AssumeIndependenceQueryGraphCardinalityModel(planContext, selectivityCalculator, IndependenceCombiner),
      selectivityCalculator = selectivityCalculator,
      simpleExpressionEvaluator = simpleExpressionEvaluator
    )

    val qpp =
      QuantifiedPathPattern(
        leftBinding = NodeBinding("a_i", "start"),
        rightBinding = NodeBinding("c_i", "end"),
        patternRelationships = List(
          PatternRelationship(
            "r_i",
            ("a_i", "b_i"),
            SemanticDirection.OUTGOING,
            List(RelTypeName("R")(InputPosition.NONE)),
            SimplePatternLength
          ),
          PatternRelationship(
            "s_i",
            ("b_i", "c_i"),
            SemanticDirection.INCOMING,
            List(RelTypeName("R")(InputPosition.NONE)),
            SimplePatternLength
          )
        ),
        selections = Selections.from(differentRelationships("r_i", "s_i")),
        repetition = Repetition.apply(2, UpperBound.Limited(2)),
        nodeVariableGroupings =
          Set(VariableGrouping("a_i", "a"), VariableGrouping("b_i", "b"), VariableGrouping("c_i", "c")),
        relationshipVariableGroupings = Set(VariableGrouping("r_i", "r"), VariableGrouping("s_i", "s"))
      )

    val labels: LabelInfo =
      Map(
        "start" -> Set(LabelName("A")(InputPosition.NONE)),
        "end" -> Set(LabelName("B")(InputPosition.NONE))
      )

    val semanticTable =
      SemanticTable()
        .addRelationship(varFor("r_i"))
        .addRelationship(varFor("s_i"))

    semanticTable.resolvedLabelNames.addAll(Map(
      "A" -> LabelId(0),
      "B" -> LabelId(1)
    ))
    semanticTable.resolvedRelTypeNames.addOne("R" -> RelTypeId(0))

    val multiplier = calculator.nodeConnectionMultiplier(
      pattern = qpp,
      labels = labels,
      selections = Selections.from(unique(add(varFor("r"), varFor("s"))))
    )(
      semanticTable = semanticTable,
      cardinalityModel = cardinalityModel,
      indexPredicateProviderContext = IndexCompatiblePredicatesProviderContext.default
    )

    val uniquenessSelectivity = math.pow(.99, 6)
    multiplier should equal(Multiplier(35 * 40 * 40 * 15 * uniquenessSelectivity / (50 * 20 * math.pow(200, 3))))
  }
}
