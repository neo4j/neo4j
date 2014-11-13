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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.scalatest.mock.MockitoSugar
import org.scalautils.Equality

import scala.Symbol
import scala.collection.mutable

trait QueryGraphProducer extends MockitoSugar {
  self: LogicalPlanningTestSupport =>


  def producePlannerQueryForPattern(query: String): PlannerQuery = {
    val q = query + " RETURN 1"
    val ast = parser.parse(q)
    val semanticChecker = new SemanticChecker(mock[SemanticCheckMonitor])
    val cleanedStatement: Statement = ast.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val semanticState = semanticChecker.check(query, cleanedStatement)

    val firstRewriteStep = astRewriter.rewrite(query, cleanedStatement, semanticState)._1
    val rewrittenAst: Statement =
      Planner.rewriteStatement(firstRewriteStep, semanticState.scopeTree)
    rewrittenAst.asInstanceOf[Query].asUnionQuery.queries.head
  }

  def produceQueryGraphForPattern(query: String): QueryGraph =
    producePlannerQueryForPattern(query).lastQueryGraph
}

trait CardinalityTestHelper extends QueryGraphProducer {

  self: CypherFunSuite with LogicalPlanningTestSupport =>

  def createCardinalityModel(stats: GraphStatistics, inboundCardinality: Cardinality, semanticTable: SemanticTable): QueryGraphCardinalityModel

  def givenPattern(pattern: String) = TestUnit(pattern)
  def givenPredicate(pattern: String) = TestUnit("MATCH " + pattern)

  case class TestUnit(query: String,
                      allNodes: Option[Long] = None,
                      knownLabelCardinality: Map[String, Long] = Map.empty,
                      knownIndexSelectivity: Map[(String, String), Double] = Map.empty,
                      knownProperties: Set[String] = Set.empty,
                      knownRelationshipCardinality: Map[(String, String, String), Long] = Map.empty,
                      queryGraphArgumentIds: Set[IdName] = Set.empty,
                      inboundCardinality: Cardinality = Cardinality(1)) {
    def withInboundCardinality(d: Double) = copy(inboundCardinality = Cardinality(d))

    def withLabel(tuple: (Symbol, Long)): TestUnit = copy(knownLabelCardinality = knownLabelCardinality + (tuple._1.name -> tuple._2))
    def withLabel(label: Symbol, cardinality: Double): TestUnit = copy(knownLabelCardinality = knownLabelCardinality + (label.name -> cardinality.toLong))

    def withQueryGraphArgumentIds(idNames: IdName*): TestUnit =
      copy(queryGraphArgumentIds = Set(idNames: _*))

    def withGraphNodes(number: Long): TestUnit = copy(allNodes = Some(number))
    def withGraphNodes(number: Double): TestUnit = copy(allNodes = Some(number.toLong))

    def withRelationshipCardinality(relationship: ((Symbol, Symbol), Symbol), cardinality: Double): TestUnit =
      withRelationshipCardinality(relationship -> cardinality.toLong)

    def withRelationshipCardinality(relationship: (((Symbol, Symbol), Symbol), Long)): TestUnit = {
      val (((lhs, relType), rhs), cardinality) = relationship
      copy (
        knownRelationshipCardinality = knownRelationshipCardinality + ((lhs.name, relType.name, rhs.name) -> cardinality)
      )
    }

    def withIndexSelectivity(v: ((Symbol, Symbol), Double)) = {
      val ((Symbol(labelName), Symbol(propertyName)), cardinality) = v
      if (!knownLabelCardinality.contains(labelName))
        fail("Label not known. Add it with withLabel")

      copy(
        knownIndexSelectivity = knownIndexSelectivity + ((labelName, propertyName) -> cardinality),
        knownProperties = knownProperties + propertyName
      )
    }

    def withKnownProperty(propertyName: Symbol) =
      copy(
        knownProperties = knownProperties + propertyName.name
      )

    def prepareTestContext:(GraphStatistics, SemanticTable) = {
      val labelIds: Map[String, Int] = knownLabelCardinality.map(_._1).zipWithIndex.toMap
      val propertyIds: Map[String, Int] = knownProperties.zipWithIndex.toMap
      val relTypeIds: Map[String, Int] = knownRelationshipCardinality.map(_._1._2).toSeq.distinct.zipWithIndex.toMap

      val statistics = new GraphStatistics {

        val nodesCardinality = allNodes.
          getOrElse(fail("All nodes not set"))

        def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
          Cardinality({
            labelId.map(
              id => getLabelName(id).map(knownLabelCardinality).getOrElse(0L)
            ).getOrElse(nodesCardinality)
          }.toDouble)

        def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] = {
          val labelName: Option[String] = getLabelName(label)
          val propertyName: Option[String] = getPropertyName(property)
          (labelName, propertyName) match {
            case (Some(lName), Some(pName)) =>
              val selectivity = knownIndexSelectivity.get((lName, pName))
              selectivity.map(Selectivity.apply)

            case _ => Some(Selectivity(0))
          }
        }

        def getCardinality(fromLabel:String, typ:String, toLabel:String): Long =
          knownRelationshipCardinality.getOrElse((fromLabel, typ, toLabel), 0)

        def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
          (fromLabel, relTypeId, toLabel) match {
            case (_, Some(id), _) if getRelationshipName(id).isEmpty => Cardinality(0)
            case (Some(lhsId), Some(id), Some(rhsId)) =>
            val lhsName = getLabelName(lhsId).get
              val rhsName = getLabelName(rhsId).get
              getRelationshipName(id)
                .map(relName => Cardinality(getCardinality(lhsName, relName, rhsName)))
                .getOrElse(Cardinality(0))
            case (Some(lhsId), Some(id), None) =>
              val lhsName = getLabelName(lhsId).get
              val relName = getRelationshipName(id).get
              val relationshipCounts = knownRelationshipCardinality.collect {
                case ((x, y, _), cardinality) if x == lhsName && y == relName => cardinality
              }
              Cardinality(relationshipCounts.sum)

            case (Some(lhsId), None, Some(rhsId)) =>
              val lhsName = getLabelName(lhsId).get
              val rhsName = getLabelName(rhsId).get
              Cardinality(knownRelationshipCardinality.collect {
                case ((a, _, c), value) if a == lhsName && c == rhsName => value
              }.sum)
            case (None, Some(id), Some(rhsId)) =>
              val rhsName = getLabelName(rhsId).get
              val relName = getRelationshipName(id).get
              Cardinality(knownRelationshipCardinality.collect {
                case ((_, b, c), value) if c == rhsName && b == relName => value
              }.sum)
            case (None, None, Some(rhsId)) =>
              val rhsName = getLabelName(rhsId).get
              Cardinality(knownRelationshipCardinality.collect {
                case ((_, _, c), value) if c == rhsName => value
              }.sum)
            case (None, Some(id), None) =>
              val relName = getRelationshipName(id).get
              Cardinality(knownRelationshipCardinality.collect {
                case ((_, b, _), value) if b == relName => value
              }.sum)
            case (Some(lhsId), None, None) =>
              val lhsName = getLabelName(lhsId).get
              Cardinality(knownRelationshipCardinality.collect {
                case ((a, _, _), value) if a == lhsName => value
              }.sum)
            case (None, None, None) =>
              Cardinality(knownRelationshipCardinality.values.sum)
          }

        private def getLabelName(labelId: LabelId) = labelIds.collectFirst {
          case (name, id) if id == labelId.id => name
        }

        private def getRelationshipName(relTypeId: RelTypeId) = relTypeIds.collectFirst {
          case (name, id) if id == relTypeId.id => name
        }

        private def getPropertyName(propertyId: PropertyKeyId) = propertyIds.collectFirst {
          case (name, id) if id == propertyId.id => name
        }
      }

      val semanticTable: SemanticTable = new SemanticTable()
      fill(semanticTable.resolvedLabelIds, labelIds, LabelId.apply)
      fill(semanticTable.resolvedPropertyKeyNames, propertyIds, PropertyKeyId.apply)
      fill(semanticTable.resolvedRelTypeNames, relTypeIds, RelTypeId.apply)

      (statistics, semanticTable)
    }

    private def fill[T](destination: mutable.Map[String, T], source: Iterable[(String, Int)], f: Int => T) {
      source.foreach {
        case (name, id) => destination += name -> f(id)
      }
    }

    implicit val cardinalityEq = new Equality[Cardinality] {
      def areEqual(a: Cardinality, b: Any): Boolean = b match {
        case b: Cardinality =>
          val tolerance = Math.max(0.05, a.amount * 0.05) // 5% off is acceptable
          a.amount === b.amount +- tolerance
        case _ => false
      }
    }

    def shouldHaveQueryGraphCardinality(number: Double) {
      val (statistics, semanticTable) = prepareTestContext
      val queryGraph = createQueryGraph()
      val cardinalityModel: QueryGraphCardinalityModel = createCardinalityModel(statistics, inboundCardinality, semanticTable)
      val result = cardinalityModel(queryGraph, Map.empty)
      result should equal(Cardinality(number))
    }

    def shouldHavePlannerQueryCardinality(f: QueryGraphCardinalityModel => Metrics.CardinalityModel)(number: Double) {
      val (statistics, semanticTable) = prepareTestContext
      val graphCardinalityModel = createCardinalityModel(statistics, inboundCardinality, semanticTable)
      val cardinalityModelUnderTest = f(graphCardinalityModel)
      val plannerQuery: PlannerQuery = producePlannerQueryForPattern(query)
      val plan = newMockedLogicalPlanWithSolved(Set.empty, plannerQuery)
      cardinalityModelUnderTest(plan) should equal(Cardinality(number))
    }

    def createQueryGraph(): QueryGraph = {
      produceQueryGraphForPattern(query)
        .withArgumentIds(queryGraphArgumentIds)
    }
  }

  val DEFAULT_PREDICATE_SELECTIVITY = GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY.factor
  val DEFAULT_EQUALITY_SELECTIVITY = .1
}
