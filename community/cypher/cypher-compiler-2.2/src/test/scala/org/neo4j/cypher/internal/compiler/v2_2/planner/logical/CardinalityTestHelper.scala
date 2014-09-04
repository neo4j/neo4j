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
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, Planner, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, PropertyKeyId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{TokenContext, GraphStatistics}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._

case class CardinalityTestHelper(query: String,
                                 allNodes: Option[Int] = None,
                                 knownLabelCardinality: Map[String, Int] = Map.empty,
                                 knownIndexSelectivity: Map[(String, String), Double] = Map.empty,
                                 knownProperties: Set[String] = Set.empty) extends CypherFunSuite with LogicalPlanningTestSupport {


  def withLabel(tuple: (Symbol, Int)): CardinalityTestHelper = copy(knownLabelCardinality = knownLabelCardinality + (tuple._1.name -> tuple._2))

  def withAllNodes(number: Int): CardinalityTestHelper = copy(allNodes = Some(number))

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

  def shouldHaveCardinality(number: Double) {
    shouldHaveCardinality(Math.round(number).toInt)
  }

  def shouldHaveCardinality(number: Int) {

    val labelIds: Map[String, Int] = knownLabelCardinality.map(_._1).zipWithIndex.toMap
    val propertyIds: Map[String, Int] = knownProperties.zipWithIndex.toMap

    val statistics = new GraphStatistics {

      def nodesWithLabelCardinality(labelId: LabelId): Cardinality = {
        val labelName: Option[String] = getLabelName(labelId)

        labelName.
          map(l => Cardinality(knownLabelCardinality(l))).
          getOrElse(Cardinality(0))
      }

      def nodesCardinality: Cardinality =
        allNodes.
          map(x => Cardinality(x)).
          getOrElse(fail("All nodes not set"))

      def indexSelectivity(label: LabelId, property: PropertyKeyId): Selectivity = {
        val labelName: Option[String] = getLabelName(label)
        val propertyName: Option[String] = getPropertyName(property)
        (labelName, propertyName) match {
          case (Some(lName), Some(pName)) =>
            val selectivity = knownIndexSelectivity.get((lName, pName))
            selectivity.map(Selectivity.apply).getOrElse(GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY)

          case _ => Selectivity(0)
        }
      }

      private def getLabelName(labelId: LabelId) = labelIds.collectFirst {
        case (name, id) if id == labelId.id => name
      }

      private def getPropertyName(propertyId: PropertyKeyId) = propertyIds.collectFirst {
        case (name, id) if id == propertyId.id => name
      }

      // Not used
      def nodesWithLabelSelectivity(labelId: LabelId) = ???
      def degreeByRelationshipTypeAndDirection(relTypeId: RelTypeId, direction: Direction) = ???
      def degreeByLabelRelationshipTypeAndDirection(labelId: LabelId, relTypeId: RelTypeId, direction: Direction) = ???
      def relationshipsWithTypeSelectivity(relTypeId: RelTypeId) = ???
      def degreeByLabelRelationshipTypeAndDirection(fromLabel: LabelId, relTypeId: RelTypeId, direction: Direction, toLabel: LabelId): Multiplier = ???
    }

    val tokenContext = new TokenContext {
      def getOptLabelId(labelName: String) = labelIds.get(labelName)
      def getOptPropertyKeyId(propertyKeyName: String) = propertyIds.get(propertyKeyName)

      // Not used
      def getOptRelTypeId(relType: String) = ???
      def getRelTypeName(id: Int) = ???
      def getRelTypeId(relType: String) = ???
      def getLabelName(id: Int) = ???
      def getPropertyKeyId(propertyKeyName: String) = ???
      def getPropertyKeyName(id: Int) = ???
      def getLabelId(labelName: String) = ???
    }

    val queryGraph = createQueryGraphAndSemanticStableTable()
    val selectivityModel = new StatisticsBackedSelectivityModel(statistics, queryGraph, tokenContext)
    val cardinalityModel = QueryGraphCardinalityModel(statistics, selectivityModel, tokenContext)

    val result = cardinalityModel(queryGraph)
    result.map(n => Math.round(n)) should equal(Cardinality(number))
  }

  def createQueryGraphAndSemanticStableTable(): QueryGraph = {
    val q = query + " RETURN 1"
    val ast = parser.parse(q)

    val firstRewriteStep = astRewriter.rewrite(query, ast)._1
    val rewrittenAst: Statement =
      Planner.rewriteStatement(firstRewriteStep)

    rewrittenAst.asInstanceOf[Query].asUnionQuery.queries.head.graph
  }
}
