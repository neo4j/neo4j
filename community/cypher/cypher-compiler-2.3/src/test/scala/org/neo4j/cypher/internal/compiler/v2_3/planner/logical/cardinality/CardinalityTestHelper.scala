/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.v2_3.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.SemanticTableHelper
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, StrictnessMode}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, QueryGraphProducer, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.ast.Identifier
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, PropertyKeyId, RelTypeId, SemanticTable}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable

trait CardinalityTestHelper extends QueryGraphProducer with CardinalityCustomMatchers {

  self: CypherFunSuite with LogicalPlanningTestSupport =>

  import SemanticTableHelper._

  def combiner: SelectivityCombiner = IndependenceCombiner

  def not(number: Double) = Selectivity.of(number).getOrElse(Selectivity.ONE).negate.factor
  def and(numbers: Double*) = combiner.andTogetherSelectivities(numbers.map(Selectivity.of(_).getOrElse(Selectivity.ONE))).get.factor
  def or(numbers: Double*) = combiner.orTogetherSelectivities(numbers.map(Selectivity.of(_).getOrElse(Selectivity.ONE))).get.factor
  def orTimes(times: Int, number: Double) = combiner.orTogetherSelectivities(1.to(times).map(_ => Selectivity.of(number).get)).get.factor

  def degree(above: Double, below: Double) = above / below

  case class TestUnit(query: String,
                      allNodes: Option[Double] = None,
                      knownLabelCardinality: Map[String, Double] = Map.empty,
                      knownIndexSelectivity: Map[(String, String), Double] = Map.empty,
                      knownIndexPropertyExistsSelectivity: Map[(String, String), Double] = Map.empty,
                      knownProperties: Set[String] = Set.empty,
                      knownRelationshipCardinality: Map[(String, String, String), Double] = Map.empty,
                      knownNodeNames: Set[String] = Set.empty,
                      knownRelNames: Set[String] = Set.empty,
                      queryGraphArgumentIds: Set[IdName] = Set.empty,
                      inboundCardinality: Cardinality = Cardinality(1),
                      strictness: Option[StrictnessMode] = None) {

    self =>

    def withNodeName(nodeName: String) = copy(knownNodeNames = knownNodeNames + nodeName)

    def withRelationshipName(relName: String) = copy(knownRelNames = knownRelNames + relName)

    def withInboundCardinality(d: Double) = copy(inboundCardinality = Cardinality(d))

    def withLabel(tuple: (Symbol, Double)): TestUnit = copy(knownLabelCardinality = knownLabelCardinality + (tuple._1.name -> tuple._2))

    def withLabel(label: Symbol, cardinality: Double): TestUnit = copy(knownLabelCardinality = knownLabelCardinality + (label.name -> cardinality))

    def addWithLabels(cardinality: Double, labels: Symbol*) = {
      val increments = labels.map { label => label.name -> cardinality }.toMap
      copy(knownLabelCardinality = knownLabelCardinality.fuse(increments)(_ + _))
    }

    def withQueryGraphArgumentIds(idNames: IdName*): TestUnit =
      copy(queryGraphArgumentIds = Set(idNames: _*))

    def withGraphNodes(number: Double): TestUnit = copy(allNodes = Some(number))


    def withRelationshipCardinality(relationship: (((Symbol, Symbol), Symbol), Double)): TestUnit = {
      val (((lhs, relType), rhs), cardinality) = relationship
      val key = (lhs.name, relType.name, rhs.name)
      assert(!knownRelationshipCardinality.contains(key), "This label/type/label combo is already known")
      copy (
        knownRelationshipCardinality = knownRelationshipCardinality + (key -> cardinality)
      )
    }

    def addRelationshipCardinality(relationship: (((Symbol, Symbol), Symbol), Double)): TestUnit = {
      val (((lhs, relType), rhs), cardinality) = relationship
      val key = (lhs.name, relType.name, rhs.name)
      val increment = Map(key -> cardinality)
      copy (
        knownRelationshipCardinality = knownRelationshipCardinality.fuse(increment)(_ + _)
      )
    }

    def withIndexSelectivity(v: ((Symbol, Symbol), Double)) = {
      val ((Symbol(labelName), Symbol(propertyName)), selectivity) = v
      if (!knownLabelCardinality.contains(labelName))
        fail("Label not known. Add it with withLabel")

      copy(
        knownIndexSelectivity = knownIndexSelectivity + ((labelName, propertyName) -> selectivity),
        knownProperties = knownProperties + propertyName
      )
    }

    def withIndexPropertyExistsSelectivity(v: ((Symbol, Symbol), Double)) = {
      val ((Symbol(labelName), Symbol(propertyName)), selectivity) = v
      if (!knownLabelCardinality.contains(labelName))
        fail("Label not known. Add it with withLabel")

      copy(
        knownIndexPropertyExistsSelectivity = knownIndexPropertyExistsSelectivity + ((labelName, propertyName) -> selectivity),
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
              id => getLabelName(id).map(knownLabelCardinality).getOrElse(0.0)
            ).getOrElse(nodesCardinality)
          })

        def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] = {
          val labelName: Option[String] = getLabelName(label)
          val propertyName: Option[String] = getPropertyName(property)
          (labelName, propertyName) match {
            case (Some(lName), Some(pName)) =>
              val selectivity = knownIndexSelectivity.get((lName, pName))
              selectivity.map(Selectivity.of(_).getOrElse(Selectivity.ONE))

            case _ => Some(Selectivity.ZERO)
          }
        }

        def indexPropertyExistsSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] = {
          val labelName: Option[String] = getLabelName(label)
          val propertyName: Option[String] = getPropertyName(property)
          (labelName, propertyName) match {
            case (Some(lName), Some(pName)) =>
              val selectivity = knownIndexPropertyExistsSelectivity.get((lName, pName))
              selectivity.map(s => Selectivity.of(s).get)

            case _ => Some(Selectivity.ZERO)
          }
        }

        def getCardinality(fromLabel:String, typ:String, toLabel:String): Double =
          knownRelationshipCardinality.getOrElse((fromLabel, typ, toLabel), 0.0)

        def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
          (fromLabel, relTypeId, toLabel) match {
            case (_, Some(id), _) if getRelationshipName(id).isEmpty => Cardinality(0)
            case (Some(id), _, _) if getLabelName(id).isEmpty        => Cardinality(0)
            case (_, _, Some(id)) if getLabelName(id).isEmpty        => Cardinality(0)

            case (l1, t1, r1) =>
              val matchingCardinalities = knownRelationshipCardinality collect {
                case ((l2, t2, r2), c) if
                l1.forall(x => getLabelName(x).get == l2) &&
                  t1.forall(x => getRelationshipName(x).get == t2) &&
                  r1.forall(x => getLabelName(x).get == r2) => c
              }

              if (matchingCardinalities.isEmpty)
                Cardinality(0)
              else
                Cardinality(matchingCardinalities.sum)
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

      val semanticTable: SemanticTable = {
        val empty = SemanticTable()
        val withNodes = knownNodeNames.foldLeft(empty) { case (table, node) => table.addNode(Identifier(node)(pos)) }
        val withNodesAndRels = knownRelNames.foldLeft(withNodes) { case (table, rel) => table.addRelationship(Identifier(rel)(pos)) }
        withNodesAndRels
      }

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

    def createQueryGraph(semanticTable: SemanticTable): (QueryGraph, SemanticTable) = {
      val (queryGraph, rewrittenTable) = produceQueryGraphForPattern(query)
      (queryGraph.withArgumentIds(queryGraphArgumentIds), semanticTable.transplantResolutionOnto(rewrittenTable))
    }
  }
}

trait CardinalityCustomMatchers {

  class MapEqualityWithDouble[T](expected: Map[T, Cardinality], tolerance: Double)(implicit num: Numeric[Cardinality])
    extends Matcher[Map[T, Cardinality]] {

    def apply(other: Map[T, Cardinality]) = {
      MatchResult(
        expected.size == other.size && expected.foldLeft(true) {
          case (acc, (key, value)) =>
            import Cardinality._
            import org.scalautils.Tolerance._
            import org.scalautils.TripleEquals._
            acc && other.contains(key) && other(key) === value +- tolerance
        },
        s"""$other did not equal "$expected" wrt a tolerance of $tolerance""",
        s"""$other equals "$expected" wrt a tolerance of $tolerance"""
      )
    }

  }

  def equalWithTolerance[T](expected: Map[T, Cardinality], tolerance: Double): Matcher[Map[T, Cardinality]] = {
    new MapEqualityWithDouble[T](expected, tolerance)(NumericCardinality)
  }
}
