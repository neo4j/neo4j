/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.{Property, Identifier}
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.internal.commands.SchemaIndex
import scala.Some
import org.neo4j.cypher.internal.commands.HasLabel
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.NodeByLabel

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use
 */
class StartPointChoosingBuilder extends PlanBuilder {

  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    def findStartPoints( query:PartiallySolvedQuery, boundNodes:Seq[String] ) : Seq[StartItem] =
      query.matchPattern.disconnectedPatternsWithout(boundNodes).map {
        p => tryFindingIndexSeekPlan(p, plan.query.where, ctx) getOrElse {
          tryFindingALabelScanPlan(p, plan.query.where) getOrElse ({
            pickAGlobalScanStart(p)
          })
        }
    }

    def findStartPointsForFullQuery( query:PartiallySolvedQuery, boundNodes:Seq[String]): PartiallySolvedQuery = {
      query.start match {
        case Seq() if query.matchPattern.nonEmpty => query.copy(start=findStartPoints(query, boundNodes).map(Unsolved(_)))
        case _                                    => query
      }
    }

    // Note: It'd be easy to combine this with having some bound start points here
    plan.copy(query=findStartPointsForFullQuery(plan.query, plan.pipe.symbols.keys))
  }


  private def pickAGlobalScanStart(pattern: MatchPattern): StartItem =
    AllNodes(pattern.possibleStartNodes.head)

  private def tryFindingALabelScanPlan(pattern: MatchPattern, where:Seq[QueryToken[Predicate]]): Option[StartItem] = {
    val identifierToLabel: Map[IdentifierName, Seq[LabelName]] = extractIdentifiersWithLabels(pattern, where)
    if (identifierToLabel.nonEmpty) {
      val labelName = identifierToLabel.values.head.head
      val identifier = identifierToLabel.keys.head
      Some(NodeByLabel(identifier, labelName))
    } else {
      None
    }
  }

  private def tryFindingIndexSeekPlan(plan: MatchPattern, where:Seq[QueryToken[Predicate]], ctx: PlanContext):
  Option[StartItem] = {

    val identifierToLabel: Map[IdentifierName, Seq[LabelName]] = extractIdentifiersWithLabels(plan, where)
    val labelPropertyCombo: Map[IdentifierName, Map[LabelName, Seq[PropertyKey]]] =
      extractPropertiesForLabels(identifierToLabel, where).filter{
        case (identifier, labelPropertyKey) => labelPropertyKey.values.exists(_.nonEmpty)
      }

    val possibleHints = for {
      (identifier, labelsAndProps) <- labelPropertyCombo
      (label, props) <- labelsAndProps
      prop <- props

      if (ctx.getIndexRuleId(label, prop).nonEmpty)
    } yield SchemaIndex(identifier, label, prop, None)


    if (possibleHints.isEmpty) {
      None
    } else {
      Some(possibleHints.head)
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) = !plan.query.extracted && plan != apply(plan, ctx) // TODO: This can be optimized

  def priority = PlanBuilder.IndexLookup

  private def extractIdentifiersWithLabels(pattern: MatchPattern, where:Seq[QueryToken[Predicate]]): Map[IdentifierName, Seq[LabelName]] = {
    val labelledNodes: Seq[(String, Seq[LabelValue])] = where.flatMap {
      case Unsolved(HasLabel(Identifier(identifier), labelNames)) => Some(identifier -> labelNames)
      case _                                                      => None
    }

    var identifierToLabel = Map[IdentifierName, Seq[LabelName]]()

    labelledNodes.foreach {
      case (identifier, labelValues) =>
        val combinedLabels = identifierToLabel.getOrElse(identifier, Seq.empty) ++ labelValues.map(_.name)
        identifierToLabel += (identifier -> combinedLabels)
    }

    val identifiersInPattern = pattern.possibleStartNodes.toSet
    identifierToLabel.filterKeys( identifiersInPattern.contains(_) )
  }

  private def extractPropertiesForLabels(identifierToLabel: Map[String, Seq[String]], where: Seq[QueryToken[Predicate]]):
  Map[String, Map[String, Seq[String]]] = identifierToLabel.map {
    case (identifier, labels) =>

      val properties: Seq[String] = where.flatMap {
        case Unsolved(Equals(Property(Identifier(id), propertyName), expression)) if id == identifier =>
          Some(propertyName)

        case Unsolved(Equals(expression, Property(Identifier(id), propertyName))) if id == identifier =>
          Some(propertyName)

        case _                                                                                        => None
      }

      val propertiesPerLabel: Map[String, Seq[String]] = labels.map(label => label -> properties).toMap
      identifier -> propertiesPerLabel

  }.toMap

}