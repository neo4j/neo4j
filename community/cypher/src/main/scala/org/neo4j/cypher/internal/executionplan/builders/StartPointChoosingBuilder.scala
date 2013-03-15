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

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands.{NodeByLabel, SchemaIndex, Equals, HasLabel}
import org.neo4j.cypher.internal.commands.expressions.{Property, Identifier}
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.UnableToPickIndexException

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use
 */
class StartPointChoosingBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    val identifierToLabel: Map[String, Seq[String]] = extractIdentifiersWithLabels(plan)

    val labelPropertyCombo: Map[String, Map[String, Seq[String]]] = extractPropertiesForLabels(identifierToLabel, plan)

    val noProperty = !labelPropertyCombo.exists {
      case (identifier, map) => map.values.exists(_.nonEmpty)
    }

    val singleLabelPredicate =
      identifierToLabel.size == 1 &&
        identifierToLabel.values.head.size == 1

    if (noProperty && singleLabelPredicate) {
      val (identifier, labels) = identifierToLabel.head

      produceLabelStartItem(plan, identifier, labels.head)
    } else {
      tryToFindMatchingIndex(labelPropertyCombo, ctx, plan)
    }
  }

  private def produceLabelStartItem(plan: ExecutionPlanInProgress, identifier: String, label: String): ExecutionPlanInProgress = {
    plan.copy(query = plan.query.copy(start = Seq(Unsolved(NodeByLabel(identifier, label)))))
  }

  private def tryToFindMatchingIndex(labelPropertyCombo: Map[String, Map[String, Seq[String]]], ctx: PlanContext, plan: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    val possibleHints = for {
      (identifier, labelsAndProps) <- labelPropertyCombo
      (label, props) <- labelsAndProps
      prop <- props

      if (ctx.getIndexRuleId(label, prop).nonEmpty)
    } yield SchemaIndex(identifier, label, prop, None)

    val labels = labelPropertyCombo.head._2.keys.toList

    possibleHints.toList match {
      case head :: Nil             => plan.copy(query = plan.query.copy(start = Seq(Unsolved(possibleHints.head))))
      case head :: tail            => throw new UnableToPickIndexException("More than one index available to start from, please use index hints to pick one.")
      case Nil if labels.size == 1 => produceLabelStartItem(plan, labelPropertyCombo.keys.head, labels.head)
      case Nil                     => throw new UnableToPickIndexException("There is no index available to start the query from, please add an explicit start clause.")
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    plan.pipe.symbols.isEmpty &&
      plan.query.start.isEmpty &&
      !plan.query.extracted

  def priority = PlanBuilder.IndexLookup

  private def extractIdentifiersWithLabels(plan: ExecutionPlanInProgress): Map[String, Seq[String]] = {
    val labelledNodes: Seq[(String, Seq[LabelValue])] = plan.query.where.flatMap {
      case Unsolved(HasLabel(Identifier(identifier), labelNames)) => Some(identifier -> labelNames)
      case _                                                      => None
    }

    var identifierToLabel = Map[String, Seq[String]]()

    labelledNodes.foreach {
      case (identifier, labelValues) =>
        val combinedLabels = identifierToLabel.getOrElse(identifier, Seq.empty) ++ labelValues.map(_.name)
        identifierToLabel += (identifier -> combinedLabels)
    }

    identifierToLabel
  }

  private def extractPropertiesForLabels(identifierToLabel: Map[String, Seq[String]], plan: ExecutionPlanInProgress):
  Map[String, Map[String, Seq[String]]] = {
    identifierToLabel.map {
      case (identifier, labels) =>

        val properties: Seq[String] = plan.query.where.flatMap {
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
}