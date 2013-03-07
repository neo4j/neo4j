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
import org.neo4j.cypher.internal.commands.{IndexHint, Equals, HasLabel}
import org.neo4j.cypher.internal.commands.expressions.{Property, Identifier}
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.{UnableToPickIndexException, IndexHintException}

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use
 */
class StartPointChoosingBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    val identifierToLabel: Map[String, Seq[String]] = extractIdentifiersWithLabels(plan)

    val labelPropertyCombo: Map[String, Map[String, Seq[String]]] = extractPropertiesForLabels(identifierToLabel, plan)

    val possibleHints = for {
      (identifier, labelsAndProps) <- labelPropertyCombo
      (label, props)               <- labelsAndProps
      prop                         <- props

      if( ctx.getIndexRuleId(label, prop).nonEmpty )
    } yield IndexHint(identifier, label, prop, None )

    possibleHints.toList match {
      case head :: Nil  => plan.copy( query = plan.query.copy(start=Seq(Unsolved(possibleHints.head))))
      case head :: tail => throw new UnableToPickIndexException("More than one index available to start from, please use index hints to pick one.")
      case Nil          => throw new UnableToPickIndexException("There is no index available to start the query from, please add an explicit start clause.")
    }


  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    plan.pipe.symbols.isEmpty &&
      plan.query.start.isEmpty

  def priority = PlanBuilder.IndexLookup

  private def extractIdentifiersWithLabels(plan: ExecutionPlanInProgress):Map[String, Seq[String]] = {
    val labelledNodes: Seq[(String, Seq[LabelValue])] = plan.query.where.flatMap {
      case Unsolved(HasLabel(Identifier(identifier), labelNames)) => Some(identifier -> labelNames)
      case _ => None
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
  Map[String, Map[String, Seq[String]]] =
  {
    identifierToLabel.map
      {
        case (identifier, labels) =>

          val properties: Seq[String] = plan.query.where.flatMap {
            case Unsolved(Equals(Property(Identifier(id), propertyName), expression)) if id == identifier =>
              Some(propertyName)
            case Unsolved(Equals(expression, Property(Identifier(id), propertyName))) if id == identifier =>
              Some(propertyName)
            case _ => None
          }

          val toMap: Map[String, Seq[String]] = labels.map(label => label -> properties).toMap
          identifier -> toMap

      }.toMap
  }
}