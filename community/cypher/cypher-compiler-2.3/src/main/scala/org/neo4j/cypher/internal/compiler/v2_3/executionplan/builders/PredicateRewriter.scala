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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3._
import commands._
import commands.expressions.{Property, Expression, Identifier}
import commands.values.{KeyToken, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, HasLabel, True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Namer, RandomNamer, ExecutionPlanInProgress, PlanBuilder}
import spi.PlanContext
import collection.Map
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor

class PredicateRewriter(namer: Namer = new RandomNamer) extends PlanBuilder {

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean = {
    findNodeWithLabels(plan.query.patterns).nonEmpty ||
      findPatternWithProperties(plan.query.patterns).nonEmpty ||
      findVarlengthPatternWithProperties(plan.query.patterns).nonEmpty
  }

  case class LabelExtraction(patternWithLabels: Unsolved[Pattern],
                             labels: Seq[KeyToken],
                             patternWithoutLabels: Unsolved[Pattern],
                             name: String)

  case class PropertyExtraction(patternWithProperties: Unsolved[Pattern],
                                props: Map[String, Expression],
                                patternWithoutProperties: Unsolved[Pattern],
                                name: String)

  private def findNodeWithLabels(tokens: Seq[QueryToken[Pattern]]): Option[LabelExtraction] =
    tokens.collectFirst {
      case node@Unsolved(pattern@SingleNode(name, labels, _)) if labels.nonEmpty =>
        LabelExtraction(node, labels, Unsolved(pattern.copy(labels = Seq())), name)

      case relationship@Unsolved(RelationshipPattern(pattern, left@SingleNode(name, labels, _), _))
        if labels.nonEmpty =>
        LabelExtraction(relationship, labels, Unsolved(pattern.changeEnds(left = left.copy(labels = Seq.empty))), name)

      case relationship@Unsolved(RelationshipPattern(pattern, _, right@SingleNode(name, labels, _)))
        if labels.nonEmpty =>
        LabelExtraction(relationship, labels, Unsolved(pattern.changeEnds(right = right.copy(labels = Seq.empty))), name)
    }

  private def findPatternWithProperties(tokens: Seq[QueryToken[Pattern]]): Option[PropertyExtraction] =
    tokens.collectFirst {
      case node@Unsolved(pattern@SingleNode(name, _, props)) if props.nonEmpty =>
        PropertyExtraction(node, props, Unsolved(pattern.copy(properties = Map())), name)

      case relationship@Unsolved(RelationshipPattern(pattern, left@SingleNode(name, _, props), _))
        if props.nonEmpty =>
        PropertyExtraction(relationship, props, Unsolved(pattern.changeEnds(left = left.copy(properties = Map.empty))), name)

      case relationship@Unsolved(RelationshipPattern(pattern, _, right@SingleNode(name, _, props)))
        if props.nonEmpty =>
        PropertyExtraction(relationship, props, Unsolved(pattern.changeEnds(right = right.copy(properties = Map.empty))), name)

      case relationship@Unsolved(rel@RelatedTo(_, _, relName, _, _, props)) if props.nonEmpty =>
        PropertyExtraction(relationship, props, Unsolved(rel.copy(properties = Map.empty)), relName)
    }

  private def findVarlengthPatternWithProperties(tokens: Seq[QueryToken[Pattern]]): Option[(VarLengthRelatedTo, VarLengthRelatedTo, Predicate)] = {
    val relationShip: Option[VarLengthRelatedTo] = tokens.collectFirst {
      case relationship@Unsolved(rel@VarLengthRelatedTo(_, _, _, _, _, _, _, _, props)) if props.nonEmpty =>
        rel
    }

    relationShip.map {
      rel =>
        val iteratorName = rel.relIterator.getOrElse(namer.nextName())
        val innerSymbolName = namer.nextName()
        val innerPredicate1: Seq[Predicate] = rel.properties.toSeq.map {
          case (prop, value) => Equals(Property(Identifier(innerSymbolName), UnresolvedProperty(prop)), value).asInstanceOf[Predicate]
        }
        val innerPredicate2 = True().andWith(innerPredicate1: _*)
        val predicate = AllInCollection(Identifier(iteratorName), innerSymbolName, innerPredicate2)

        (rel, rel.copy(relIterator = Some(iteratorName), properties = Map.empty), predicate)
    }
  }

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val maybeLabel = findNodeWithLabels(plan.query.patterns)
    val maybeProp = findPatternWithProperties(plan.query.patterns)
    val maybeVarlengthProp: Option[(VarLengthRelatedTo, VarLengthRelatedTo, Predicate)] = findVarlengthPatternWithProperties(plan.query.patterns)

    val (predicates, oldPattern: Unsolved[Pattern], newPattern: Unsolved[Pattern]) = (maybeLabel, maybeProp, maybeVarlengthProp) match {
      case (Some(nodeWithLabel), _, _) =>
        val predicates = mapLabelsToPredicates(nodeWithLabel.labels, nodeWithLabel.name)
        (predicates, nodeWithLabel.patternWithLabels, nodeWithLabel.patternWithoutLabels)

      case (_, Some(nodeWithProp), _) =>
        val predicates = mapPropertiesToPredicates(nodeWithProp.props, nodeWithProp.name)
        (predicates, nodeWithProp.patternWithProperties, nodeWithProp.patternWithoutProperties)

      case (_, _, Some((originalRel, newRel, predicate))) =>
        (Seq(Unsolved(predicate)), Unsolved(originalRel), Unsolved(newRel))

      case (None, None, None) =>
        throw new ThisShouldNotHappenError("Andres", "This query should not have been treated by this plan builder")
    }

    plan.copy(query = plan.query.copy(
      where = plan.query.where ++ predicates,
      patterns = plan.query.patterns.replace(oldPattern, newPattern)))
  }

  def mapLabelsToPredicates(tokens: Seq[KeyToken], name: String): Seq[Unsolved[Predicate]] = tokens.map {
    token => Unsolved(HasLabel(Identifier(name), token).asInstanceOf[Predicate])
  }

  def mapPropertiesToPredicates(props: Map[String, Expression], name: String): Seq[Unsolved[Predicate]] = props.toSeq.map {
    case (prop, value) => Unsolved(Equals(Property(Identifier(name), UnresolvedProperty(prop)), value).asInstanceOf[Predicate])
  }
}
