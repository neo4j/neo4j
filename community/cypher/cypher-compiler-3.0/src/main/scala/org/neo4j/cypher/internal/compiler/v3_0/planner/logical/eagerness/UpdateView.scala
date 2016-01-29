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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.helpers.fixedPoint

import scala.annotation.tailrec

case class UpdateView(mutatingPatterns: Seq[MutatingPattern]) extends Update {

  override def deletes(name: IdName) = {

    @tailrec
    def matches(step: PathStep): Boolean = step match {
      case NodePathStep(Variable(id), next) => id == name.name || matches(next)
      case SingleRelationshipPathStep(Variable(id), _, next) => id == name.name || matches(next)
      case MultiRelationshipPathStep(Variable(id), _, next) => id == name.name || matches(next)
      case NilPathStep => false
    }

    mutatingPatterns.exists {
      case DeleteExpressionPattern(Variable(id), _) if id == name.name => true
      case DeleteExpressionPattern(_:PathExpression, _) => true // Conservative (because we do not know the nodes). Any delete against a path will trigger Eagerness
      case _ => false
    }
  }

  override def createsRelationships = mutatingPatterns.exists(_.isInstanceOf[CreateRelationshipPattern])

  override def updatesRelationships = mutatingPatterns.exists {
    case _: CreateRelationshipPattern => true
    case _: SetRelationshipPropertiesFromMapPattern => true
    case _: SetRelationshipPropertyPattern => true
    case _: DeleteExpressionPattern => true
    case _ => false
  }

  override def isEmpty = mutatingPatterns.isEmpty

  override def addedLabelsNotOn(id: IdName): Set[LabelName] = (mutatingPatterns collect {
    case x: SetLabelPattern if x.idName != id => x.labels
    case x: CreateNodePattern => x.labels

  }).flatten.toSet

  override def removedLabelsNotOn(id: IdName): Set[LabelName] = (mutatingPatterns collect {
    case x: RemoveLabelPattern if x.idName != id => x.labels
  }).flatten.toSet

  override def containsDeletes = mutatingPatterns.exists(_.isInstanceOf[DeleteExpressionPattern])

  override def deletesOtherThan(ids: Set[IdName]): Boolean = {
    // Recursively go down FOREACHs to fetch all DELETEs

    val deleteActions = fixedPoint((patterns: Seq[MutatingPattern]) => patterns.collect {
      case x: DeleteExpressionPattern => Seq(x)
      case ForeachPattern(_, _, pq) => pq.allQueryGraphs.flatMap(_.mutatingPatterns)
      case _ => Seq.empty
    }.flatten)(mutatingPatterns)


    val onlySafeDeletes = deleteActions forall {
      case DeleteExpressionPattern(Variable(name), _) if ids(IdName(name)) => true
      case _ => false
    }

    !onlySafeDeletes
  }

  override def createsNodes = mutatingPatterns.exists(_.isInstanceOf[CreateNodePattern])

  override def updatesNodePropertiesNotOn(id: IdName): CreatesPropertyKeys = mutatingPatterns.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys) {
    case (acc, c: SetNodePropertyPattern) if c.idName != id => acc + CreatesKnownPropertyKeys(Set(c.propertyKey))
    case (acc, c: SetNodePropertiesFromMapPattern) if c.idName != id => acc + CreatesPropertyKeys(c.expression)
    case (acc, CreateNodePattern(_, _, Some(properties))) => acc + CreatesPropertyKeys(properties)
    case (acc, _) => acc
  }

  override def relationshipPropertyUpdatesNotOn(id: IdName): CreatesPropertyKeys = collectPropertyUpdates(_ != id)

  override def allRelationshipPropertyUpdates: CreatesPropertyKeys = collectPropertyUpdates(_ => true)

  private def collectPropertyUpdates(f: (IdName => Boolean)) = mutatingPatterns.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys) {
    case (acc, c: SetRelationshipPropertyPattern) if f(c.idName) => acc + CreatesKnownPropertyKeys(Set(c.propertyKey))
    case (acc, c: SetRelationshipPropertiesFromMapPattern) if f (c.idName) => acc + CreatesPropertyKeys(c.expression)
    case (acc, CreateRelationshipPattern(_, _, _, _, Some(props), _)) => acc + CreatesPropertyKeys(props)
    case (acc, _) => acc
  }

  override def relTypesCreated: Set[RelTypeName] = mutatingPatterns.collect {
    case p: CreateRelationshipPattern => p.relType
  }.toSet
}
