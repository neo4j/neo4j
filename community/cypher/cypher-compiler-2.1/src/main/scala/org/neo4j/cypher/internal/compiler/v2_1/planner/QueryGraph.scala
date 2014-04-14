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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, IdName}

abstract class QueryGraph {
  def selections: Selections
  def patternNodes: Set[IdName]
  def patternRelationships: Set[PatternRelationship]

  def coveredIds: Set[IdName] = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)

  def argumentIds: Set[IdName] = Set.empty

  def optionalMatches: Seq[OptionalQueryGraph] = Seq.empty

  def knownLabelsOnNode(node: IdName): Seq[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Seq.empty)
      .flatMap(_.labels).toSeq

  def findRelationshipsEndingOn(id: IdName): Set[PatternRelationship] =
    patternRelationships.filter { r => r.nodes._1 == id || r.nodes._2 == id }


}

case class OptionalQueryGraph(selections: Selections,
                              patternNodes: Set[IdName],
                              patternRelationships: Set[PatternRelationship],
                              override val argumentIds: Set[IdName]) extends QueryGraph

/*
An abstract representation of the query graph being solved at the current step
 */
case class MainQueryGraph(projections: Map[String, Expression],
                          selections: Selections,
                          patternNodes: Set[IdName],
                          patternRelationships: Set[PatternRelationship],
                          override val optionalMatches: Seq[OptionalQueryGraph]) extends QueryGraph {

  override def coveredIds: Set[IdName] = super.coveredIds ++ optionalMatches.flatMap(_.coveredIds)

  def withAddedOptionalMatch(selections:Selections, nodes:Set[IdName], rels:Set[PatternRelationship]): MainQueryGraph = {
    val argumentIds = coveredIds intersect QueryGraph.coveredIdsForPatterns(nodes, rels)
    copy(optionalMatches = optionalMatches :+ OptionalQueryGraph(selections, nodes, rels, argumentIds))
  }
}

object QueryGraph {
  def empty: MainQueryGraph = MainQueryGraph(Map.empty, Selections(), Set.empty, Set.empty, Seq.empty)

  def coveredIdsForPatterns(patternNodes: Set[IdName], patternRels: Set[PatternRelationship]) =
    patternNodes ++ patternRels.flatMap(_.coveredIds)
}

object SelectionPredicates {
  def fromWhere(where: Where): Seq[(Set[IdName], Expression)] = extractPredicates(where.expression)

  private def idNames(predicate: Expression): Set[IdName] = predicate.treeFold(Set.empty[IdName]) {
    case id: Identifier =>
      (acc: Set[IdName], _) => acc + IdName(id.name)
  }

  private def extractPredicates(predicate: Expression): Seq[(Set[IdName], Expression)] = {
    predicate.treeFold(Seq.empty[(Set[IdName], Expression)]) {
      // n:Label
      case predicate@HasLabels(identifier@Identifier(name), labels) =>
        (acc, _) => acc ++ labels.map { label: LabelName =>
          Set(IdName(name)) -> predicate.copy(labels = Seq(label))(predicate.position)
        }
      // and
      case _: And =>
        (acc, children) => children(acc)
      case predicate: Expression =>
        (acc, _) => acc :+ (idNames(predicate) -> predicate)
    }
  }
}


