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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NamedPath
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, IdName}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.NameSupport


trait SubQuery {
  def queryGraph:QueryGraph
}

case class OptionalMatch(queryGraph:QueryGraph) extends SubQuery
case class Exists(exp:PatternExpression, queryGraph:QueryGraph) extends SubQuery

// An abstract representation of the query graph being solved at the current step
case class QueryGraph(patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      namedPaths: Set[NamedPath] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      projections: Map[String, Expression] = Map.empty,
                      subQueries: Seq[SubQuery] = Seq.empty) {

  def ++(in: QueryGraph): QueryGraph = in match {
    case other: QueryGraph => QueryGraph(
      projections = projections ++ other.projections,
      selections = selections ++ other.selections,
      patternNodes = patternNodes ++ other.patternNodes,
      patternRelationships = patternRelationships ++ other.patternRelationships,
      subQueries = subQueries ++ other.subQueries,
      namedPaths = namedPaths ++ other.namedPaths,
      argumentIds = argumentIds ++ other.argumentIds)
  }

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = coveredIds intersect optionalMatch.coveredIds
    copy(subQueries = subQueries :+ OptionalMatch(optionalMatch.addArgumentId(argumentIds.toSeq))).
      addCoveredIdsAsProjections()
  }

  def addPatternNodes(nodes: Traversable[IdName]): QueryGraph = nodes.foldLeft(QueryGraph.empty) {
    case (qg, id) => qg.addPatternNode(id)
  }

  def introducedIds: Set[IdName] = coveredIds -- argumentIds

  def addPatternNode(node: IdName) = copy(
    patternNodes = patternNodes + node,
    projections = projections + symbol(node.name)
  )

  def add(rel: PatternRelationship) =
    addPatternNode(rel.nodes._1).
      addPatternNode(rel.nodes._2).
      copy(
        patternRelationships = addPatternNode(rel.nodes._1).addPatternNode(rel.nodes._2).patternRelationships + rel,
        projections = addPatternNode(rel.nodes._1).addPatternNode(rel.nodes._2).projections + symbol(rel.name.name))

  def add(path: NamedPath) = copy(
    namedPaths = namedPaths + path,
    projections = projections + symbol(path.name.name)  )

  def addArgumentId(newIds: Seq[IdName]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)

  private def symbol(name: String) = name -> Identifier(name)(null)

  protected def addPreparedPredicates(predicates: Seq[Predicate]) =
    copy(selections = selections.copy(predicates = selections.predicates ++ predicates))

  def changeProjections(projections: Map[String, Expression]) = copy(projections = projections)

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def coveredIds: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships, namedPaths)
    val optionalMatchIds = subQueries.flatMap {
      case OptionalMatch(qg) => qg.coveredIds
    }
    patternIds ++ argumentIds ++ optionalMatchIds
  }

  def knownLabelsOnNode(node: IdName): Seq[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Seq.empty)
      .flatMap(_.labels).toSeq

  def findRelationshipsEndingOn(id: IdName): Set[PatternRelationship] =
    patternRelationships.filter {
      r => r.nodes._1 == id || r.nodes._2 == id
    }

  def add(predicates: Seq[Expression]): QueryGraph = addPreparedPredicates(predicates.flatMap(SelectionPredicates.extractPredicates))

  def addCoveredIdsAsProjections(): QueryGraph = {
    val coveredIdProjections = coveredIds.map(x => x.name -> Identifier(x.name)(null)).toMap
    copy(projections = projections ++ coveredIdProjections)
  }

  def optionalMatches : Seq[QueryGraph] = subQueries.collect {
    case OptionalMatch(qg) => qg
  }
}

object QueryGraph {
  val empty: QueryGraph = QueryGraph()

  def coveredIdsForPatterns(patternNodes: Set[IdName], patternRels: Set[PatternRelationship], namedPaths: Set[NamedPath]) = {
    val map1 = patternRels.flatMap(_.coveredIds)
    val map2 = namedPaths.flatMap(_.coveredIds)
    patternNodes ++ map1 ++ map2
  }
}

object SelectionPredicates {
  def fromWhere(where: Where): Set[Predicate] = extractPredicates(where.expression)

  private def idNames(predicate: Expression): Set[IdName] = predicate.treeFold(Set.empty[IdName]) {
    case Identifier(name) =>
      (acc: Set[IdName], _) => acc + IdName(name)
  }

  def extractPredicates(predicate: Expression): Set[Predicate] = {
    predicate.treeFold(Set.empty[Predicate]) {
      // n:Label
      case predicate@HasLabels(identifier@Identifier(name), labels) =>
        (acc, _) => acc ++ labels.map { label: LabelName =>
          Predicate(Set(IdName(name)), predicate.copy(labels = Seq(label))(predicate.position))
        }
      // and
      case _: And =>
        (acc, children) => children(acc)
      case predicate: Expression =>
        (acc, _) => acc + Predicate(idNames(predicate), predicate)
    }
  }.toSet
}
