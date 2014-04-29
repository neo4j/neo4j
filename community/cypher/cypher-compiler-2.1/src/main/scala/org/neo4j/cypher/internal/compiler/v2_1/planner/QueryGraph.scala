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
import org.neo4j.cypher.InternalException

trait SubQuery {
  def queryGraph: QueryGraph
}

case class OptionalMatch(queryGraph: QueryGraph) extends SubQuery
case class Exists(predicate: Predicate, queryGraph: QueryGraph) extends SubQuery

// An abstract representation of the query graph being solved at the current step
case class QueryGraph(patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      projections: Map[String, Expression] = Map.empty,
                      sortItems: Seq[SortItem] = Seq.empty,
                      subQueries: Seq[SubQuery] = Seq.empty,
                      limit: Option[Expression] = None,
                      skip: Option[Expression] = None,
                      tail: Option[QueryGraph] = None) {

  def ++(other: QueryGraph): QueryGraph =
    QueryGraph(
      projections = projections ++ other.projections,
      selections = selections ++ other.selections,
      patternNodes = patternNodes ++ other.patternNodes,
      patternRelationships = patternRelationships ++ other.patternRelationships,
      subQueries = subQueries ++ other.subQueries,
      argumentIds = argumentIds ++ other.argumentIds,
      limit = either(limit, other.limit),
      skip = either(skip, other.skip))

  private def either[T](a: Option[T], b:Option[T]):Option[T] = (a, b) match {
    case (None, s) => s
    case (s, None) => s
    case (None, None) => None
    case (Some(_), Some(_)) => throw new InternalException("Can't join two query graphs with different SKIP")
  }


  def equivalent(other: QueryGraph) =
    patternRelationships == other.patternRelationships &&
    patternNodes == other.patternNodes &&
    selections == other.selections &&
    projections == other.projections &&
    sortItems == other.sortItems

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

  def addArgumentId(newIds: Seq[IdName]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)

  private def symbol(name: String) = name -> Identifier(name)(null)

  protected def addPreparedPredicates(predicates: Seq[Predicate]) =
    copy(selections = selections.copy(predicates = selections.predicates ++ predicates))

  def changeProjections(projections: Map[String, Expression]) = copy(projections = projections)

  def changeSortItems(sortItems: Seq[SortItem]) = copy(sortItems = sortItems)

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def coveredIds: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    val optionalMatchIds = subQueries.flatMap {
      case OptionalMatch(qg) => qg.coveredIds
      case _ => Vector.empty
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

  def patternPredicates = subQueries.collect { case e: Exists => e }
}

object QueryGraph {
  val empty: QueryGraph = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }
}

object SelectionPredicates {
  def fromWhere(where: Where): Set[Predicate] = extractPredicates(where.expression)

  def idNames(predicate: Expression): Set[IdName] = predicate.treeFold(Set.empty[IdName]) {
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
      // iterable expression should not depend on the identifier they introduce
      case predicate: IterablePredicateExpression =>
        val innerDeps = predicate.innerPredicate.map(idNames(_)).getOrElse(Set.empty) - IdName(predicate.identifier.name)
        (acc, _) => acc + Predicate(idNames(predicate.expression) ++ innerDeps, predicate)
      // generic expression
      case predicate: Expression =>
        (acc, _) => acc + Predicate(idNames(predicate), predicate)
    }
  }.toSet
}
