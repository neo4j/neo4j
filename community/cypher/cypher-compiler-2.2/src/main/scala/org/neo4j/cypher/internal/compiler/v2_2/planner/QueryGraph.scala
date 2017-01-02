/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

import scala.collection.{mutable, GenTraversableOnce}


case class QueryGraph(patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: Seq[QueryGraph] = Seq.empty,
                      hints: Set[Hint] = Set.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty)
  extends PageDocFormatting { // with ToPrettyString[QueryGraph] {
//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def size = patternRelationships.size

  def addPatternNodes(nodes: IdName*): QueryGraph = copy(patternNodes = patternNodes ++ nodes)

  def addPatternRelationship(rel: PatternRelationship): QueryGraph =
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      patternRelationships = patternRelationships + rel
    )

  def addPatternRelationships(rels: Seq[PatternRelationship]) =
    rels.foldLeft[QueryGraph](this)((qg, rel) => qg.addPatternRelationship(rel))

  def addShortestPath(shortestPath: ShortestPathPattern): QueryGraph = {
    val rel = shortestPath.rel
    copy (
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      shortestPathPatterns = shortestPathPatterns + shortestPath
    )
  }

  def addShortestPaths(shortestPaths: ShortestPathPattern*): QueryGraph = shortestPaths.foldLeft(this)((qg, p) => qg.addShortestPath(p))
  def addArgumentIds(newIds: Seq[IdName]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)
  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints ++ addedHints)

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)
  def withArgumentIds(newArgumentIds: Set[IdName]): QueryGraph =
    copy(argumentIds = newArgumentIds)

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toSeq))
  }

  def withOptionalMatches(optionalMatches: Seq[QueryGraph]): QueryGraph = {
    copy(optionalMatches = optionalMatches)
  }

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def knownLabelsOnNode(node: IdName): Seq[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Seq.empty)
      .flatMap(_.labels).toSeq

  def findRelationshipsEndingOn(id: IdName): Set[PatternRelationship] =
    patternRelationships.filter { r => r.left == id || r.right == id }

  def allPatternNodes: Set[IdName] =
    patternNodes ++ optionalMatches.flatMap(_.allPatternNodes)

  def coveredIds: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ argumentIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def allCoveredIds: Set[IdName] = {
    val optionalMatchIds = optionalMatches.flatMap(_.allCoveredIds)
    coveredIds ++ optionalMatchIds
  }

  val allHints: Set[Hint] =
    if (optionalMatches.isEmpty) hints else hints ++ optionalMatches.flatMap(_.allHints)

  def numHints = allHints.size

  def ++(other: QueryGraph): QueryGraph =
    QueryGraph(
      selections = selections ++ other.selections,
      patternNodes = patternNodes ++ other.patternNodes,
      patternRelationships = patternRelationships ++ other.patternRelationships,
      optionalMatches = optionalMatches ++ other.optionalMatches,
      argumentIds = argumentIds ++ other.argumentIds,
      hints = hints ++ other.hints,
      shortestPathPatterns = shortestPathPatterns ++ other.shortestPathPatterns
    )

  def isCoveredBy(other: QueryGraph): Boolean = {
    patternNodes.subsetOf(other.patternNodes) &&
      patternRelationships.subsetOf(other.patternRelationships) &&
      argumentIds.subsetOf(other.argumentIds) &&
      optionalMatches.toSet.subsetOf(other.optionalMatches.toSet) &&
      selections.predicates.subsetOf(other.selections.predicates) &&
      shortestPathPatterns.subsetOf(other.shortestPathPatterns)
  }

  def covers(other: QueryGraph): Boolean = other.isCoveredBy(this)

  def hasOptionalPatterns = optionalMatches.nonEmpty

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    patternNodes.collect { case node: IdName => node -> selections.labelsOnNode(node) }.toMap

  /**
   * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
   * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
   * connected query graphs.
   */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[IdName]
    patternNodes.toSeq.collect {
      case patternNode if !visited(patternNode) =>
        val qg = connectedComponentFor(patternNode, visited)
        val coveredIds = qg.coveredIds
        val predicates = selections.predicates.filter(_.dependencies.subsetOf(coveredIds))
        val arguments = argumentIds
        val filteredHints = hints.filter(h => coveredIds.contains(IdName(h.identifier.name)))
        val shortestPaths = shortestPathPatterns.filter {
          p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
        }
        qg.
          withSelections(Selections(predicates)).
          withArgumentIds(arguments).
          addHints(filteredHints).
          addShortestPaths(shortestPaths.toSeq: _*)
    }
  }

  def withoutPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy( patternRelationships = patternRelationships -- patterns )

  private def connectedComponentFor(startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    val nodesSolvedByArguments = patternNodes intersect argumentIds
    queue.enqueue(nodesSolvedByArguments.toSeq:_*)
    var qg = QueryGraph.empty.withArgumentIds(argumentIds)
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      qg = if (visited(node)) {
        qg
      } else {
        visited += node

        val filteredPatterns = patternRelationships.filter { rel =>
          rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
        }

        queue.enqueue(filteredPatterns.toSeq.map(_.otherSide(node)): _*)

        qg
          .addPatternNodes(node)
          .addPatternRelationships(filteredPatterns.toSeq)
      }
    }
    qg
  }

  // This is here to stop usage of copy from the outside
  private def copy(patternRelationships: Set[PatternRelationship] = patternRelationships,
                   patternNodes: Set[IdName] = patternNodes,
                   argumentIds: Set[IdName] = argumentIds,
                   selections: Selections = selections,
                   optionalMatches: Seq[QueryGraph] = optionalMatches,
                   hints: Set[Hint] = hints,
                   shortestPathPatterns: Set[ShortestPathPattern] = shortestPathPatterns) =
  QueryGraph(patternRelationships, patternNodes, argumentIds, selections, optionalMatches, hints, shortestPathPatterns)
}

object QueryGraph {
  val empty = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    import scala.math.Ordering.Implicits

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.coveredIds.toSeq.sorted(IdName.byName)
      val ys = y.coveredIds.toSeq.sorted(IdName.byName)
      Implicits.seqDerivedOrdering[Seq, IdName](IdName.byName).compare(xs, ys)
    }
  }
}
