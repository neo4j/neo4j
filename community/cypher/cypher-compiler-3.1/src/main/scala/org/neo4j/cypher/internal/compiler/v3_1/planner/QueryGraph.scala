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
package org.neo4j.cypher.internal.compiler.v3_1.planner

import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_1.ast._

import scala.collection.{GenTraversableOnce, mutable}


case class QueryGraph(patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: Vector[QueryGraph] = Vector.empty,
                      hints: Set[Hint] = Set.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty,
                      mutatingPatterns: Seq[MutatingPattern] = Seq.empty)
  extends UpdateGraph {

  def size = patternRelationships.size

  def isEmpty: Boolean = this == QueryGraph.empty

  def nonEmpty: Boolean = !isEmpty

  def mapSelections(f: Selections => Selections): QueryGraph =
    copy(selections = f(selections), optionalMatches = optionalMatches.map(_.mapSelections(f)))

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

  /*
  Includes not only pattern nodes in the read part of the query graph, but also pattern nodes from CREATE and MERGE
   */
  def allPatternNodes: Set[IdName] =
    patternNodes ++
    optionalMatches.flatMap(_.allPatternNodes) ++
    createNodePatterns.map(_.nodeName) ++
    mergeNodePatterns.map(_.createNodePattern.nodeName) ++
    mergeRelationshipPatterns.flatMap(_.createNodePatterns.map(_.nodeName))

  def allPatternRelationshipsRead: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationshipsRead)

  def allPatternNodesRead: Set[IdName] =
    patternNodes ++ optionalMatches.flatMap(_.allPatternNodesRead)

  def addShortestPaths(shortestPaths: ShortestPathPattern*): QueryGraph = shortestPaths.foldLeft(this)((qg, p) => qg.addShortestPath(p))
  def addArgumentId(newId: IdName): QueryGraph = copy(argumentIds = argumentIds + newId)
  def addArgumentIds(newIds: Seq[IdName]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)
  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints ++ addedHints)

  def withoutHints(hintsToIgnore: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints -- hintsToIgnore)

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)
  def withArgumentIds(newArgumentIds: Set[IdName]): QueryGraph =
    copy(argumentIds = newArgumentIds)

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toSeq))
  }

  def withOptionalMatches(optionalMatches: Vector[QueryGraph]): QueryGraph = {
    copy(optionalMatches = optionalMatches)
  }

  def withMergeMatch(matchGraph: QueryGraph): QueryGraph = {
    if (mergeQueryGraph.isEmpty) throw new IllegalArgumentException("Don't add a merge to this non-merge QG")

    // NOTE: Merge can only contain one mutating pattern
    assert(mutatingPatterns.length == 1)
    val newMutatingPattern = mutatingPatterns.collectFirst {
      case p: MergeNodePattern => p.copy(matchGraph = matchGraph)
      case p: MergeRelationshipPattern => p.copy(matchGraph = matchGraph)
    }.get

    copy(argumentIds = matchGraph.argumentIds, mutatingPatterns = Seq(newMutatingPattern))
  }

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def knownProperties(idName: IdName): Set[Property] =
    selections.propertyPredicatesForSet.getOrElse(idName, Set.empty)

  private def knownLabelsOnNode(node: IdName): Set[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Set.empty)
      .flatMap(_.labels)

  def allKnownLabelsOnNode(node: IdName): Set[LabelName] =
    knownLabelsOnNode(node) ++ optionalMatches.flatMap(_.allKnownLabelsOnNode(node))

  def allKnownPropertiesOnIdentifier(idName: IdName): Set[Property] =
    knownProperties(idName) ++ optionalMatches.flatMap(_.allKnownPropertiesOnIdentifier(idName))

  def allKnownNodeProperties: Set[Property] = {
    val matchedNodes = patternNodes ++ patternRelationships.flatMap(r => Set(r.nodes._1, r.nodes._2))
    matchedNodes.flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownNodeProperties)
  }

  def allKnownRelProperties: Set[Property] =
    patternRelationships.map(_.name).flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownRelProperties)


  def findRelationshipsEndingOn(id: IdName): Set[PatternRelationship] =
    patternRelationships.filter { r => r.left == id || r.right == id }

  def allPatternRelationships: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationships) ++
    // Recursively add relationships from the merge-read-part
    mergeNodePatterns.flatMap(_.matchGraph.allPatternRelationships) ++
    mergeRelationshipPatterns.flatMap(_.matchGraph.allPatternRelationships)

  def coveredIdsExceptArguments: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def coveredIds: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ argumentIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def allCoveredIds: Set[IdName] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    coveredIds ++ otherSymbols
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
      shortestPathPatterns = shortestPathPatterns ++ other.shortestPathPatterns,
      mutatingPatterns = mutatingPatterns ++ other.mutatingPatterns
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
        val predicates = selections.predicates.filter(_.dependencies.subsetOf(coveredIds ++ argumentIds))
        val filteredHints = hints.filter(h => h.variables.forall(variable => coveredIds.contains(IdName(variable.name))))
        val shortestPaths = shortestPathPatterns.filter {
          p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
        }
        qg.
          withSelections(Selections(predicates)).
          withArgumentIds(argumentIds).
          addHints(filteredHints).
          addShortestPaths(shortestPaths.toSeq: _*)
    }
  }

  def withoutPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy( patternRelationships = patternRelationships -- patterns )

  def joinHints: Set[UsingJoinHint] =
    hints.collect { case hint: UsingJoinHint => hint }

  private def connectedComponentFor(startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    var qg = QueryGraph.empty
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      if (!visited(node)) {
        visited += node

        val filteredPatterns = patternRelationships.filter { rel =>
          rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
        }

        val patternsWithSameName =
          patternRelationships.filterNot(filteredPatterns).filter { r => filteredPatterns.exists(_.name == r.name) }

        queue.enqueue(filteredPatterns.toSeq.map(_.otherSide(node)): _*)
        queue.enqueue(patternsWithSameName.toSeq.flatMap(r => Seq(r.left, r.right)): _*)

        val patternsInConnectedComponent = filteredPatterns ++ patternsWithSameName
        qg = qg
          .addPatternNodes(node)
          .addPatternRelationships(patternsInConnectedComponent.toSeq)

        val alreadyHaveArguments = qg.argumentIds.nonEmpty

        if (!alreadyHaveArguments && (relationshipPullsInArguments(qg.coveredIds) || predicatePullsInArguments(node))) {
          qg = qg.withArgumentIds(argumentIds)
          val nodesSolvedByArguments = patternNodes intersect qg.argumentIds
          queue.enqueue(nodesSolvedByArguments.toSeq: _*)
        }
      }
    }
    qg
  }

  private def relationshipPullsInArguments(coveredIds: Set[IdName]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: IdName) = selections.flatPredicates.exists {
    case p =>
      val deps = p.dependencies.map(IdName.fromVariable)
      deps(node) && (deps intersect argumentIds).nonEmpty
  }

  def containsReads: Boolean = {
    (patternNodes -- argumentIds).nonEmpty ||
    patternRelationships.nonEmpty ||
    selections.nonEmpty ||
    shortestPathPatterns.nonEmpty ||
    optionalMatches.nonEmpty ||
    containsMergeRecursive
  }

  def writeOnly = !containsReads && containsUpdates

  def addMutatingPatterns(patterns: MutatingPattern*): QueryGraph =
    copy(mutatingPatterns = mutatingPatterns ++ patterns)

  // This is here to stop usage of copy from the outside
  private def copy(patternRelationships: Set[PatternRelationship] = patternRelationships,
                   patternNodes: Set[IdName] = patternNodes,
                   argumentIds: Set[IdName] = argumentIds,
                   selections: Selections = selections,
                   optionalMatches: Vector[QueryGraph] = optionalMatches,
                   hints: Set[Hint] = hints,
                   shortestPathPatterns: Set[ShortestPathPattern] = shortestPathPatterns,
                   mutatingPatterns: Seq[MutatingPattern] = mutatingPatterns) =
  QueryGraph(patternRelationships, patternNodes, argumentIds, selections, optionalMatches, hints, shortestPathPatterns, mutatingPatterns)
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
