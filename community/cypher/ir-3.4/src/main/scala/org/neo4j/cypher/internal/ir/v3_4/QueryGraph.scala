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
package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ir.v3_4.helpers.ExpressionConverters._
import org.neo4j.cypher.internal.v3_4.expressions._

import scala.collection.{GenTraversableOnce, mutable}
import scala.runtime.ScalaRunTime

/*
This is one of the core classes used during query planning. It represents the declarative query,
it contains no more information that the AST, but it contains data in a format that is easier
to consume by the planner. If you want to trace this back to the original query - one QueryGraph
represents all the MATCH, OPTIONAL MATCHes, and update clauses between two WITHs.
 */
case class QueryGraph(// !!! If you change anything here, make sure to update the equals method at the bottom of this class !!!
                      patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: IndexedSeq[QueryGraph] = Vector.empty,
                      hints: Set[Hint] = Set.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty,
                      mutatingPatterns: Seq[MutatingPattern] = Seq.empty)
  extends UpdateGraph {

  /**
    * Dependencies from this QG to variables - from WHERE predicates and update clauses using expressions
    *
    * @return
    */
  def dependencies: Set[IdName] =
    optionalMatches.flatMap(_.dependencies).toSet ++
      selections.predicates.flatMap(_.dependencies) ++
      mutatingPatterns.flatMap(_.dependencies) ++
      argumentIds

  /**
    * The size of a QG is defined as the number of pattern relationships that are introduced
    */
  def size: Int = patternRelationships.size

  def isEmpty: Boolean = this == QueryGraph.empty

  def nonEmpty: Boolean = !isEmpty

  def mapSelections(f: Selections => Selections): QueryGraph =
    copy(selections = f(selections), optionalMatches = optionalMatches.map(_.mapSelections(f)))

  def addPatternNodes(nodes: IdName*): QueryGraph =
    copy(patternNodes = patternNodes ++ nodes)

  def addPatternRelationship(rel: PatternRelationship): QueryGraph =
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      patternRelationships = patternRelationships + rel
    )

  def addPatternRelationships(rels: Seq[PatternRelationship]): QueryGraph =
    rels.foldLeft[QueryGraph](this)((qg, rel) => qg.addPatternRelationship(rel))

  def addShortestPath(shortestPath: ShortestPathPattern): QueryGraph = {
    val rel = shortestPath.rel
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      shortestPathPatterns = shortestPathPatterns + shortestPath
    )
  }

  /**
    * Includes not only pattern nodes in the read part of the query graph, but also pattern nodes from CREATE and MERGE
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

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = {
    copy(hints = hints ++ addedHints)
  }

  def withoutHints(hintsToIgnore: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints -- hintsToIgnore)

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)

  def withArgumentIds(newArgumentIds: Set[IdName]): QueryGraph =
    copy(argumentIds = newArgumentIds)

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toIndexedSeq))
  }

  def withOptionalMatches(optionalMatches: IndexedSeq[QueryGraph]): QueryGraph = {
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

  def withPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patterns)

  def withPatternNodes(nodes: Set[IdName]): QueryGraph =
    copy(patternNodes = nodes)

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

  /**
    * Variables are bound after matching this QG, but before optional
    * matches and updates have been applied
    */
  def idsWithoutOptionalMatchesOrUpdates: Set[IdName] =
    QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships) ++ argumentIds

  /**
    * All variables that are bound after this QG has been matched
     */
  def allCoveredIds: Set[IdName] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    idsWithoutOptionalMatchesOrUpdates ++ otherSymbols
  }

  def allHints: Set[Hint] =
    hints ++ optionalMatches.flatMap(_.allHints)

  def numHints: Int = allHints.size

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

  def hasOptionalPatterns: Boolean = optionalMatches.nonEmpty

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    patternNodes.collect { case node: IdName => node -> selections.labelsOnNode(node) }.toMap

  /**
    * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
    * Connected here means can be reached through a relationship pattern.
    * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
    * connected query graphs.
    */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[IdName]

    def createComponentQueryGraphStartingFrom(patternNode: IdName) = {
      val qg = connectedComponentFor(patternNode, visited)
      val coveredIds = qg.idsWithoutOptionalMatchesOrUpdates
      val shortestPaths = shortestPathPatterns.filter {
        p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
      }
      val shortestPathIds = shortestPaths.flatMap(p => Set(p.rel.name) ++ p.name)
      val allIds = coveredIds ++ argumentIds ++ shortestPathIds
      val predicates = selections.predicates.filter(_.dependencies.subsetOf(allIds))
      val filteredHints = hints.filter(h => h.variables.forall(variable => coveredIds.contains(IdName(variable.name))))
      qg.
        withSelections(Selections(predicates)).
        withArgumentIds(argumentIds).
        addHints(filteredHints).
        addShortestPaths(shortestPaths.toIndexedSeq: _*)
    }

    /*
    We want the components that have patterns connected to arguments to be planned first, so we do not pull in arguments
    to other components by mistake
     */
    val argumentComponents = (patternNodes intersect argumentIds).toIndexedSeq.collect {
      case patternNode if !visited(patternNode) =>
        createComponentQueryGraphStartingFrom(patternNode)
    }

    val rest = patternNodes.toIndexedSeq.collect {
      case patternNode if !visited(patternNode) =>
        createComponentQueryGraphStartingFrom(patternNode)
    }

    argumentComponents ++ rest
  }

  def withoutPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patternRelationships -- patterns)

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

        queue.enqueue(filteredPatterns.toIndexedSeq.map(_.otherSide(node)): _*)
        queue.enqueue(patternsWithSameName.toIndexedSeq.flatMap(r => Seq(r.left, r.right)): _*)

        val patternsInConnectedComponent = filteredPatterns ++ patternsWithSameName
        qg = qg
          .addPatternNodes(node)
          .addPatternRelationships(patternsInConnectedComponent.toIndexedSeq)

        val alreadyHaveArguments = qg.argumentIds.nonEmpty

        if (!alreadyHaveArguments && (argumentsOverLapsWith(qg.idsWithoutOptionalMatchesOrUpdates) || predicatePullsInArguments(node))) {
          qg = qg.withArgumentIds(argumentIds)
          val nodesSolvedByArguments = patternNodes intersect qg.argumentIds
          queue.enqueue(nodesSolvedByArguments.toIndexedSeq: _*)
        }
      }
    }
    qg
  }

  private def argumentsOverLapsWith(coveredIds: Set[IdName]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: IdName) = selections.flatPredicates.exists { p =>
      val dependencies = p.dependencies.map(IdName.fromVariable)
      dependencies(node) && (dependencies intersect argumentIds).nonEmpty
  }

  def containsReads: Boolean = {
    (patternNodes -- argumentIds).nonEmpty ||
      patternRelationships.nonEmpty ||
      selections.nonEmpty ||
      shortestPathPatterns.nonEmpty ||
      optionalMatches.nonEmpty ||
      containsMergeRecursive
  }

  def writeOnly: Boolean = !containsReads && containsUpdates

  def addMutatingPatterns(patterns: MutatingPattern*): QueryGraph =
    copy(mutatingPatterns = mutatingPatterns ++ patterns)

  override def toString: String = {
    var added = false
    val builder = new StringBuilder("QueryGraph {")
    val stringifier = ExpressionStringifier()

    def prettyPattern(p: PatternRelationship): String = {
      val lArrow = if (p.dir == SemanticDirection.INCOMING) "<" else ""
      val rArrow = if (p.dir == SemanticDirection.OUTGOING) ">" else ""
      val types = if (p.types.isEmpty)
        ""
      else
        p.types.map(l => l.name).mkString(":", ":", "")


      val name = p.name.name
      val length = p.length match {
        case SimplePatternLength => ""
        case VarPatternLength(1, None) => "*"
        case VarPatternLength(x, None) => s"*$x.."
        case VarPatternLength(min, Some(max)) => s"*$min..$max"
      }

      val relInfo = s"$name$types$length"

      val left = s"(${p.nodes._1.name})-$lArrow-"
      val right = s"-$rArrow-(${p.nodes._2.name})"

      if (relInfo.isEmpty)
        left + right
      else
        s"$left[$relInfo]$right"
    }

    def addSetIfNonEmpty[T](s: Iterable[T], name: String, f: T => String) = {
      if (s.nonEmpty) {
        if(added)
          builder.append(", ")
        else
          added = true

        val sortedInput = if(s.isInstanceOf[Set[_]]) s.map(x => f(x)).toSeq.sorted else s.map(f)
        builder.
          append(s"$name: ").
          append(sortedInput.mkString("['", "', '", "']"))
      }
    }

    addSetIfNonEmpty(patternNodes, "Nodes", (_: IdName).name)
    addSetIfNonEmpty(patternRelationships, "Rels", prettyPattern)
    addSetIfNonEmpty(argumentIds, "Arguments", (_: IdName).name)
    addSetIfNonEmpty(selections.flatPredicates, "Predicates", stringifier.apply)
    addSetIfNonEmpty(shortestPathPatterns, "Shortest paths", (_: ShortestPathPattern).toString)
    addSetIfNonEmpty(optionalMatches, "Optional Matches: ", (_: QueryGraph).toString)
    addSetIfNonEmpty(hints, "Hints", (_: Hint).toString)

    builder.append("}")
    builder.toString()
  }

  /**
    * We have to do this special treatment of QG to avoid problems when checking that the produced plan actually
    * solves what we set out to solve. In some rare circumstances, we'll get a few optional matches that are independent of each other.
    *
    * Given the way our planner works, it can unpredictably plan these optional matches in different orders, which leads to an exception being thrown when
    * checking that the correct query has been solved.
    */
  override def equals(in: scala.Any): Boolean = in match {
    case other: QueryGraph if other canEqual this =>

      val optionals = if (optionalMatches.isEmpty) {
        true
      } else {
        compareOptionalMatches(other)
      }

      patternRelationships == other.patternRelationships &&
        patternNodes == other.patternNodes &&
        argumentIds == other.argumentIds &&
        selections == other.selections &&
        optionals &&
        hints == other.hints &&
        shortestPathPatterns == other.shortestPathPatterns &&
        mutatingPatterns == other.mutatingPatterns

    case _ =>
      false
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[QueryGraph]

  override def hashCode(): Int = {
    val optionals = if(optionalMatches.nonEmpty && containsIndependentOptionalMatches)
      optionalMatches.toSet
    else
      optionalMatches

    ScalaRunTime._hashCode((patternRelationships, patternNodes, argumentIds, selections, optionals, hints, shortestPathPatterns, mutatingPatterns))
  }

  private lazy val containsIndependentOptionalMatches = {
    val nonOptional = idsWithoutOptionalMatchesOrUpdates -- argumentIds

    val result = this.optionalMatches.foldLeft(false) {
      case (acc, oqg) =>
        acc || (oqg.dependencies -- nonOptional).nonEmpty
    }

    result
  }

  private def compareOptionalMatches(other: QueryGraph) = {
    if (containsIndependentOptionalMatches) {
      optionalMatches.toSet == other.optionalMatches.toSet
    } else
      optionalMatches == other.optionalMatches
  }
}

object QueryGraph {
  val empty = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]): Set[IdName] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    import scala.math.Ordering.Implicits

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted(IdName.byName)
      val ys = y.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted(IdName.byName)
      Implicits.seqDerivedOrdering[Seq, IdName](IdName.byName).compare(xs, ys)
    }
  }

}
