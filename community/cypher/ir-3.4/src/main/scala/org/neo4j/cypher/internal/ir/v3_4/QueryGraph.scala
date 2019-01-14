/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import scala.collection.mutable.ArrayBuffer
import scala.collection.{GenSeq, GenTraversableOnce, mutable}
import scala.runtime.ScalaRunTime

/*
This is one of the core classes used during query planning. It represents the declarative query,
it contains no more information that the AST, but it contains data in a format that is easier
to consume by the planner. If you want to trace this back to the original query - one QueryGraph
represents all the MATCH, OPTIONAL MATCHes, and update clauses between two WITHs.
 */
case class QueryGraph(// !!! If you change anything here, make sure to update the equals method at the bottom of this class !!!
                      patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[String] = Set.empty,
                      argumentIds: Set[String] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: IndexedSeq[QueryGraph] = Vector.empty,
                      hints: Seq[Hint] = Seq.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty,
                      mutatingPatterns: IndexedSeq[MutatingPattern] = IndexedSeq.empty)
  extends UpdateGraph {

  /**
    * Dependencies from this QG to variables - from WHERE predicates and update clauses using expressions
    *
    * @return
    */
  def dependencies: Set[String] =
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

  def addPatternNodes(nodes: String*): QueryGraph =
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
  def allPatternNodes: collection.Set[String] = {
    val nodes = mutable.Set[String]()
    collectAllPatternNodes(nodes.add)
    nodes
  }

  def collectAllPatternNodes(f: (String) => Unit): Unit = {
    patternNodes.foreach(f)
    optionalMatches.foreach(m => m.allPatternNodes.foreach(f))
    createNodePatterns.foreach(p => f(p.nodeName))
    mergeNodePatterns.foreach(p => f(p.createNodePattern.nodeName))
    mergeRelationshipPatterns.foreach(p => p.createNodePatterns.foreach(pp => f(pp.nodeName)))
  }

  def allPatternRelationshipsRead: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationshipsRead)

  def allPatternNodesRead: Set[String] =
    patternNodes ++ optionalMatches.flatMap(_.allPatternNodesRead)

  def addShortestPaths(shortestPaths: ShortestPathPattern*): QueryGraph = shortestPaths.foldLeft(this)((qg, p) => qg.addShortestPath(p))

  def addArgumentId(newId: String): QueryGraph = copy(argumentIds = argumentIds + newId)

  def addArgumentIds(newIds: Seq[String]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)

  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = {
    copy(hints = hints ++ addedHints)
  }

  def withoutHints(hintsToIgnore: GenSeq[Hint]): QueryGraph = copy(
    hints = hints.diff(hintsToIgnore),
    optionalMatches = optionalMatches.map(_.withoutHints(hintsToIgnore)))

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)

  def withArgumentIds(newArgumentIds: Set[String]): QueryGraph =
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

    copy(argumentIds = matchGraph.argumentIds, mutatingPatterns = IndexedSeq(newMutatingPattern))
  }

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def withPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patterns)

  def withPatternNodes(nodes: Set[String]): QueryGraph =
    copy(patternNodes = nodes)

  def knownProperties(idName: String): Set[Property] =
    selections.propertyPredicatesForSet.getOrElse(idName, Set.empty)

  private def knownLabelsOnNode(node: String): Set[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Set.empty)
      .flatMap(_.labels)

  def allKnownLabelsOnNode(node: String): Set[LabelName] =
    knownLabelsOnNode(node) ++ optionalMatches.flatMap(_.allKnownLabelsOnNode(node))

  def allKnownPropertiesOnIdentifier(idName: String): Set[Property] =
    knownProperties(idName) ++ optionalMatches.flatMap(_.allKnownPropertiesOnIdentifier(idName))

  def allKnownNodeProperties: Set[Property] = {
    val matchedNodes = patternNodes ++ patternRelationships.flatMap(r => Set(r.nodes._1, r.nodes._2))
    matchedNodes.flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownNodeProperties)
  }

  def allKnownRelProperties: Set[Property] =
    patternRelationships.map(_.name).flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownRelProperties)

  def findRelationshipsEndingOn(id: String): Set[PatternRelationship] =
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
  def idsWithoutOptionalMatchesOrUpdates: Set[String] =
    QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships) ++ argumentIds

  /**
    * All variables that are bound after this QG has been matched
     */
  def allCoveredIds: Set[String] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    idsWithoutOptionalMatchesOrUpdates ++ otherSymbols
  }

  def allHints: Seq[Hint] =
    hints ++ optionalMatches.flatMap(_.allHints)

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

  def patternNodeLabels: Map[String, Set[LabelName]] =
    patternNodes.collect { case node: String => node -> selections.labelsOnNode(node) }.toMap

  /**
    * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
    * Connected here means can be reached through a relationship pattern.
    * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
    * connected query graphs.
    */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[String]

    def createComponentQueryGraphStartingFrom(patternNode: String) = {
      val qg = connectedComponentFor(patternNode, visited)
      val coveredIds = qg.idsWithoutOptionalMatchesOrUpdates
      val shortestPaths = shortestPathPatterns.filter {
        p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
      }
      val shortestPathIds = shortestPaths.flatMap(p => Set(p.rel.name) ++ p.name)
      val allIds = coveredIds ++ argumentIds ++ shortestPathIds
      val predicates = selections.predicates.filter(_.dependencies.subsetOf(allIds))
      val filteredHints = hints.filter(h => h.variables.forall(variable => coveredIds.contains(variable.name)))
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

  def joinHints: Seq[UsingJoinHint] =
    hints.collect { case hint: UsingJoinHint => hint }


  private def connectedComponentFor(startNode: String, visited: mutable.Set[String]): QueryGraph = {
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

  private def argumentsOverLapsWith(coveredIds: Set[String]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: String) = selections.flatPredicates.exists { p =>
      val dependencies = p.dependencies.map(_.name)
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

  def addMutatingPatterns(pattern: MutatingPattern): QueryGraph = {
    val copyPatterns = new mutable.ArrayBuffer[MutatingPattern](mutatingPatterns.size + 1)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns.append(pattern)

    copy(mutatingPatterns = copyPatterns)
  }

  def addMutatingPatterns(patterns: Seq[MutatingPattern]): QueryGraph = {
    val copyPatterns = new ArrayBuffer[MutatingPattern](patterns.size)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns.appendAll(patterns)
    copy(mutatingPatterns = copyPatterns)
  }

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


      val name = p.name
      val length = p.length match {
        case SimplePatternLength => ""
        case VarPatternLength(1, None) => "*"
        case VarPatternLength(x, None) => s"*$x.."
        case VarPatternLength(min, Some(max)) => s"*$min..$max"
      }

      val relInfo = s"$name$types$length"

      val left = s"(${p.nodes._1})-$lArrow-"
      val right = s"-$rArrow-(${p.nodes._2})"

      if (relInfo.isEmpty)
        left + right
      else
        s"$left[$relInfo]$right"
    }

    def addSetIfNonEmptyS(s: Iterable[String], name: String): Unit  = addSetIfNonEmpty(s, name, (x:String) => x)
    def addSetIfNonEmpty[T](s: Iterable[T], name: String, f: T => String): Unit = {
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

    addSetIfNonEmptyS(patternNodes, "Nodes")
    addSetIfNonEmpty(patternRelationships, "Rels", prettyPattern)
    addSetIfNonEmptyS(argumentIds, "Arguments")
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
      // ignore order, but differentiate between different counts of the same element
        hints.groupBy(identity) == other.hints.groupBy(identity) &&
        shortestPathPatterns == other.shortestPathPatterns &&
        mutatingPatterns == other.mutatingPatterns

    case _ =>
      false
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[QueryGraph]

  override lazy val hashCode: Int = {
    val optionals = if(optionalMatches.nonEmpty && containsIndependentOptionalMatches)
      optionalMatches.toSet
    else
      optionalMatches

    ScalaRunTime._hashCode((patternRelationships, patternNodes, argumentIds, selections, optionals, hints.groupBy(identity), shortestPathPatterns, mutatingPatterns))
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

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]): Set[String] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    import scala.math.Ordering.Implicits

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      val ys = y.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      Implicits.seqDerivedOrdering[Seq, String].compare(xs, ys)
    }
  }

}
