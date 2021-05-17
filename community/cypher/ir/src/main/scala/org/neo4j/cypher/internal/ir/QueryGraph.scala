/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.AllNameGenerators
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

import scala.collection.GenSet
import scala.collection.GenTraversableOnce
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.Implicits
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
                      hints: Set[Hint] = Set.empty,
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
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  lazy val allQGsWithLeafInfo: Seq[QgWithLeafInfo] = {
    val patternComprehensions = this.findByAllClass[PatternComprehension].toSet.map((e: PatternComprehension) => ExpressionConverters.asQueryGraph(e, e.dependencies.map(_.name), new AllNameGenerators))
    val patternExpressions = this.findByAllClass[PatternExpression].toSet.map((e: PatternExpression) => ExpressionConverters.asQueryGraph(e, e.dependencies.map(_.name), new AllNameGenerators))
    val allQgsWithLeafInfo = Seq(this) ++
      patternComprehensions ++
      patternExpressions
    allQgsWithLeafInfo.map(QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves) ++
      optionalMatches.flatMap(_.allQGsWithLeafInfo)
  }

  /**
   * Includes not only pattern nodes in the read part of the query graph, but also pattern nodes from CREATE and MERGE
   */
  def allPatternNodes: collection.Set[String] = {
    val nodes = mutable.Set[String]()
    collectAllPatternNodes(nodes.add)
    nodes
  }

  def collectAllPatternNodes(f: String => Unit): Unit = {
    patternNodes.foreach(f)
    optionalMatches.foreach(m => m.allPatternNodes.foreach(f))
    for {
      create <- createPatterns
      createNode <- create.nodes
    } {
      f(createNode.idName)
    }
    mergeNodePatterns.foreach(p => f(p.createNode.idName))
    mergeRelationshipPatterns.foreach(p => p.createNodes.foreach(pp => f(pp.idName)))
  }

  def allPatternRelationshipsRead: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationshipsRead) ++ shortestPathPatterns.map(_.rel)

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

  def removePredicates(predicates: Set[Predicate]): QueryGraph = {
    val newSelections = Selections(selections.predicates -- predicates)
    copy(selections = newSelections)
  }

  def addPredicates(outerScope: Set[String], predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates(outerScope)).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = {
    copy(hints = hints ++ addedHints)
  }

  def withoutHints(hintsToIgnore: GenSet[Hint]): QueryGraph = copy(
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

  def withHints(hints: Set[Hint]): QueryGraph = copy(hints = hints)

  def withPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patterns)

  def withPatternNodes(nodes: Set[String]): QueryGraph =
    copy(patternNodes = nodes)

  def knownProperties(idName: String): Set[PropertyKeyName] =
    selections.allPropertyPredicatesInvolving.getOrElse(idName, Set.empty).map(_.propertyKey)

  private def possibleLabelsOnNode(node: String): Set[LabelName] = {
    val label = selections
      .allHasLabelsInvolving.getOrElse(node, Set.empty)
      .flatMap(_.labels)
    val labelOrType = selections
      .allHasLabelsOrTypesInvolving.getOrElse(node, Set.empty)
      .flatMap(_.labelsOrTypes).map(lblOrType => LabelName(lblOrType.name)(lblOrType.position))
    label ++ labelOrType
  }

  private def possibleTypesOnRel(rel: String): Set[RelTypeName] = {
    val inlinedTypes = patternRelationships
      .find(_.name == rel)
      .toSet[PatternRelationship]
      .flatMap(_.types.toSet)

    val whereClauseTypes = selections
      .allHasTypesInvolving.getOrElse(rel, Set.empty)
      .flatMap(_.types)

    val whereClauseLabelOrTypes = selections
      .allHasLabelsOrTypesInvolving.getOrElse(rel, Set.empty)
      .flatMap(_.labelsOrTypes).map(lblOrType => RelTypeName(lblOrType.name)(lblOrType.position))

    inlinedTypes ++ whereClauseTypes ++ whereClauseLabelOrTypes
  }

  def allPossibleLabelsOnNode(node: String): Set[LabelName] =
    possibleLabelsOnNode(node) ++ optionalMatches.flatMap(_.allPossibleLabelsOnNode(node))

  def allPossibleTypesOnRel(rel: String): Set[RelTypeName] =
    possibleTypesOnRel(rel) ++ optionalMatches.flatMap(_.allPossibleTypesOnRel(rel))

  def allKnownPropertiesOnIdentifier(idName: String): Set[PropertyKeyName] =
    knownProperties(idName) ++ optionalMatches.flatMap(_.allKnownPropertiesOnIdentifier(idName))

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
    QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships) ++ argumentIds ++ shortestPathPatterns.flatMap(_.name)

  /**
   * All variables that are bound after this QG has been matched
   */
  def allCoveredIds: Set[String] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    idsWithoutOptionalMatchesOrUpdates ++ otherSymbols
  }

  def allHints: Set[Hint] =
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

  def patternNodeLabels: Map[String, Set[LabelName]] = {
    // Node label predicates are extracted from the pattern nodes to predicates in LabelPredicateNormalizer.
    // Therefore, we only need to look in selections.
    patternNodes.collect { case node: String => node -> selections.labelsOnNode(node) }.toMap
  }

  def patternRelationshipTypes: Map[String, RelTypeName] = {
    // Pattern relationship type predicates are inlined in PlannerQueryBuilder::inlineRelationshipTypePredicates().
    // Therefore, we don't need to look at predicates in selections.
    patternRelationships.collect { case PatternRelationship(name, _, _, Seq(relType), _) => name -> relType}.toMap
  }

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

  def joinHints: Set[UsingJoinHint] =
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
    (patternNodes.nonEmpty && (patternNodes -- argumentIds).nonEmpty) ||
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
    val stringifier = ExpressionStringifier(
      extension = new ExpressionStringifier.Extension {
        override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
          case pp: PartialPredicate[_] => s"partial(${ctx(pp.coveredPredicate)}, ${ctx(pp.coveringPredicate)})"
          case e                       => e.asCanonicalStringVal
        }
      },
      alwaysParens = false,
      alwaysBacktick = false,
      preferSingleQuotes = false,
      sensitiveParamsAsParams = false,
    )

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
    addSetIfNonEmpty(patternRelationships, "Rels", (_: PatternRelationship).toString)
    addSetIfNonEmptyS(argumentIds, "Arguments")
    addSetIfNonEmpty(selections.flatPredicates, "Predicates", (e: Expression) => stringifier.apply(e))
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

      patternRelationships == other.patternRelationships &&
        patternNodes == other.patternNodes &&
        argumentIds == other.argumentIds &&
        selections == other.selections &&
        optionalMatches.toSet == other.optionalMatches.toSet &&
        hints == other.hints &&
        shortestPathPatterns == other.shortestPathPatterns &&
        mutatingPatterns == other.mutatingPatterns

    case _ =>
      false
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[QueryGraph]

  override lazy val hashCode: Int = {
    ScalaRunTime._hashCode((patternRelationships, patternNodes, argumentIds, selections, optionalMatches.toSet, hints.groupBy(identity), shortestPathPatterns, mutatingPatterns))
  }

}

object QueryGraph {
  val empty = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]): Set[String] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      val ys = y.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      Implicits.seqDerivedOrdering[Seq, String].compare(xs, ys)
    }
  }

}
