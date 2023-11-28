/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.Implicits
import scala.runtime.ScalaRunTime

/**
 * This is one of the core classes used during query planning. It represents the declarative query,
 * it contains no more information than the AST, but it contains data in a format that is easier
 * to consume by the planner. If you want to trace this back to the original query - one QueryGraph
 * represents all the MATCH, OPTIONAL MATCHes, and update clauses between two WITHs.
 *
 * A query graph has these contracts. All methods in this class that return a QueryGraph must uphold these contracts.
 * copy is hidden from the outside to avoid the creation of QueryGraphs not adhering to these contracts.
 * apply is overridden to avoid the creation of QueryGraphs not adhering to these contracts.
 * {{{
 *   qg.nodeConnections.flatMap(_.boundaryNodesSet).subsetOf(qg.patternNodes)
 *
 *   qg.shortestRelationshipPatterns.flatMap(_.rel.boundaryNodesSet).subsetOf(qg.patternNodes)
 * }}}
 *
 * @param shortestRelationshipPatterns shortest path patterns coming from shortestPath() or allShortestPaths(), made of a single var-length relationship and its endpoints.
 * @param selectivePathPatterns        path selector such as ANY k, SHORTEST k, or SHORTEST k GROUPS, applied to a path pattern introduced as part of Graph Pattern Matching.
 * @param patternNodes                 unconditional singleton pattern nodes excluding strict interior pattern nodes of selective path patterns.
 *                                     These can be connected to each other via node connections, creating connected components, and can potentially be used for node leaf plans.
 */
final case class QueryGraph private (
  patternRelationships: Set[PatternRelationship] = Set.empty,
  quantifiedPathPatterns: Set[QuantifiedPathPattern] = Set.empty,
  patternNodes: Set[String] = Set.empty,
  argumentIds: Set[String] = Set.empty,
  selections: Selections = Selections(),
  optionalMatches: IndexedSeq[QueryGraph] = Vector.empty,
  hints: Set[Hint] = Set.empty,
  shortestRelationshipPatterns: Set[ShortestRelationshipPattern] = Set.empty,
  mutatingPatterns: IndexedSeq[MutatingPattern] = IndexedSeq.empty,
  selectivePathPatterns: Set[SelectivePathPattern] = Set.empty
  // !!! If you change anything here, make sure to update the equals, ++ and hashCode methods at the bottom of this class !!!
) extends UpdateGraph {

  // This is here to stop usage of copy from the outside
  protected def copy(
    patternRelationships: Set[PatternRelationship] = patternRelationships,
    quantifiedPathPatterns: Set[QuantifiedPathPattern] = quantifiedPathPatterns,
    patternNodes: Set[String] = patternNodes,
    argumentIds: Set[String] = argumentIds,
    selections: Selections = selections,
    optionalMatches: IndexedSeq[QueryGraph] = optionalMatches,
    hints: Set[Hint] = hints,
    shortestRelationshipPatterns: Set[ShortestRelationshipPattern] = shortestRelationshipPatterns,
    mutatingPatterns: IndexedSeq[MutatingPattern] = mutatingPatterns,
    selectivePathPatterns: Set[SelectivePathPattern] = selectivePathPatterns
  ): QueryGraph =
    new QueryGraph(
      patternRelationships,
      quantifiedPathPatterns,
      patternNodes,
      argumentIds,
      selections,
      optionalMatches,
      hints,
      shortestRelationshipPatterns,
      mutatingPatterns,
      selectivePathPatterns
    )

  val nodeConnections: Set[NodeConnection] = Set.empty[NodeConnection] ++
    patternRelationships ++
    quantifiedPathPatterns ++
    selectivePathPatterns

  /**
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  lazy val allQGsWithLeafInfo: Seq[QgWithLeafInfo] = {
    val iRExpressions: Seq[QgWithLeafInfo] = this.folder.findAllByClass[IRExpression].flatMap((e: IRExpression) =>
      e.query.allQGsWithLeafInfo
    )
    QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves(this) +:
      (iRExpressions ++
        optionalMatches.flatMap(_.allQGsWithLeafInfo))
  }

  // -----------------
  // Override elements
  // -----------------

  def withPatternNodes(nodes: Set[String]): QueryGraph = copy(patternNodes = nodes)

  def withArgumentIds(newArgumentIds: Set[String]): QueryGraph = copy(argumentIds = newArgumentIds)

  def withOptionalMatches(optionalMatches: IndexedSeq[QueryGraph]): QueryGraph = copy(optionalMatches = optionalMatches)

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def withHints(hints: Set[Hint]): QueryGraph = copy(hints = hints)

  /**
   * Sets both patternNodes and patternRelationships from this pattern relationship. Compare with `addPatternRelationship`.
   * Note that other patternNodes and patternRelationships get removed.
   *
   * @param pattern the relationship defining the pattern of this query graph
   */
  def withPattern(pattern: PatternRelationship): QueryGraph =
    copy(
      patternNodes = pattern.boundaryNodesSet.map(_.name),
      patternRelationships = Set(pattern)
    )

  /**
   * Note that pattern nodes of these patterns are added, too.
   * Other pattern nodes are not removed.
   */
  def withPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(
      patternNodes = patternNodes ++ patterns.flatMap(_.boundaryNodesSet).map(_.name),
      patternRelationships = patterns
    )

  /**
   * Note that pattern nodes of these patterns are added, too.
   * Other pattern nodes are not removed.
   */
  def withQuantifiedPathPatterns(patterns: Set[QuantifiedPathPattern]): QueryGraph =
    copy(
      patternNodes = patternNodes ++ patterns.flatMap(_.boundaryNodesSet).map(_.name),
      quantifiedPathPatterns = patterns
    )

  def withMutatingPattern(mutatingPatterns: IndexedSeq[MutatingPattern]): QueryGraph =
    copy(mutatingPatterns = mutatingPatterns)

  // ------------
  // Add elements
  // ------------

  def addPathPatterns(pathPatterns: PathPatterns): QueryGraph =
    pathPatterns.pathPatterns.foldLeft(this) {
      case (qg, pathPattern) => qg.addPathPattern(pathPattern)
    }

  def addPathPattern(pathPattern: PathPattern): QueryGraph =
    pathPattern match {
      case exhaustivePathPattern: ExhaustivePathPattern[_]   => addExhaustivePathPattern(exhaustivePathPattern)
      case selectivePathPattern: SelectivePathPattern        => addSelectivePathPattern(selectivePathPattern)
      case shortestRelationship: ShortestRelationshipPattern => addShortestRelationship(shortestRelationship)
    }

  private def addExhaustivePathPattern(pathPattern: ExhaustivePathPattern[_]): QueryGraph =
    pathPattern match {
      case ExhaustivePathPattern.SingleNode(name)             => addPatternNodes(name.name)
      case ExhaustivePathPattern.NodeConnections(connections) => addNodeConnections(connections.toIterable)
    }

  def addPatternNodes(nodes: String*): QueryGraph =
    copy(patternNodes = patternNodes ++ nodes)

  def addPatternRelationship(rel: PatternRelationship): QueryGraph =
    copy(
      patternNodes = patternNodes ++ rel.boundaryNodesSet.map(_.name),
      patternRelationships = patternRelationships + rel
    )

  def addNodeConnection(connection: NodeConnection): QueryGraph = {
    connection match {
      case patternRelationship: PatternRelationship => addPatternRelationship(patternRelationship)
      case qpp: QuantifiedPathPattern               => addQuantifiedPathPattern(qpp)
      case spp: SelectivePathPattern                => addSelectivePathPattern(spp)
    }
  }

  def addNodeConnections(connections: Iterable[NodeConnection]): QueryGraph =
    connections.foldLeft(this) {
      case (qg, connection) => qg.addNodeConnection(connection)
    }

  def addPatternRelationships(rels: Set[PatternRelationship]): QueryGraph =
    rels.foldLeft[QueryGraph](this)((qg, rel) => qg.addPatternRelationship(rel))

  def addQuantifiedPathPattern(pattern: QuantifiedPathPattern): QueryGraph =
    copy(
      patternNodes = patternNodes ++ pattern.boundaryNodesSet.map(_.name),
      quantifiedPathPatterns = quantifiedPathPatterns + pattern
    )

  def addShortestRelationship(shortestRelationship: ShortestRelationshipPattern): QueryGraph = {
    val rel = shortestRelationship.rel
    copy(
      patternNodes = patternNodes ++ rel.boundaryNodesSet.map(_.name),
      shortestRelationshipPatterns = shortestRelationshipPatterns + shortestRelationship
    )
  }

  def addShortestRelationships(shortestRelationships: ShortestRelationshipPattern*): QueryGraph =
    shortestRelationships.foldLeft(this)((qg, p) => qg.addShortestRelationship(p))

  /**
   * Returns a copy of the query graph, with an additional selective path pattern added.
   */
  def addSelectivePathPattern(selectivePathPattern: SelectivePathPattern): QueryGraph =
    copy(
      patternNodes = patternNodes ++ selectivePathPattern.boundaryNodesSet.map(_.name),
      selectivePathPatterns = selectivePathPatterns + selectivePathPattern
    )

  def addArgumentId(newId: String): QueryGraph = copy(argumentIds = argumentIds + newId)

  def addArgumentIds(newIds: Iterable[String]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)

  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addPredicates(predicates: Set[Predicate]): QueryGraph = {
    val newSelections = Selections(selections.predicates ++ predicates)
    copy(selections = newSelections)
  }

  def addPredicates(outerScope: Set[String], predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates(outerScope)).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: IterableOnce[Hint]): QueryGraph = {
    copy(hints = hints ++ addedHints)
  }

  def addOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toIndexedSeq))
  }

  def addMutatingPatterns(pattern: MutatingPattern): QueryGraph = {
    val copyPatterns = new mutable.ArrayBuffer[MutatingPattern](mutatingPatterns.size + 1)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns += pattern

    copy(mutatingPatterns = copyPatterns.toIndexedSeq)
  }

  def addMutatingPatterns(patterns: Seq[MutatingPattern]): QueryGraph = {
    val copyPatterns = new ArrayBuffer[MutatingPattern](patterns.size)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns.appendAll(patterns)
    copy(mutatingPatterns = copyPatterns.toIndexedSeq)
  }

  // ---------------
  // Remove elements
  // ---------------

  def removeArgumentIds(idsToRemove: Iterable[String]): QueryGraph = copy(argumentIds = argumentIds -- idsToRemove)

  def removePredicates(predicates: Set[Predicate]): QueryGraph = {
    val newSelections = Selections(selections.predicates -- predicates)
    copy(selections = newSelections)
  }

  def removeHints(hintsToIgnore: Set[Hint]): QueryGraph = copy(
    hints = hints.diff(hintsToIgnore),
    optionalMatches = optionalMatches.map(_.removeHints(hintsToIgnore))
  )

  def removeArguments(): QueryGraph = withArgumentIds(Set.empty)

  /**
   * Removes a pattern relationship.
   * Note that this method does not remove the boundary nodes from pattern nodes.
   */
  def removePatternRelationship(pattern: PatternRelationship): QueryGraph =
    copy(patternRelationships = patternRelationships - pattern)

  // ----------
  // Other defs
  // ----------

  /**
   * Dependencies from this QG to variables - from WHERE predicates and update clauses using expressions
   */
  def dependencies: Set[String] =
    optionalMatches.flatMap(_.dependencies).toSet ++
      selections.predicates.flatMap(_.dependencies) ++
      mutatingPatterns.flatMap(_.dependencies.map(_.name)) ++
      quantifiedPathPatterns.flatMap(_.dependencies).map(_.name) ++
      selectivePathPatterns.flatMap(_.dependencies).map(_.name) ++
      argumentIds

  /**
   * The size of a QG is defined as the number of node connections that are introduced
   */
  def size: Int = nodeConnections.size

  def isEmpty: Boolean = this == QueryGraph.empty

  def nonEmpty: Boolean = !isEmpty

  def mapSelections(f: Selections => Selections): QueryGraph =
    copy(
      selections = f(selections),
      optionalMatches = optionalMatches.map(_.mapSelections(f)),
      quantifiedPathPatterns = quantifiedPathPatterns.map(qpp => qpp.copy(selections = f(qpp.selections))),
      selectivePathPatterns = selectivePathPatterns.map(spp => spp.copy(selections = f(spp.selections)))
    )

  /**
   * All unconditional singleton nodes of this query graph.
   * This includes not only the MATCH but also pattern nodes from CREATE and MERGE.
   */
  def allPatternNodes: collection.Set[String] = {
    val nodes = mutable.Set[String]()
    collectAllPatternNodes(nodes.add)
    nodes
  }

  private def collectAllPatternNodes(f: String => Unit): Unit = {
    patternNodes.foreach(f)
    selectivePathPatterns.foreach(_.pathPattern.connections.foreach(_.boundaryNodesSet.map(_.name).foreach(f)))
    optionalMatches.foreach(m => m.allPatternNodes.foreach(f))
    for {
      create <- createPatterns
      createNode <- create.nodes
    } {
      f(createNode.variable.name)
    }
    mergeNodePatterns.foreach(p => f(p.createNode.variable.name))
    mergeRelationshipPatterns.foreach(p => p.createNodes.foreach(pp => f(pp.variable.name)))
  }

  def allPatternRelationshipsRead: Set[PatternRelationship] =
    patternRelationships ++
      optionalMatches.flatMap(_.allPatternRelationshipsRead) ++
      shortestRelationshipPatterns.map(_.rel) ++
      quantifiedPathPatterns.flatMap(_.asQueryGraph.allPatternRelationshipsRead) ++
      selectivePathPatterns.flatMap(_.asQueryGraph.allPatternRelationshipsRead)

  def allPatternNodesRead: Set[String] =
    patternNodes ++
      optionalMatches.flatMap(_.allPatternNodesRead) ++
      quantifiedPathPatterns.flatMap(_.asQueryGraph.allPatternNodesRead) ++
      selectivePathPatterns.flatMap(_.asQueryGraph.allPatternNodesRead)

  private def knownProperties(idName: String): Set[PropertyKeyName] =
    selections.allPropertyPredicatesInvolving.getOrElse(idName, Set.empty).map(_.propertyKey)

  private def possibleLabelsOnNode(node: String): Set[LabelName] = {
    val label = selections
      .allHasLabelsInvolving.getOrElse(node, Set.empty)
      .flatMap(_.labels)
    val labelOrType = selections
      .allHasLabelsOrTypesInvolving.getOrElse(node, Set.empty)
      .flatMap(_.labelsOrTypes).map(_.asLabelName)
    label ++ labelOrType
  }

  def inlinedRelTypes(rel: String): Set[RelTypeName] = {
    patternRelationships
      .find(_.variable.name == rel)
      .toSet[PatternRelationship]
      .flatMap(_.types.toSet)
  }

  private def possibleTypesOnRel(rel: String): Set[RelTypeName] = {
    val whereClauseTypes = selections
      .allHasTypesInvolving.getOrElse(rel, Set.empty)
      .flatMap(_.types)

    val whereClauseLabelOrTypes = selections
      .allHasLabelsOrTypesInvolving.getOrElse(rel, Set.empty)
      .flatMap(_.labelsOrTypes).map(lblOrType => RelTypeName(lblOrType.name)(lblOrType.position))

    inlinedRelTypes(rel) ++ whereClauseTypes ++ whereClauseLabelOrTypes
  }

  private def traverseAllQueryGraphs[A](f: QueryGraph => Set[A]): Set[A] =
    f(this) ++
      optionalMatches.flatMap(_.traverseAllQueryGraphs(f)) ++
      quantifiedPathPatterns.flatMap(_.asQueryGraph.traverseAllQueryGraphs(f)) ++
      selectivePathPatterns.flatMap(_.asQueryGraph.traverseAllQueryGraphs(f))

  def allPossibleLabelsOnNode(node: String): Set[LabelName] =
    traverseAllQueryGraphs(_.possibleLabelsOnNode(node))

  def allPossibleTypesOnRel(rel: String): Set[RelTypeName] =
    traverseAllQueryGraphs(_.possibleTypesOnRel(rel))

  def allKnownPropertiesOnIdentifier(idName: String): Set[PropertyKeyName] =
    traverseAllQueryGraphs(_.knownProperties(idName))

  def allSelections: Selections =
    Selections(traverseAllQueryGraphs(_.selections.predicates))

  def coveredIdsForPatterns: Set[String] = {
    val patternRelIds = nodeConnections.flatMap(_.coveredIds).map(_.name)
    patternNodes ++ patternRelIds
  }

  /**
   * Variables that are bound after matching this QG, but before optional
   * matches and updates have been applied
   */
  def idsWithoutOptionalMatchesOrUpdates: Set[String] =
    coveredIdsForPatterns ++
      argumentIds ++
      shortestRelationshipPatterns.flatMap(_.maybePathVar).map(_.name)

  /**
   * All variables that are bound after this QG has been matched
   */
  def allCoveredIds: Set[String] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds.map(_.name))
    idsWithoutOptionalMatchesOrUpdates ++ otherSymbols
  }

  def allHints: Set[Hint] =
    hints ++ optionalMatches.flatMap(_.allHints)

  def ++(other: QueryGraph): QueryGraph = {
    other match {
      case QueryGraph(
          otherPatternRelationships,
          otherQuantifiedPathPatterns,
          otherPatternNodes,
          otherArgumentIds,
          otherSelections,
          otherOptionalMatches,
          otherHints,
          otherShortestRelationshipPatterns,
          otherMutatingPatterns,
          otherSelectivePathPatterns
        ) =>
        QueryGraph(
          selections = selections ++ otherSelections,
          patternNodes = patternNodes ++ otherPatternNodes,
          quantifiedPathPatterns = quantifiedPathPatterns ++ otherQuantifiedPathPatterns,
          patternRelationships = patternRelationships ++ otherPatternRelationships,
          optionalMatches = optionalMatches ++ otherOptionalMatches,
          argumentIds = argumentIds ++ otherArgumentIds,
          hints = hints ++ otherHints,
          shortestRelationshipPatterns = shortestRelationshipPatterns ++ otherShortestRelationshipPatterns,
          mutatingPatterns = mutatingPatterns ++ otherMutatingPatterns,
          selectivePathPatterns = selectivePathPatterns ++ otherSelectivePathPatterns
        )
    }
  }

  def hasOptionalPatterns: Boolean = optionalMatches.nonEmpty

  def patternNodeLabels: Map[String, Set[LabelName]] = {
    // Node label predicates are extracted from the pattern nodes to predicates in LabelPredicateNormalizer.
    // Therefore, we only need to look in selections.
    val labelsOnPatternNodes = patternNodes.toVector.map(node => node -> selections.labelsOnNode(node))

    val labelsOnSelectivePathPatternNodes =
      for {
        selectivePathPattern <- selectivePathPatterns.toVector
        connection <- selectivePathPattern.pathPattern.connections.toIterable
        node <- connection.boundaryNodesSet.map(_.name)
      } yield node -> (selections ++ selectivePathPattern.selections).labelsOnNode(node)

    (labelsOnPatternNodes ++ labelsOnSelectivePathPatternNodes).groupMapReduce(_._1)(_._2)(_.union(_))
  }

  def patternRelationshipTypes: Map[String, RelTypeName] = {
    // Pattern relationship type predicates are inlined in PlannerQueryBuilder::inlineRelationshipTypePredicates().
    // Therefore, we don't need to look at predicates in selections.
    val selectivePathPatternsRelationships =
      for {
        selectivePathPattern <- selectivePathPatterns.toVector
        connection <- selectivePathPattern.pathPattern.connections.toIterable
        relationship <- connection match {
          case patternRelationship: PatternRelationship => Some(patternRelationship)
          case _: QuantifiedPathPattern                 => None
        }
      } yield relationship

    val allRelationships = patternRelationships ++ selectivePathPatternsRelationships

    allRelationships.collect {
      case PatternRelationship(name, _, _, Seq(relType), _) => name.name -> relType
    }.toMap
  }

  /**
   * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
   * Connected here means can be reached through a relationship pattern.
   * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
   * connected query graphs.
   */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[String]

    val (predicatesWithLocalDependencies, strayPredicates) = selections.predicates.partition {
      p => (p.dependencies -- argumentIds).nonEmpty
    }

    def createComponentQueryGraphStartingFrom(patternNode: String) = {
      val qg = connectedComponentFor(patternNode, visited)
      val coveredIds = qg.idsWithoutOptionalMatchesOrUpdates
      val shortestRelationships = shortestRelationshipPatterns.filter {
        _.rel.boundaryNodesSet.map(_.name).forall(coveredIds.contains)
      }
      val shortestPathIds = shortestRelationships.flatMap(p => Set(p.rel.variable) ++ p.maybePathVar).map(_.name)
      val allIds = coveredIds ++ argumentIds ++ shortestPathIds

      val predicates = predicatesWithLocalDependencies.filter(_.dependencies.subsetOf(allIds))
      val filteredHints = hints.filter(_.variables.forall(variable => coveredIds.contains(variable.name)))
      qg.withSelections(Selections(predicates))
        .withArgumentIds(argumentIds)
        .addHints(filteredHints)
        .addShortestRelationships(shortestRelationships.toIndexedSeq: _*)
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

    (argumentComponents ++ rest) match {
      case first +: rest =>
        first.addPredicates(strayPredicates) +: rest
      case x => x
    }
  }

  def joinHints: Set[UsingJoinHint] =
    hints.collect { case hint: UsingJoinHint => hint }

  private def connectedComponentFor(startNode: String, visited: mutable.Set[String]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    var connectedComponent = QueryGraph.empty
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      if (!visited(node)) {
        visited += node

        val (nodeConnections, nodes) = findConnectedEntities(node, connectedComponent)

        queue.enqueueAll(nodes)

        connectedComponent = connectedComponent
          .addPatternNodes(node)
          .addNodeConnections(nodeConnections)

        val alreadyHaveArguments = connectedComponent.argumentIds.nonEmpty

        if (
          !alreadyHaveArguments && (argumentsOverLapsWith(
            connectedComponent.idsWithoutOptionalMatchesOrUpdates
          ) || predicatePullsInArguments(node))
        ) {
          connectedComponent = connectedComponent.withArgumentIds(argumentIds)
          val nodesSolvedByArguments = patternNodes intersect connectedComponent.argumentIds
          queue.enqueueAll(nodesSolvedByArguments.toIndexedSeq)
        }
      }
    }
    connectedComponent
  }

  private def findConnectedEntities(
    node: String,
    connectedComponent: QueryGraph
  ): (Set[NodeConnection], Set[String]) = {

    // All node connections that either have `node` as the left or the right node.
    val nodeConnectionsOfNode = nodeConnections.filter { nc =>
      nc.boundaryNodesSet.map(_.name).contains(node) && !connectedComponent.nodeConnections.contains(nc)
    }
    // All nodes that get connected through `nodeConnectionsOfNode`
    val nodesConnectedThroughOneConnection = nodeConnectionsOfNode.map(_.otherSide(varFor(node)))

    // `(a)-[r]->(b), (c)-[r]->(d)` are connected through both relationships being named `r`.
    val patternRelationshipsWithSameName =
      patternRelationships.filterNot(nodeConnectionsOfNode).filter { r =>
        nodeConnectionsOfNode.exists {
          case r2: PatternRelationship if r.variable == r2.variable => true
          case _                                                    => false
        }
      }
    // All nodes that get connected through `patternRelationshipsWithSameName`
    val patternRelationshipsWithSameNameNodes = patternRelationshipsWithSameName.flatMap(r => Seq(r.left, r.right))

    (
      nodeConnectionsOfNode ++ patternRelationshipsWithSameName,
      (nodesConnectedThroughOneConnection ++ patternRelationshipsWithSameNameNodes).map(_.name)
    )
  }

  private def argumentsOverLapsWith(coveredIds: Set[String]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: String) = selections.flatPredicates.exists { p =>
    val dependencies = p.dependencies.map(_.name)
    dependencies(node) && (dependencies intersect argumentIds).nonEmpty
  }

  def standaloneArgumentPatternNodes: Set[String] = {
    patternNodes
      .intersect(argumentIds)
      .diff(nodeConnections.flatMap(_.coveredIds).map(_.name))
      .diff(shortestRelationshipPatterns.flatMap(_.rel.coveredIds).map(_.name))
  }

  override def toString: String = {
    var added = false
    val builder = new StringBuilder("QueryGraph {")
    def addSetIfNonEmptyS(s: Iterable[String], name: String): Unit = addSetIfNonEmpty(s, name, (x: String) => x)
    def addSetIfNonEmpty[T](s: Iterable[T], name: String, f: T => String): Unit = {
      if (s.nonEmpty) {
        if (added)
          builder.append(", ")
        else
          added = true

        val sortedInput = if (s.isInstanceOf[Set[_]]) s.map(x => f(x)).toSeq.sorted else s.map(f)
        builder.append(s"$name: ").append(sortedInput.mkString("['", "', '", "']"))
      }
    }

    addSetIfNonEmptyS(patternNodes, "Nodes")
    addSetIfNonEmpty(patternRelationships, "Rels", (_: PatternRelationship).toString)
    addSetIfNonEmpty(quantifiedPathPatterns, "Quantified path patterns", (_: QuantifiedPathPattern).toString)
    addSetIfNonEmptyS(argumentIds, "Arguments")
    addSetIfNonEmpty(selections.flatPredicates, "Predicates", (e: Expression) => QueryGraph.stringifier.apply(e))
    addSetIfNonEmpty(shortestRelationshipPatterns, "Shortest relationships", (_: ShortestRelationshipPattern).toString)
    addSetIfNonEmpty(optionalMatches, "Optional Matches: ", (_: QueryGraph).toString)
    addSetIfNonEmpty(hints, "Hints", (_: Hint).toString)
    addSetIfNonEmpty(mutatingPatterns, "MutatingPatterns", (_: MutatingPattern).toString)
    addSetIfNonEmpty(selectivePathPatterns, "SelectivePathPatterns", (_: SelectivePathPattern).toString)

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
  override def equals(in: Any): Boolean = {
    in match {
      case other @ QueryGraph(
          otherPatternRelationships,
          otherQuantifiedPathPatterns,
          otherPatternNodes,
          otherArgumentIds,
          otherSelections,
          otherOptionalMatches,
          otherHints,
          otherShortestRelationshipPatterns,
          otherMutatingPatterns,
          otherSelectivePathPatterns
        ) =>
        if (this eq other) {
          true
        } else {
          patternRelationships == otherPatternRelationships &&
          quantifiedPathPatterns == otherQuantifiedPathPatterns &&
          patternNodes == otherPatternNodes &&
          argumentIds == otherArgumentIds &&
          selections == otherSelections &&
          optionalMatches.toSet == otherOptionalMatches.toSet &&
          hints == otherHints &&
          shortestRelationshipPatterns == otherShortestRelationshipPatterns &&
          mutatingPatterns == otherMutatingPatterns &&
          selectivePathPatterns == otherSelectivePathPatterns
        }
      case _ =>
        false
    }
  }

  override lazy val hashCode: Int = this match {
    // The point of this "useless" match case is to catch your attention if you modified the fields of the QueryGraph.
    // Please remember to update the hash code.
    case QueryGraph(_, _, _, _, _, _, _, _, _, _) =>
      ScalaRunTime._hashCode((
        patternRelationships,
        quantifiedPathPatterns,
        patternNodes,
        argumentIds,
        selections,
        optionalMatches.toSet,
        hints.groupBy(identity),
        shortestRelationshipPatterns,
        mutatingPatterns,
        selectivePathPatterns
      ))
  }

}

object QueryGraph {
  def empty: QueryGraph = new QueryGraph()

  // Overridden to avoid creating illegal QGs
  def apply(
    patternRelationships: Set[PatternRelationship] = Set.empty,
    quantifiedPathPatterns: Set[QuantifiedPathPattern] = Set.empty,
    patternNodes: Set[String] = Set.empty,
    argumentIds: Set[String] = Set.empty,
    selections: Selections = Selections(),
    optionalMatches: IndexedSeq[QueryGraph] = Vector.empty,
    hints: Set[Hint] = Set.empty,
    shortestRelationshipPatterns: Set[ShortestRelationshipPattern] = Set.empty,
    mutatingPatterns: IndexedSeq[MutatingPattern] = IndexedSeq.empty,
    selectivePathPatterns: Set[SelectivePathPattern] = Set.empty
  ): QueryGraph = {
    val allPatternNodes = patternNodes ++
      patternRelationships.flatMap(_.boundaryNodesSet).map(_.name) ++
      quantifiedPathPatterns.flatMap(_.boundaryNodesSet).map(_.name) ++
      selectivePathPatterns.flatMap(_.boundaryNodesSet).map(_.name) ++
      shortestRelationshipPatterns.flatMap(_.rel.boundaryNodesSet).map(_.name)

    new QueryGraph(
      patternRelationships,
      quantifiedPathPatterns,
      allPatternNodes,
      argumentIds,
      selections,
      optionalMatches,
      hints,
      shortestRelationshipPatterns,
      mutatingPatterns,
      selectivePathPatterns
    )
  }

  val stringifier: ExpressionStringifier = ExpressionStringifier(
    extensionStringifier = new ExpressionStringifier.Extension {

      override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
        case pp: PartialPredicate[_] => s"partial(${ctx(pp.coveredPredicate)}, ${ctx(pp.coveringPredicate)})"
        case e                       => e.asCanonicalStringVal
      }
    },
    alwaysParens = false,
    alwaysBacktick = false,
    preferSingleQuotes = false,
    sensitiveParamsAsParams = false
  )

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      val ys = y.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq.sorted
      Implicits.seqOrdering[Seq, String].compare(xs, ys)
    }
  }

}
