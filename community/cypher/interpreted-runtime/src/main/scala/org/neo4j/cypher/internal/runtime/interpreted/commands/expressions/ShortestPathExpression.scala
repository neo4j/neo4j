/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AllInList
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.NoneInList
import org.neo4j.cypher.internal.runtime.interpreted.commands.ShortestPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.SingleNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Ands
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Not
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.PropertyExists
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeReference
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

case class ShortestPathExpression(shortestPathPattern: ShortestPath,
                                  perStepPredicates: Seq[Predicate] = Seq.empty,
                                  fullPathPredicates: Seq[Predicate] = Seq.empty,
                                  withFallBack: Boolean = false,
                                  disallowSameNode: Boolean = true,
                                  operatorId: Id = Id.INVALID_ID) extends Expression {

  val predicates = perStepPredicates ++ fullPathPredicates

  def apply(row: ReadableRow, state: QueryState): AnyValue = {
    apply(row, state, state.memoryTracker.memoryTrackerForOperator(operatorId.x))
  }

  def apply(row: ReadableRow, state: QueryState, memoryTracker: MemoryTracker): AnyValue = {
    if (anyStartpointsContainNull(row)) {
      Values.NO_VALUE
    } else {
      val start = getEndPoint(row, state, shortestPathPattern.left)
      val end = getEndPoint(row, state, shortestPathPattern.right)
      if (!shortestPathPattern.allowZeroLength && disallowSameNode && start
        .equals(end)) throw new ShortestPathCommonEndNodesForbiddenException
      getMatches(row, start, end, state, memoryTracker)
    }
  }

  private def getMatches(ctx: ReadableRow, start: NodeValue, end: NodeValue, state: QueryState, memoryTracker: MemoryTracker): AnyValue = {
    val (expander, nodePredicates) = addPredicates(ctx, makeRelationshipTypeExpander(), state)
    val maybePredicate = if (predicates.isEmpty) None else Some(Ands(NonEmptyList.from(predicates)))
    /* This test is made after a full shortest path candidate has been produced,
     * accepting or disqualifying it as appropriate.
     */
    val cypherRow = ctx.asInstanceOf[CypherRow] // TODO: less ugly solution to evaluating predicates
    val shortestPathPredicate = createShortestPathPredicate(cypherRow, maybePredicate, state)

    if (shortestPathPattern.single) {
      val result = state.query
        .singleShortestPath(start.id(), end.id(), shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander,
          shortestPathPredicate, nodePredicates, memoryTracker)
      if (!shortestPathPattern.allowZeroLength && result.forall(p => p.length() == 0))
        Values.NO_VALUE
      else result.map(ValueUtils.fromPath).getOrElse(Values.NO_VALUE)
    }
    else {
      val result = state.query
        .allShortestPath(start.id(), end.id(), shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander,
          shortestPathPredicate, nodePredicates, memoryTracker)
        .filter { p => shortestPathPattern.allowZeroLength || p.length() > 0 }
        .map(ValueUtils.fromPath)
        .toArray
      VirtualValues.list(result:_*)
    }
  }

  private def createShortestPathPredicate(incomingCtx: CypherRow,
                                          maybePredicate: Option[Predicate],
                                          state: QueryState): KernelPredicate[Path] =
    new KernelPredicate[Path] {

      override def test(path: Path): Boolean = maybePredicate.forall {
        predicate =>
          incomingCtx.set(shortestPathPattern.pathName, ValueUtils.fromPath(path))
          incomingCtx.set(shortestPathPattern.relIterator.get, ValueUtils.asListOfEdges(path.relationships()))
          predicate.isTrue(incomingCtx, state)
      } && (!withFallBack || ShortestPathExpression.noDuplicates(path.relationships.asScala))
    }

  private def getEndPoint(ctx: ReadableRow, state: QueryState, start: SingleNode): NodeValue = {
    try {
      ctx.getByName(start.name) match {
        case node: NodeValue => node
        case node: NodeReference => state.query.nodeOps.getById(node.id())
      }
    } catch {
      case _: NotFoundException =>
        throw new SyntaxException(
          s"To find a shortest path, both ends of the path need to be provided. Couldn't find `$start`")
    }
  }

  private def anyStartpointsContainNull(ctx: ReadableRow): Boolean =
    (ctx.getByName(shortestPathPattern.left.name) eq Values.NO_VALUE) ||
      (ctx.getByName(shortestPathPattern.right.name) eq Values.NO_VALUE)

  override def children: Seq[AstNode[_]] = Seq(shortestPathPattern) ++ perStepPredicates ++ fullPathPredicates

  override def arguments: Seq[Expression] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(ShortestPathExpression(shortestPathPattern.rewrite(f), operatorId = operatorId))

  private def propertyExistsExpander(name: String) = new KernelPredicate[Entity] {
    override def test(t: Entity): Boolean = {
      t.hasProperty(name)
    }
  }

  private def propertyNotExistsExpander(name: String) = new KernelPredicate[Entity] {
    override def test(t: Entity): Boolean = {
      !t.hasProperty(name)
    }
  }

  private def cypherPositivePredicatesAsExpander(incomingCtx: ReadableRow,
                                                 variableOffset: Int,
                                                 predicate: Predicate,
                                                 state: QueryState) = new KernelPredicate[Entity] {
    override def test(t: Entity): Boolean = {
      state.expressionVariables(variableOffset) = ValueUtils.asNodeOrEdgeValue(t)
      predicate.isTrue(incomingCtx, state)
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ReadableRow,
                                                 variableOffset: Int,
                                                 predicate: Predicate,
                                                 state: QueryState) = new KernelPredicate[Entity] {
    override def test(t: Entity): Boolean = {
      state.expressionVariables(variableOffset) = ValueUtils.asNodeOrEdgeValue(t)
      !predicate.isTrue(incomingCtx, state)
    }
  }

  private def findPredicate(predicate: Predicate) = predicate match {
    case CoercedPredicate(inner: ExtendedExpression)  => inner.legacy
    case _ => predicate
  }

  //TODO we shouldn't do this matching at runtime but instead figure this out in planning
  private def addAllOrNoneRelationshipExpander(ctx: ReadableRow,
                                               currentExpander: Expander,
                                               all: Boolean,
                                               predicate: Predicate,
                                               relVariableOffset: Int,
                                               state: QueryState): Expander = {
    findPredicate(predicate) match {
      case PropertyExists(_, propertyKey) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyExistsExpander(propertyKey.name)
          else propertyNotExistsExpander(propertyKey.name))
      case Not(PropertyExists(_, propertyKey)) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyNotExistsExpander(propertyKey.name)
          else propertyExistsExpander(propertyKey.name))
      case _ => currentExpander.addRelationshipFilter(
        if (all) cypherPositivePredicatesAsExpander(ctx, relVariableOffset, predicate, state)
        else cypherNegativePredicatesAsExpander(ctx, relVariableOffset, predicate, state))
    }
  }

  //TODO we shouldn't do this matching at runtime but instead figure this out in planning
  private def addAllOrNoneNodeExpander(ctx: ReadableRow,
                                       currentExpander: Expander,
                                       all: Boolean,
                                       predicate: Predicate,
                                       relVariableOffset: Int,
                                       currentNodePredicates: Seq[KernelPredicate[Entity]],
                                       state: QueryState): (Expander, Seq[KernelPredicate[Entity]]) = {
    val filter = findPredicate(predicate) match {
      case PropertyExists(_, propertyKey) =>
        if (all) propertyExistsExpander(propertyKey.name)
        else propertyNotExistsExpander(propertyKey.name)
      case Not(PropertyExists(_, propertyKey)) =>
        if (all) propertyNotExistsExpander(propertyKey.name)
        else propertyExistsExpander(propertyKey.name)
      case _ =>
        if (all) cypherPositivePredicatesAsExpander(ctx, relVariableOffset, predicate, state)
        else cypherNegativePredicatesAsExpander(ctx, relVariableOffset, predicate, state)
    }
    (currentExpander.addNodeFilter(filter), currentNodePredicates :+ filter)
  }

  private def makeRelationshipTypeExpander(): Expander = if (shortestPathPattern.relTypes.isEmpty) {
    Expanders.allTypes(shortestPathPattern.dir)
  } else {
    shortestPathPattern.relTypes.foldLeft(Expanders.typeDir()) {
      case (e, t) => e.add(t, shortestPathPattern.dir)
    }
  }

  //TODO we should have made these decisions at plan time and not match on expressions here
  private def addPredicates(ctx: ReadableRow, relTypeAndDirExpander: Expander, state: QueryState):
  (Expander, Seq[KernelPredicate[Entity]]) =
    if (perStepPredicates.isEmpty) (relTypeAndDirExpander, Seq())
    else
      perStepPredicates.map(findPredicate).foldLeft((relTypeAndDirExpander, Seq[KernelPredicate[Entity]]())) {
        case ((currentExpander, currentNodePredicates: Seq[KernelPredicate[Entity]]), predicate) =>
          predicate match {
            case NoneInList(relFunction, _, variableOffset, innerPredicate) if isRelationshipsFunction(relFunction) =>
              val expander = addAllOrNoneRelationshipExpander(ctx, currentExpander, all = false, innerPredicate,
                variableOffset, state)
              (expander, currentNodePredicates)
            case AllInList(relFunction, _, variableOffset, innerPredicate)  if isRelationshipsFunction(relFunction) =>
              val expander = addAllOrNoneRelationshipExpander(ctx, currentExpander, all = true, innerPredicate,
                variableOffset, state)
              (expander, currentNodePredicates)
            case NoneInList(nodeFunction, _, variableOffset, innerPredicate) if isNodesFunction(nodeFunction) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = false, innerPredicate, variableOffset,
                currentNodePredicates, state)
            case AllInList(nodeFunction, _, variableOffset, innerPredicate) if isNodesFunction(nodeFunction) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = true, innerPredicate, variableOffset,
                currentNodePredicates, state)
            case _ => (currentExpander, currentNodePredicates)
          }
      }

  private def isNodesFunction(expression: Expression): Boolean = expression match {
    case _: NodesFunction => true
    case e: ExtendedExpression => isNodesFunction(e.legacy)
    case _ => false
  }

  private def isRelationshipsFunction(expression: Expression): Boolean = expression match {
    case _: RelationshipFunction => true
    case e: ExtendedExpression => isRelationshipsFunction(e.legacy)
    case _ => false
  }
}

object ShortestPathExpression {
  def noDuplicates(relationships: Iterable[Relationship]): Boolean = {
    relationships.map(_.getId).toSet.size == relationships.size
  }
}
