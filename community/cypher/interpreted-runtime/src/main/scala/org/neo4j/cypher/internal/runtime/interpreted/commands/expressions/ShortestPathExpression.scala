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

import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted.commands.{ShortestPath, SingleNode, _}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, Expander, KernelPredicate}
import org.neo4j.cypher.internal.v4_0.util.NonEmptyList
import org.neo4j.exceptions.{ShortestPathCommonEndNodesForbiddenException, SyntaxException}
import org.neo4j.graphdb.{Entity, NotFoundException, Path, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeReference, NodeValue, VirtualValues}

import scala.collection.JavaConverters._

case class ShortestPathExpression(shortestPathPattern: ShortestPath,
                                  perStepPredicates: Seq[Predicate] = Seq.empty,
                                  fullPathPredicates: Seq[Predicate] = Seq.empty,
                                  withFallBack: Boolean = false,
                                  disallowSameNode: Boolean = true) extends Expression {

  val predicates = perStepPredicates ++ fullPathPredicates

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    if (anyStartpointsContainNull(ctx)) {
      Values.NO_VALUE
    } else {
      val start = getEndPoint(ctx, state, shortestPathPattern.left)
      val end = getEndPoint(ctx, state, shortestPathPattern.right)
      if (!shortestPathPattern.allowZeroLength && disallowSameNode && start
        .equals(end)) throw new ShortestPathCommonEndNodesForbiddenException
      getMatches(ctx, start, end, state)
    }
  }

  private def getMatches(ctx: ExecutionContext, start: NodeValue, end: NodeValue, state: QueryState): AnyValue = {
    val (expander, nodePredicates) = addPredicates(ctx, makeRelationshipTypeExpander(), state)
    val maybePredicate = if (predicates.isEmpty) None else Some(Ands(NonEmptyList.from(predicates)))
    /* This test is made after a full shortest path candidate has been produced,
     * accepting or disqualifying it as appropriate.
     */
    val shortestPathPredicate = createShortestPathPredicate(ctx, maybePredicate, state)

    if (shortestPathPattern.single) {
      val result = state.query
        .singleShortestPath(start.id(), end.id(), shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander,
                            shortestPathPredicate, nodePredicates)
      if (!shortestPathPattern.allowZeroLength && result.forall(p => p.length() == 0))
        Values.NO_VALUE
      else result.map(ValueUtils.fromPath).getOrElse(Values.NO_VALUE)
    }
    else {
      val result = state.query
        .allShortestPath(start.id(), end.id(), shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander,
                         shortestPathPredicate, nodePredicates)
        .filter { p => shortestPathPattern.allowZeroLength || p.length() > 0 }.map(ValueUtils.fromPath).toArray
      VirtualValues.list(result:_*)
    }
  }

  private def createShortestPathPredicate(incomingCtx: ExecutionContext,
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

  private def getEndPoint(m: ExecutionContext, state: QueryState, start: SingleNode): NodeValue = {
    try {
      m.getByName(start.name) match {
        case node: NodeValue => node
        case node: NodeReference => state.query.nodeOps.getById(node.id())
      }
    } catch {
      case _: NotFoundException =>
        throw new SyntaxException(
          s"To find a shortest path, both ends of the path need to be provided. Couldn't find `$start`")
    }
  }

  private def anyStartpointsContainNull(m: ExecutionContext): Boolean =
    (m.getByName(shortestPathPattern.left.name) eq Values.NO_VALUE) ||
      (m.getByName(shortestPathPattern.right.name) eq Values.NO_VALUE)

  override def children: Seq[AstNode[_]] = Seq(shortestPathPattern) ++ perStepPredicates ++ fullPathPredicates

  override def arguments: Seq[Expression] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(ShortestPathExpression(shortestPathPattern.rewrite(f)))

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

  private def cypherPositivePredicatesAsExpander(incomingCtx: ExecutionContext,
                                                 variableOffset: Int,
                                                 predicate: Predicate,
                                                 state: QueryState) = new KernelPredicate[Entity] {
    override def test(t: Entity): Boolean = {
      state.expressionVariables(variableOffset) = ValueUtils.asNodeOrEdgeValue(t)
      predicate.isTrue(incomingCtx, state)
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ExecutionContext,
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
  private def addAllOrNoneRelationshipExpander(ctx: ExecutionContext,
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
  private def addAllOrNoneNodeExpander(ctx: ExecutionContext,
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
  private def addPredicates(ctx: ExecutionContext, relTypeAndDirExpander: Expander, state: QueryState):
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
