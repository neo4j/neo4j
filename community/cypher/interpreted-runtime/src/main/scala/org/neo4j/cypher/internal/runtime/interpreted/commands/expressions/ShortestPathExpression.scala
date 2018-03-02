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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates._
import org.neo4j.cypher.internal.runtime.interpreted.commands.{Pattern, ShortestPath, SingleNode, _}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{Expander, KernelPredicate}
import org.neo4j.cypher.internal.util.v3_4.{NonEmptyList, ShortestPathCommonEndNodesForbiddenException, SyntaxException}
import org.neo4j.graphdb.{Path, PropertyContainer, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeReference, NodeValue, VirtualValues}

import scala.collection.JavaConverters._
import scala.collection.Map

case class ShortestPathExpression(shortestPathPattern: ShortestPath,
                                  perStepPredicates: Seq[Predicate] = Seq.empty,
                                  fullPathPredicates: Seq[Predicate] = Seq.empty,
                                  withFallBack: Boolean = false, disallowSameNode: Boolean = true) extends Expression {

  val pathPattern: Seq[Pattern] = Seq(shortestPathPattern)
  val pathVariables = Set(shortestPathPattern.pathName, shortestPathPattern.relIterator.getOrElse(""))
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
      else result.map(ValueUtils.asPathValue).getOrElse(Values.NO_VALUE)
    }
    else {
      val result = state.query
        .allShortestPath(start.id(), end.id(), shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander,
                         shortestPathPredicate, nodePredicates)
        .filter { p => shortestPathPattern.allowZeroLength || p.length() > 0 }.map(ValueUtils.asPathValue).toArray
      VirtualValues.list(result:_*)
    }
  }

  private def createShortestPathPredicate(incomingCtx: ExecutionContext,
                                          maybePredicate: Option[Predicate],
                                          state: QueryState): KernelPredicate[Path] =
    new KernelPredicate[Path] {

      override def test(path: Path): Boolean = maybePredicate.forall {
        predicate =>
          incomingCtx += shortestPathPattern.pathName -> ValueUtils.asPathValue(path)
          incomingCtx += shortestPathPattern.relIterator.get -> ValueUtils.asListOfEdges(path.relationships())
          predicate.isTrue(incomingCtx, state)
      } && (!withFallBack || ShortestPathExpression.noDuplicates(path.relationships.asScala))
    }

  private def getEndPoint(m: Map[String, AnyValue], state: QueryState, start: SingleNode): NodeValue =
    m.getOrElse(start.name,
      throw new SyntaxException(
        s"To find a shortest path, both ends of the path need to be provided. Couldn't find `$start`")) match {
      case node: NodeValue => node
      case node: NodeReference => state.query.nodeOps.getById(node.id())
    }

  private def anyStartpointsContainNull(m: Map[String, Any]): Boolean =
    m(shortestPathPattern.left.name) == Values.NO_VALUE || m(shortestPathPattern.right.name) == Values.NO_VALUE

  override def children = Seq(shortestPathPattern)

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(ShortestPathExpression(shortestPathPattern.rewrite(f)))

  def symbolTableDependencies = shortestPathPattern.symbolTableDependencies + shortestPathPattern.left
    .name + shortestPathPattern.right.name

  private def propertyExistsExpander(name: String) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      t.hasProperty(name)
    }
  }

  private def propertyNotExistsExpander(name: String) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !t.hasProperty(name)
    }
  }

  private def cypherPositivePredicatesAsExpander(incomingCtx: ExecutionContext,
                                                 name: String,
                                                 predicate: Predicate,
                                                 state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      predicate.isTrue(incomingCtx += (name -> ValueUtils.asNodeOrEdgeValue(t)), state)
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ExecutionContext,
                                                 name: String,
                                                 predicate: Predicate,
                                                 state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !predicate.isTrue(incomingCtx += (name -> ValueUtils.asNodeOrEdgeValue(t)), state)
    }
  }

  private def addAllOrNoneRelationshipExpander(ctx: ExecutionContext,
                                               currentExpander: Expander,
                                               all: Boolean,
                                               predicate: Predicate,
                                               relName: String,
                                               state: QueryState): Expander = {
    predicate match {
      case PropertyExists(_, propertyKey) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyExistsExpander(propertyKey.name)
          else propertyNotExistsExpander(propertyKey.name))
      case Not(PropertyExists(_, propertyKey)) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyNotExistsExpander(propertyKey.name)
          else propertyExistsExpander(propertyKey.name))
      case _ => currentExpander.addRelationshipFilter(
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate, state)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate, state))
    }
  }

  private def addAllOrNoneNodeExpander(ctx: ExecutionContext, currentExpander: Expander, all: Boolean,
                                       predicate: Predicate, relName: String,
                                       currentNodePredicates: Seq[KernelPredicate[PropertyContainer]],
                                       state: QueryState): (Expander, Seq[KernelPredicate[PropertyContainer]]) = {
    val filter = predicate match {
      case PropertyExists(_, propertyKey) =>
        if (all) propertyExistsExpander(propertyKey.name)
        else propertyNotExistsExpander(propertyKey.name)
      case Not(PropertyExists(_, propertyKey)) =>
        if (all) propertyNotExistsExpander(propertyKey.name)
        else propertyExistsExpander(propertyKey.name)
      case _ =>
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate, state)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate, state)
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

  private def addPredicates(ctx: ExecutionContext, relTypeAndDirExpander: Expander, state: QueryState):
  (Expander, Seq[KernelPredicate[PropertyContainer]]) =
    if (perStepPredicates.isEmpty) (relTypeAndDirExpander, Seq())
    else
      perStepPredicates.foldLeft((relTypeAndDirExpander, Seq[KernelPredicate[PropertyContainer]]())) {
        case ((currentExpander, currentNodePredicates: Seq[KernelPredicate[PropertyContainer]]), predicate) =>
          predicate match {
            case NoneInList(RelationshipFunction(_), symbolName, innerPredicate) =>
              val expander = addAllOrNoneRelationshipExpander(ctx, currentExpander, all = false, innerPredicate,
                symbolName, state)
              (expander, currentNodePredicates)
            case AllInList(RelationshipFunction(_), symbolName, innerPredicate) =>
              val expander = addAllOrNoneRelationshipExpander(ctx, currentExpander, all = true, innerPredicate,
                symbolName, state)
              (expander, currentNodePredicates)
            case NoneInList(NodesFunction(_), symbolName, innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = false, innerPredicate, symbolName,
                                       currentNodePredicates, state)
            case AllInList(NodesFunction(_), symbolName, innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = true, innerPredicate, symbolName,
                                       currentNodePredicates, state)
            case _ => (currentExpander, currentNodePredicates)
          }
      }
}

object ShortestPathExpression {
  def noDuplicates(relationships: Iterable[Relationship]): Boolean = {
    relationships.map(_.getId).toSet.size == relationships.size
  }
}
