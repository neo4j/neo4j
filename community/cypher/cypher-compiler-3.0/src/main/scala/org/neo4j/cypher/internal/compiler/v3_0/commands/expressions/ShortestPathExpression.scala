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
package org.neo4j.cypher.internal.compiler.v3_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_0.commands.predicates._
import org.neo4j.cypher.internal.compiler.v3_0.commands.{PathExtractor, Pattern, ShortestPath, SingleNode, _}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.SyntaxException
import org.neo4j.cypher.internal.frontend.v3_0.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb._
import java.util.function.{Predicate => KernelPredicate}
import org.neo4j.kernel.Traversal

import scala.collection.JavaConverters._
import scala.collection.Map

case class ShortestPathExpression(shortestPathPattern: ShortestPath, predicates: Seq[Predicate] = Seq.empty) extends Expression with PathExtractor {
  val pathPattern: Seq[Pattern] = Seq(shortestPathPattern)
  val pathVariables = Set(shortestPathPattern.pathName, shortestPathPattern.relIterator.getOrElse(""))

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    if (anyStartpointsContainNull(ctx)) {
      Stream.empty
    } else {
      getMatches(ctx)
    }
  }

  private def getMatches(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val start = getEndPoint(ctx, shortestPathPattern.left)
    val end = getEndPoint(ctx, shortestPathPattern.right)
    val (expander, nodePredicates) = addPredicates(ctx, makeRelationshipTypeExpander())
    val shortestPathPredicate = createShortestPathPredicate(ctx, predicates)
    val shortestPathStrategy = if (shortestPathPattern.single)
      new SingleShortestPathStrategy(expander, shortestPathPattern.allowZeroLength, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), shortestPathPredicate, nodePredicates)
    else
      new AllShortestPathsStrategy(expander, shortestPathPattern.allowZeroLength, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), shortestPathPredicate)

    shortestPathStrategy.findResult(start, end)
  }

  /* This test is made after a full shortest path candidate has been produced,
   * accepting or disqualifying it as appropriate.
   */
  private def createShortestPathPredicate(incomingCtx: ExecutionContext, predicates: Seq[Predicate])(implicit state: QueryState): ShortestPathPredicate = new ShortestPathPredicate {
    override def test(path: Path): Boolean = if (predicates.isEmpty) true
    else {
      incomingCtx += shortestPathPattern.pathName -> path
      incomingCtx += shortestPathPattern.relIterator.get -> path.relationships()

      val ands = Ands(NonEmptyList.from(predicates))
      ands.isTrue(incomingCtx)
    }
  }

  private def getEndPoint(m: Map[String, Any], start: SingleNode): Node = m.getOrElse(start.name,
    throw new SyntaxException(s"To find a shortest path, both ends of the path need to be provided. Couldn't find `$start`")).asInstanceOf[Node]

  private def anyStartpointsContainNull(m: Map[String, Any]): Boolean =
    m(shortestPathPattern.left.name) == null || m(shortestPathPattern.right.name) == null

  override def children = Seq(shortestPathPattern)

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(ShortestPathExpression(shortestPathPattern.rewrite(f)))

  def calculateType(symbols: SymbolTable) = if (shortestPathPattern.single) CTPath else CTCollection(CTPath)

  def symbolTableDependencies = shortestPathPattern.symbolTableDependencies + shortestPathPattern.left.name + shortestPathPattern.right.name

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

  private def cypherPositivePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      predicate.isTrue(incomingCtx += (name -> t))
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !predicate.isTrue(incomingCtx += (name -> t))
    }
  }

  private def addAllOrNoneRelationshipExpander(ctx: ExecutionContext, currentExpander: Expander, all: Boolean, predicate: Predicate, relName: String)(implicit state: QueryState) = {
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
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate))
    }
  }

  private def addAllOrNoneNodeExpander(ctx: ExecutionContext, currentExpander: Expander, all: Boolean,
                                       predicate: Predicate, relName: String,
                                       currentNodePredicates: Seq[KernelPredicate[PropertyContainer]])
                                      (implicit state: QueryState): (Expander, Seq[KernelPredicate[PropertyContainer]]) = {
    val filter = predicate match {
      case PropertyExists(_, propertyKey) =>
        if (all) propertyExistsExpander(propertyKey.name)
        else propertyNotExistsExpander(propertyKey.name)
      case Not(PropertyExists(_, propertyKey)) =>
        if (all) propertyNotExistsExpander(propertyKey.name)
        else propertyExistsExpander(propertyKey.name)
      case _ =>
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate)
    }
    (currentExpander.addNodeFilter(filter), currentNodePredicates :+ filter)
  }

  private def makeRelationshipTypeExpander() = if (shortestPathPattern.relTypes.isEmpty) {
    Traversal.expanderForAllTypes(toGraphDb(shortestPathPattern.dir))
  } else {
    shortestPathPattern.relTypes.foldLeft(Traversal.emptyExpander()) {
      case (e, t) => e.add(withName(t), toGraphDb(shortestPathPattern.dir))
    }
  }

  private def addPredicates(ctx: ExecutionContext, relTypeAndDirExpander: Expander)(implicit state: QueryState):
  (Expander, Seq[KernelPredicate[PropertyContainer]]) =
    if (predicates.isEmpty) (relTypeAndDirExpander, Seq())
    else
      predicates.foldLeft((relTypeAndDirExpander, Seq[KernelPredicate[PropertyContainer]]())) {
        case ((currentExpander, currentNodePredicates: Seq[KernelPredicate[PropertyContainer]]), predicate) =>
          predicate match {
            case NoneInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              (addAllOrNoneRelationshipExpander(ctx, currentExpander, all = false, innerPredicate, symbolName), currentNodePredicates)
            case AllInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              (addAllOrNoneRelationshipExpander(ctx, currentExpander, all = true, innerPredicate, symbolName), currentNodePredicates)
            case NoneInCollection(NodesFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = false, innerPredicate, symbolName, currentNodePredicates)
            case AllInCollection(NodesFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = true, innerPredicate, symbolName, currentNodePredicates)
            case _ => (currentExpander, currentNodePredicates)
          }
      }

  private def doesNotDependOnFullPath(predicate: Predicate): Boolean = {
    (predicate.symbolTableDependencies intersect pathVariables).isEmpty
  }

  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes, ReadsAllRelationships)
}

trait ShortestPathStrategy {
  def findResult(start: Node, end: Node): Any
}

class SingleShortestPathStrategy(expander: Expander, allowZeroLength: Boolean, depth: Int, predicate: ShortestPathPredicate,
                                 filters: Seq[KernelPredicate[PropertyContainer]]) extends ShortestPathStrategy {
  private val finder = new org.neo4j.graphalgo.impl.path.ShortestPath(depth, expander, predicate) {
    protected override def filterNextLevelNodes(nextNode: Node): Node = {
      if (filters.isEmpty)
        nextNode
      else
        if (filters.forall(filter => filter test nextNode)) nextNode
        else null
    }
  }

  def findResult(start: Node, end: Node): Path = {
    val result = finder.findSinglePath(start, end)
    if (!allowZeroLength && result != null && result.length() == 0)
      null
    else
      result
  }
}

class AllShortestPathsStrategy(expander: Expander, allowZeroLength: Boolean, depth: Int, predicate: ShortestPathPredicate) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth, predicate)

  def findResult(start: Node, end: Node): Stream[Path] = {
    finder.findAllPaths(start, end).asScala.toStream
  }.filter { p => allowZeroLength || p.length() > 0 }
}
