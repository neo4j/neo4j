/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{PathExtractor, Pattern, ShortestPath, SingleNode, _}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb._
import org.neo4j.kernel.Traversal

import scala.collection.JavaConverters._
import scala.collection.Map

case class ShortestPathExpression(shortestPathPattern: ShortestPath, predicates: Seq[Predicate] = Seq.empty) extends Expression with PathExtractor {
  val pathPattern:Seq[Pattern] = Seq(shortestPathPattern)
  val pathIdentifiers = Set(shortestPathPattern.pathName, shortestPathPattern.relIterator.getOrElse(""))


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
    val expander: Expander = addPredicates(ctx, makeRelationshipTypeExpander())
    val shortestPathPredicate = createShortestPathPredicate(ctx, predicates)
    val shortestPathStrategy = if (shortestPathPattern.single)
      new SingleShortestPathStrategy(expander, shortestPathPattern.allowZeroLength, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), shortestPathPredicate)
    else
      new AllShortestPathsStrategy(expander, shortestPathPattern.allowZeroLength, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), shortestPathPredicate)

    shortestPathStrategy.findResult(start, end)
  }

  /* This test is made after a full shortest path candidate has been produced,
   * accepting or disqualifying it as appropriate.
   */
  private def createShortestPathPredicate(incomingCtx: ExecutionContext, predicates: Seq[Predicate])(implicit state: QueryState): ShortestPathPredicate = new ShortestPathPredicate {
    override def test(path: Path): Boolean = if (predicates.isEmpty) true else {
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

  def calculateType(symbols: SymbolTable) =  if (shortestPathPattern.single) CTPath else CTCollection(CTPath)

  def symbolTableDependencies = shortestPathPattern.symbolTableDependencies + shortestPathPattern.left.name + shortestPathPattern.right.name

  private def propertyExistsExpander(name: String) = new org.neo4j.function.Predicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      t.hasProperty(name)
    }
  }

  private def propertyNotExistsExpander(name: String) = new org.neo4j.function.Predicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !t.hasProperty(name)
    }
  }

  private def cypherPositivePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new org.neo4j.function.Predicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      predicate.isTrue(incomingCtx += (name -> t))
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new org.neo4j.function.Predicate[PropertyContainer] {
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

  private def makeRelationshipTypeExpander() = if (shortestPathPattern.relTypes.isEmpty) {
    Traversal.expanderForAllTypes(toGraphDb(shortestPathPattern.dir))
  } else {
    shortestPathPattern.relTypes.foldLeft(Traversal.emptyExpander()) {
      case (e, t) => e.add(DynamicRelationshipType.withName(t), toGraphDb(shortestPathPattern.dir))
    }
  }

  private def addPredicates(ctx: ExecutionContext, relTypeAndDirExpander: Expander)(implicit state: QueryState): Expander = if (predicates.isEmpty) relTypeAndDirExpander
  else
    predicates.foldLeft(relTypeAndDirExpander) {
      case (currentExpander, predicate) =>
        predicate match {
          case NoneInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
            addAllOrNoneRelationshipExpander(ctx, currentExpander, all = false, innerPredicate, symbolName)
          case AllInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
            addAllOrNoneRelationshipExpander(ctx, currentExpander, all = true, innerPredicate, symbolName)
          case _ => currentExpander
        }
    }

  private def doesNotDependOnFullPath(predicate: Predicate): Boolean = {
    (predicate.symbolTableDependencies intersect pathIdentifiers).isEmpty
  }

  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes, ReadsAllRelationships)
}

trait ShortestPathStrategy {
  def findResult(start: Node, end: Node): Any
}

class SingleShortestPathStrategy(expander: Expander, allowZeroLength: Boolean, depth: Int, predicate: ShortestPathPredicate) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth, predicate)

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
