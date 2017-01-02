/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.{PathExtractor, Pattern, ShortestPath, SingleNode}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{Effects, ReadsNodes, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphdb.{DynamicRelationshipType, Expander, Node, Path}
import org.neo4j.kernel.Traversal

import scala.collection.JavaConverters._
import scala.collection.Map

case class ShortestPathExpression(ast: ShortestPath) extends Expression with PathExtractor {
  val pathPattern:Seq[Pattern] = Seq(ast)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    if (anyStartpointsContainNull(ctx)) {
      Stream.empty
    } else {
      getMatches(ctx)
    }
  }

  private def getMatches(m: Map[String, Any]): Any = {
    val start = getEndPoint(m, ast.left)
    val end = getEndPoint(m, ast.right)
    shortestPathStrategy.findResult(start, end)
  }

  def getEndPoint(m: Map[String, Any], start: SingleNode): Node = m.getOrElse(start.name,
    throw new SyntaxException(s"To find a shortest path, both ends of the path need to be provided. Couldn't find `${start}`")).asInstanceOf[Node]

  private def anyStartpointsContainNull(m: Map[String, Any]): Boolean =
    m(ast.left.name) == null || m(ast.right.name) == null

  override def children = Seq(ast)

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(ShortestPathExpression(ast.rewrite(f)))

  private lazy val expander: Expander = if (ast.relTypes.isEmpty) {
    Traversal.expanderForAllTypes(ast.dir)
  } else {
    ast.relTypes.foldLeft(Traversal.emptyExpander()) {
      case (e, t) => e.add(DynamicRelationshipType.withName(t), ast.dir)
    }
  }

  val shortestPathStrategy = if (ast.single)
    new SingleShortestPathStrategy(expander, ast.allowZeroLength, ast.maxDepth.getOrElse(15))
  else
    new AllShortestPathsStrategy(expander, ast.allowZeroLength, ast.maxDepth.getOrElse(15))

  def calculateType(symbols: SymbolTable) =  shortestPathStrategy.typ

  def symbolTableDependencies = ast.symbolTableDependencies + ast.left.name + ast.right.name

  override def localEffects(symbols: SymbolTable) = Effects(ReadsNodes, ReadsRelationships)
}

trait ShortestPathStrategy {
  def findResult(start: Node, end: Node): Any
  def typ: CypherType
}

class SingleShortestPathStrategy(expander: Expander, allowZeroLength: Boolean, depth: Int) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Path = {
    val result = finder.findSinglePath(start, end)
    if (!allowZeroLength && result != null && result.length() == 0)
      null
    else
      result
  }

  def typ = CTPath
}

class AllShortestPathsStrategy(expander: Expander, allowZeroLength: Boolean, depth: Int) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Stream[Path] = {
    finder.findAllPaths(start, end).asScala.toStream
  }.filter { p => allowZeroLength || p.length() > 0 }

  def typ = CTCollection(CTPath)
}
