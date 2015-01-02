/**
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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.{SingleNode, Pattern, PathExtractor, ShortestPath}
import pipes.QueryState
import symbols._
import org.neo4j.cypher.SyntaxException
import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.graphdb.{Path, DynamicRelationshipType, Node, Expander}
import org.neo4j.kernel.Traversal
import collection.Map
import scala.collection.JavaConverters._

case class ShortestPathExpression(ast: ShortestPath) extends Expression with PathExtractor {
  val pathPattern:Seq[Pattern] = Seq(ast)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    if (anyStartpointsContainNull(ctx)) {
      Stream(null)
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
    new SingleShortestPathStrategy(expander, ast.maxDepth.getOrElse(15))
  else
    new AllShortestPathsStrategy(expander, ast.maxDepth.getOrElse(15))

  def calculateType(symbols: SymbolTable) =  shortestPathStrategy.typ

  def symbolTableDependencies = ast.symbolTableDependencies + ast.left.name + ast.right.name
}

trait ShortestPathStrategy {
  def findResult(start: Node, end: Node): Any
  def typ: CypherType
}

class SingleShortestPathStrategy(expander: Expander, depth: Int) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Path = finder.findSinglePath(start, end)

  def typ = CTPath
}

class AllShortestPathsStrategy(expander: Expander, depth: Int) extends ShortestPathStrategy {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Stream[Path] = {
    finder.findAllPaths(start, end).asScala.toStream
  }

  def typ = CTCollection(CTPath)
}
