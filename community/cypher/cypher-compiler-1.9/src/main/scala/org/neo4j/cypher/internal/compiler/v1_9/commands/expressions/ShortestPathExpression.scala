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
package org.neo4j.cypher.internal.compiler.v1_9.commands.expressions

import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import collection.Map
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphalgo.GraphAlgoFactory
import scala.collection.JavaConverters._
import org.neo4j.cypher.SyntaxException
import org.neo4j.kernel.Traversal
import org.neo4j.graphdb.{Path, DynamicRelationshipType, Node, Expander}
import org.neo4j.cypher.internal.compiler.v1_9.commands.{Pattern, PathExtractor, ShortestPath}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

case class ShortestPathExpression(ast: ShortestPath) extends Expression with PathExtractor {
  val pathPattern:Seq[Pattern] = Seq(ast)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Stream[Path] = {
    if (anyStartpointsContainNull(ctx)) {
      Stream(null)
    } else {
      getMatches(ctx)
    }
  }

  private def getMatches(m: Map[String, Any]): Stream[Path] = {
    val start = getEndPoint(m, ast.start)
    val end = getEndPoint(m, ast.end)
    foo.findResult(start, end)
  }

  def getEndPoint(m: Map[String, Any], start: String): Node = m.getOrElse(start, throw new SyntaxException("To find a shortest path, both ends of the path need to be provided. Couldn't find `" + start + "`")).asInstanceOf[Node]

  private def anyStartpointsContainNull(m: Map[String, Any]): Boolean =
    symbolTableDependencies.exists(key => m.get(key) match {
      case None => throw new ThisShouldNotHappenError("Andres", "This execution plan should not exist.")
      case Some(null) => true
      case Some(x) => false
    })

  def children = Seq(ast)

  def rewrite(f: (Expression) => Expression): Expression = f(ShortestPathExpression(ast.rewrite(f)))

  private lazy val expander: Expander = if (ast.relTypes.isEmpty) {
    Traversal.expanderForAllTypes(ast.dir)
  } else {
    ast.relTypes.foldLeft(Traversal.emptyExpander()) {
      case (e, t) => e.add(DynamicRelationshipType.withName(t), ast.dir)
    }
  }

  val foo = if (ast.single)
    new SingleShortestPathFOO(expander, ast.maxDepth.getOrElse(15))
  else
    new AllShortestPathsFOO(expander, ast.maxDepth.getOrElse(15))

  def calculateType(symbols: SymbolTable) = {
    ast.throwIfSymbolsMissing(symbols)
    PathType()
  }

  def symbolTableDependencies = ast.symbolTableDependencies
}

trait FOO {
  def findResult(start: Node, end: Node): Stream[Path]
}

class SingleShortestPathFOO(expander: Expander, depth: Int) extends FOO {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Stream[Path] = {
    Option(finder.findSinglePath(start, end)).toStream
  }
}

class AllShortestPathsFOO(expander: Expander, depth: Int) extends FOO {
  private val finder = GraphAlgoFactory.shortestPath(expander, depth)

  def findResult(start: Node, end: Node): Stream[Path] = {
    finder.findAllPaths(start, end).asScala.toStream
  }
}
