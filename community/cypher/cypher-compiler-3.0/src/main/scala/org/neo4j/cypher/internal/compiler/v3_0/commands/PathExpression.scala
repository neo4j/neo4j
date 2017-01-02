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
package org.neo4j.cypher.internal.compiler.v3_0.commands

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.compiler.v3_0.helpers.UnNamedNameGenerator.isNamed
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.MatchingContext
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

case class PathExpression(pathPattern: Seq[Pattern], predicate:Predicate=True())
  extends Expression
  with PathExtractor
  with PatternGraphBuilder {
  val variables: Seq[(String, CypherType)] = pathPattern.flatMap(pattern => pattern.possibleStartPoints.filter(p => isNamed(p._1)))
  val symbols2 = SymbolTable(variables.toMap)
  val variablesInClause = Pattern.variables(pathPattern)

  val matchingContext = new MatchingContext(symbols2, predicate.atoms, buildPatternGraph(symbols2, pathPattern), variablesInClause)
  val interestingPoints: Seq[String] = pathPattern.
    flatMap(_.possibleStartPoints.map(_._1)).
    filter(isNamed).
    distinct

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    // If any of the points we need is null, the whole expression will return null
    val returnNull = interestingPoints.exists(key => ctx.get(key) match {
      case None       => throw new AssertionError("This execution plan should not exist.")
      case Some(null) => true
      case Some(_)    => false
    })

    if (returnNull) {
      null
    } else {
      matchingContext.getMatches(ctx, state).map(getPath)
    }
  }

  override def children = pathPattern :+ predicate

  def arguments: Seq[Expression] = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(PathExpression(pathPattern.map(_.rewrite(f)), predicate.rewriteAsPredicate(f)))

  def calculateType(symbols: SymbolTable): CypherType = CTList(CTPath)

  def symbolTableDependencies = {
    val patternDependencies = pathPattern.flatMap(_.symbolTableDependencies).toSet
    val startPointDependencies = pathPattern.flatMap(_.possibleStartPoints).map(_._1).filter(isNamed).toSet
    patternDependencies ++ startPointDependencies
  }

  override def toString() = s"PathExpression(${pathPattern.mkString(",")}, $predicate)"
}
