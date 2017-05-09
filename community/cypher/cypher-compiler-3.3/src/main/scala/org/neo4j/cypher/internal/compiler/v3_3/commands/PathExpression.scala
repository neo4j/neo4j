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
package org.neo4j.cypher.internal.compiler.v3_3.commands

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_3.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_3.pipes.matching.MatchingContext
import org.neo4j.cypher.internal.compiler.v3_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_3.helpers.UnNamedNameGenerator.isNamed
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

/*
This class does pattern matching inside an Expression. It's used as a fallback when the
expression cannot be unnested from inside an expression. It is used for pattern expressions
and pattern comprehension
 */
case class PathExpression(pathPattern: Seq[Pattern], predicate: Predicate,
                          projection: Expression, allowIntroducingNewIdentifiers: Boolean = false)
  extends Expression with PatternGraphBuilder {
  private val variables: Seq[(String, CypherType)] =
    pathPattern.flatMap(pattern => pattern.possibleStartPoints.filter(p => isNamed(p._1)))
  private val symbols2 = SymbolTable(variables.toMap)
  private val variablesInClause = Pattern.variables(pathPattern)

  private val matchingContext = new MatchingContext(symbols2, predicate.atoms, buildPatternGraph(symbols2, pathPattern), variablesInClause)
  private val interestingPoints: Seq[String] = pathPattern.
    flatMap(_.possibleStartPoints.map(_._1)).
    filter(isNamed).
    distinct

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyRef = {
    // If any of the points we need is null, the whole expression will return null
    val returnNull = interestingPoints.exists(key => ctx.get(key) match {
      case Some(null) => true
      case None if !allowIntroducingNewIdentifiers =>
        throw new AssertionError("This execution plan should not exist.")
      case _ => false
    })

    if (returnNull) {
      null
    } else {
      matchingContext.
        getMatches(ctx, state). // find matching subgraphs
        filter(predicate.isTrue(_)(state)). // filter out graphs not matching the predicate
        map(projection.apply(_)(state)) // project from found subgraphs
    }
  }

  override def children = pathPattern :+ predicate

  override def arguments = Seq.empty

  override def rewrite(f: (Expression) => Expression) =
    f(PathExpression(pathPattern.map(_.rewrite(f)), predicate.rewriteAsPredicate(f), projection, allowIntroducingNewIdentifiers))

  override def symbolTableDependencies = {
    val patternDependencies = pathPattern.flatMap(_.symbolTableDependencies).toSet
    val startPointDependencies = pathPattern.flatMap(_.possibleStartPoints).map(_._1).filter(isNamed).toSet
    patternDependencies ++ startPointDependencies
  }

  override def toString() = s"PathExpression(${pathPattern.mkString(",")}, $predicate, $projection)"
}
