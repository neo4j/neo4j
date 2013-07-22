/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import expressions.Expression
import expressions.Identifier._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.pipes.matching.MatchingContext
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState

case class PatternPredicate(pathPattern: Seq[Pattern], predicate:Predicate = True()) extends Predicate
  with PathExtractor
  with PatternGraphBuilder {
  val identifiers: Seq[(String, CypherType)] = pathPattern.flatMap(pattern => pattern.possibleStartPoints.filter(p => isNamed(p._1)))

  val symbols2 = SymbolTable(identifiers.toMap)
  val matchingContext = new MatchingContext(symbols2, predicate.atoms, buildPatternGraph(symbols2, pathPattern))
  val interestingPoints: Seq[String] = pathPattern.
    flatMap(_.possibleStartPoints.map(_._1)).
    filter(isNamed).
    distinct

  def isMatch(ctx: ExecutionContext)(implicit state: QueryState): Boolean = {
    // If any of the points we need is null, the predicate will be false
    val returnNull = interestingPoints.exists(key => ctx.get(key) match {
      case None       => throw new ThisShouldNotHappenError("Andres", "This execution plan should not exist.")
      case Some(null) => true
      case Some(x)    => false
    })

    if (returnNull) {
      false
    } else {
      matchingContext.getMatches(ctx, state).nonEmpty
    }
  }

  def children = pathPattern

  def rewrite(f: (Expression) => Expression): Predicate =
    PatternPredicate(pathPattern.map(_.rewrite(f)), predicate.rewrite(f))

  def symbolTableDependencies = {
    val patternDependencies = pathPattern.flatMap(_.symbolTableDependencies).toSet
    val startPointDependencies = pathPattern.flatMap(_.possibleStartPoints).map(_._1).filter(isNamed).toSet
    patternDependencies ++ startPointDependencies
  }

  override def toString() = s"PatternPredicate(${pathPattern.mkString(",")}, ${predicate}})"

  def containsIsNull = false

  def assertInnerTypes(symbols: SymbolTable) {
    pathPattern.foreach( _.throwIfSymbolsMissing(symbols) )
    val symbolsForExecution: SymbolTable = symbols.add(pathPattern.flatMap(_.possibleStartPoints).toMap)
    predicate.assertInnerTypes(symbolsForExecution)
  }
}