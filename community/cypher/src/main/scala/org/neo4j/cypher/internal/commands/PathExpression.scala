/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.pipes.matching.MatchingContext
import collection.Map
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.Path
import org.neo4j.cypher.internal.executionplan.builders.PatternGraphBuilder

case class PathExpression(pathPattern: Seq[Pattern])
  extends Expression
  with PathExtractor
  with PatternGraphBuilder {
  val identifiers: Seq[(String, CypherType)] = pathPattern.flatMap(pattern => pattern.possibleStartPoints.filterNot(p => p._1.startsWith("  UNNAMED")))

  val symbols2 = new SymbolTable(identifiers.toMap)
  val matchingContext = new MatchingContext(symbols2, Seq(), buildPatternGraph(symbols2, pathPattern))
  val interestingPoints: Seq[String] = pathPattern.
    flatMap(_.possibleStartPoints.map(_._1)).
    filterNot(_.startsWith("  UNNAMED")).
    distinct

  def apply(m: Map[String, Any]): Any = {
    val returnNull = interestingPoints.exists(key => m.get(key) match {
      case None => throw new ThisShouldNotHappenError("Andres", "This execution plan should not exist.")
      case Some(null) => true
      case Some(x) => false
    })

    if (returnNull) {
      null
    } else {
      getMatches(m.filterKeys(interestingPoints.contains)) //Only pass on to the pattern matcher
    }                                                      //the points it should care about, nothing else.
  }

  def getMatches(v1: Map[String, Any]): Traversable[Path] = {
    val matches = matchingContext.getMatches(v1)
    matches.map(getPath)
  }

  def filter(f: (Expression) => Boolean): Seq[Expression] = Seq()

  def rewrite(f: (Expression) => Expression): Expression = f(PathExpression(pathPattern.map(_.rewrite(f))))

  def calculateType(symbols: SymbolTable): CypherType = {
    pathPattern.foreach(_.assertTypes(symbols))
    new CollectionType(PathType())
  }

  def symbolTableDependencies = {
    val patternDependencies = pathPattern.flatMap(_.symbolTableDependencies).toSet
    val startPointDependencies = pathPattern.flatMap(_.possibleStartPoints).map(_._1).filterNot(_.startsWith("  UNNAMED")).toSet
    patternDependencies ++ startPointDependencies
  }
}