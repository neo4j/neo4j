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
package org.neo4j.cypher.internal.compiler.v3_2.pipes.matching

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

import scala.collection.immutable

/**
 * This class is responsible for deciding how to get the parts of the pattern that are not already bound
 *
 * The deciding factor is whether or not the pattern has loops in it. If it does, we have to use the much more
 * expensive pattern matching. If it doesn't, we get away with much simpler methods
 */
class MatchingContext(boundVariables: SymbolTable,
                      predicates: Seq[Predicate] = Seq(),
                      patternGraph: PatternGraph,
                      variablesInClause: Set[String]) {

  val builder: MatcherBuilder = decideWhichMatcherToUse()

  private def variables: immutable.Map[String, CypherType] =
    patternGraph.patternRels.values.flatMap(p => p.flatMap(_.variables2)).toMap

  lazy val symbols = {
    val ids = variables

    val variablesAlreadyInContext = ids.filter(variable => boundVariables.hasVariableNamed(variable._1))

    variablesAlreadyInContext.foreach( variable => boundVariables.evaluateType(variable._1, variable._2) )

    boundVariables.add(ids)
  }

  def getMatches(sourceRow: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    builder.getMatches(sourceRow, state)
  }

  private def decideWhichMatcherToUse(): MatcherBuilder = {
      new PatternMatchingBuilder(patternGraph, predicates, variablesInClause)
  }
}

trait MatcherBuilder {
  def name: String
  def startPoint: String
  def getMatches(sourceRow: ExecutionContext, state: QueryState): Traversable[ExecutionContext]
}

