/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import collection.immutable

/**
 * This class is responsible for deciding how to get the parts of the pattern that are not already bound
 *
 * The deciding factor is whether or not the pattern has loops in it. If it does, we have to use the much more
 * expensive pattern matching. If it doesn't, we get away with much simpler methods
 */
class MatchingContext(boundIdentifiers: SymbolTable,
                      predicates: Seq[Predicate] = Seq(),
                      patternGraph: PatternGraph,
                      identifiersInClause: Set[String]) {

  val builder: MatcherBuilder = decideWhichMatcherToUse()

  private def identifiers: immutable.Map[String, CypherType] =
    patternGraph.patternRels.values.flatMap(p => p.flatMap(_.identifiers2)).toMap

  lazy val symbols = {
    val ids = identifiers

    val identifiersAlreadyInContext = ids.filter(identifier => boundIdentifiers.hasIdentifierNamed(identifier._1))

    identifiersAlreadyInContext.foreach( identifier => boundIdentifiers.evaluateType(identifier._1, identifier._2) )

    boundIdentifiers.add(ids)
  }

  def getMatches(sourceRow: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    builder.getMatches(sourceRow, state)
  }

  private def decideWhichMatcherToUse(): MatcherBuilder = {
    if(SimplePatternMatcherBuilder.canHandle(patternGraph)) {
      new SimplePatternMatcherBuilder(patternGraph, predicates, symbols, identifiersInClause)
    } else {
      new PatternMatchingBuilder(patternGraph, predicates, identifiersInClause)
    }
  }
}

trait MatcherBuilder {
  def name: String
  def startPoint: String
  def getMatches(sourceRow: ExecutionContext, state: QueryState): Traversable[ExecutionContext]
}

