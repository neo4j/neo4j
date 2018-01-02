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

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.commands.Pattern

/**
 * A Trail is a intermediate data structure used by TraversalMatcher to produce the ExpanderSteps -> toSteps(..)
 *
 * It's also used to go from the matched paths to execution contexts.
 *
 * TODO: This is a blatant breaking of SRP. Uncle Bob would be disappointed.
 */
abstract class Trail {
  def pathDescription: Seq[String]

  def start: String

  def end: String

  def isEndPoint: Boolean

  def size: Int

  def toSteps(id: Int): Option[ExpanderStep]

  def decompose(p: Seq[PropertyContainer]): Iterator[Map[String, Any]] = decompose(p, Map.empty).map(_._2)

  protected[matching] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]): Iterator[(Seq[PropertyContainer], Map[String, Any])]

  def symbols(table: SymbolTable): SymbolTable

  def contains(target: String): Boolean

  /**
   * @return The original predicates that this Trail uses
   */
  def predicates: Seq[Seq[Predicate]]

  def patterns: Seq[Pattern]

  def nodeNames: Seq[String]

  def add(f: String => Trail): Trail

  def filter(f: Trail => Boolean): Iterable[Trail]

  def asSeq: Seq[Trail] = filter(x => true).toSeq
}
