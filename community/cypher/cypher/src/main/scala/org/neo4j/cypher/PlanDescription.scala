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
package org.neo4j.cypher
/**
 * Abstract description of an execution plan
 * @deprecated See { @link org.neo4j.graphdb.ExecutionPlanDescription}, and use
 * { @link org.neo4j.graphdb.GraphDatabaseService#execute(String, Map)} instead.
 */
@Deprecated
trait PlanDescription {
  self =>

  def name: String
  def arguments: Map[String, AnyRef]

  // TODO: These two methods need default implementations in order to be useable from cypher-compiler-1.9
  // TODO: Remove the implementation once we drop support for cypher-compiler-1.9
  def children: Seq[PlanDescription] = throw new UnsupportedOperationException("This should not have been called")
  def hasProfilerStatistics: Boolean = throw new UnsupportedOperationException("This should not have been called")

  def asJava: javacompat.PlanDescription

  def render(builder: StringBuilder) {}
  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}
}
