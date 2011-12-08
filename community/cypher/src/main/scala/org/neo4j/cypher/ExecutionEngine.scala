/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import commands._
import internal.ExecutionPlanImpl
import parser.CypherParser
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import java.lang.Error
import java.util.{Map => JavaMap}


class ExecutionEngine(graph: GraphDatabaseService) {
  checkScalaVersion()

  require(graph != null, "Can't work with a null graph database")

  val parser = new CypherParser()

  @throws(classOf[SyntaxException])
  def execute(query: String): ExecutionResult = execute(parser.parse(query))

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any]): ExecutionResult = {
    execute(parser.parse(query), params)
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExecutionResult = {
    execute(parser.parse(query), params.asScala.toMap)
  }

  @throws(classOf[SyntaxException])
  def execute(query: Query): ExecutionResult = execute(query, Map[String, Any]())

  // This is here to support Java people
  @throws(classOf[SyntaxException])
  def execute(query: Query, map: JavaMap[String, Any]): ExecutionResult = execute(query, map.asScala.toMap)


  @throws(classOf[SyntaxException])
  def execute(query: Query, params: Map[String, Any]): ExecutionResult =new ExecutionPlanImpl(query, graph).execute(params)


  def checkScalaVersion() {
    if (util.Properties.versionString.matches("^version 2.9.0")) {
      throw new Error("Cypher can only run with Scala 2.9.0. It looks like the Scala version is: " +
        util.Properties.versionString)
    }
  }

}

