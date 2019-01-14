/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package cypher.features

import cypher.features.Neo4jExceptionToExecutionFailed._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_hints_error
import org.neo4j.graphdb.{Result => Neo4jResult}
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object Neo4jAdapter {
  def apply(executionPrefix: String, graphDatabaseFactory: TestGraphDatabaseFactory,
            dbConfig: collection.Map[Setting[_], String] = Map[Setting[_], String](cypher_hints_error -> "true")): Neo4jAdapter = {
    val service = createGraphDatabase(dbConfig, graphDatabaseFactory)
    new Neo4jAdapter(service, executionPrefix)
  }

  private def createGraphDatabase(config: collection.Map[Setting[_], String], graphDatabaseFactory: TestGraphDatabaseFactory): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(graphDatabaseFactory.newImpermanentDatabase(config.asJava))
  }
}

class Neo4jAdapter(service: GraphDatabaseCypherService, executionPrefix: String) extends Graph with Neo4jProcedureAdapter {
  protected val instance: GraphDatabaseFacade = service.getGraphDatabaseService

  private val explainPrefix = "EXPLAIN\n"

  override def cypher(query: String, params: Map[String, CypherValue], meta: QueryType): Result = {
    val neo4jParams = params.mapValues(v => TCKValueToNeo4jValue(v)).asJava

    val tx = instance.beginTx
    val queryToExecute = if (meta == ExecQuery) {
      s"$executionPrefix $query"
    } else query
    val result: Result = Try(instance.execute(queryToExecute, neo4jParams)).flatMap(r => Try(convertResult(r))) match {
      case Success(converted) =>
        tx.success()
        converted
      case Failure(exception) =>
        val explainedResult = Try(instance.execute(explainPrefix + queryToExecute))
        val phase = explainedResult match {
          case Failure(_) => Phase.compile
          case Success(_) => Phase.runtime
        }
        tx.failure()
        convert(phase, exception)
    }
    Try(tx.close()) match {
      case Failure(exception) =>
        convert(Phase.runtime, exception)
      case Success(_) => result
    }
  }

  def convertResult(result: Neo4jResult): Result = {
    val header = result.columns().asScala.toList
    val rows = ArrayBuffer[Map[String, String]]()
    result.accept(new ResultVisitor[RuntimeException] {
      override def visit(row: Neo4jResult.ResultRow): Boolean = {
        rows.append(header.map(k => k -> Neo4jValueToString(row.get(k))).toMap)
        true
      }
    })
    StringRecords(header, rows.toList)
  }

  override def close(): Unit = {
    instance.shutdown()
  }

}
