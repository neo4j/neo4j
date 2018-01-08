/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import cypher.features.Neo4jExceptionToExecutionFailed._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.{ConstraintViolationException, QueryExecutionException, Result => Neo4jResult}
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Neo4jAdapter {
  def apply(): Neo4jAdapter = {
    val service = new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase())
    new Neo4jAdapter(service)
  }
}

class Neo4jAdapter(service: GraphDatabaseCypherService) extends Graph with Neo4jProcedureAdapter {
  protected val instance: GraphDatabaseFacade = service.getGraphDatabaseService

  private val explainPrefix = "EXPLAIN\n"

  override def cypher(query: String, params: Map[String, CypherValue], meta: QueryType): Result = {
    val neo4jParams = params.mapValues(v => TCKValueToNeo4jValue(v)).asJava

    val tx = instance.beginTx
    val result: Result = Try(instance.execute(query, neo4jParams)) match {
      case Success(r) =>
        Try(convertResult(r)) match {
          case Failure(exception) => convert(Phase.runtime, exception)
          case Success(converted) =>
            tx.success
            converted
        }
      case Failure(exception) =>
        val explainedResult = Try(instance.execute(explainPrefix + query))
        val phase = explainedResult match {
          case Failure(_) => Phase.compile
          case Success(_) => Phase.runtime
        }
        tx.failure
        convert(phase, exception)
    }
    Try(tx.close) match {
      case Failure(exception) =>
        convert(Phase.runtime, exception)
      case Success(_) => result
    }
  }

  def convertResult(result: Neo4jResult): Result = {
    val header = result.columns().asScala.toList
    val rows: List[Map[String, String]] = result.asScala.map {
      row =>
        row.asScala.map {
          case (k, v) => (k, Neo4jValueToString(v))
        }.toMap
    }.toList
    StringRecords(header, rows)
  }

  override def close(): Unit = {
    instance.shutdown()
  }

}

case class Neo4jExecutionException(
                                    query: String,
                                    params: Map[String, CypherValue],
                                    meta: QueryType, msg: String)
  extends Exception(s"Error when executing $meta query $query with params $params: $msg")

case class Neo4jValueConversionException(
                                          query: String,
                                          params: Map[String, CypherValue],
                                          meta: QueryType, msg: String
                                        )
  extends Exception(s"Error when converting result values for $meta query $query with params $params: $msg")
