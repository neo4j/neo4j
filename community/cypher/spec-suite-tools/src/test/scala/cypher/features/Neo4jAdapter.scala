/*
 * Copyright (c) "Neo4j"
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

import cypher.features.Neo4jExceptionToExecutionFailed.convert
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.cypher.testing.impl.driver.DriverCypherExecutorFactory
import org.neo4j.cypher.testing.impl.embedded.EmbeddedCypherExecutorFactory
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.opencypher.tools.tck.api.ExecQuery
import org.opencypher.tools.tck.api.Graph
import org.opencypher.tools.tck.api.QueryType
import org.opencypher.tools.tck.api.StringRecords
import org.opencypher.tools.tck.values.CypherValue

import java.lang.Boolean.TRUE
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Neo4jAdapter {

  val defaultTestConfig: String => collection.Map[Setting[_], Object] = featureName => defaultTestConfigValues ++ featureDependentSettings(featureName)

  val defaultTestConfigValues: collection.Map[Setting[_], Object] = Map[Setting[_], Object](cypher_hints_error -> TRUE)

  def featureDependentSettings(featureName: String): collection.Map[Setting[_], Object] = featureName match {
    case "RelationshipPatternPredicates" => Map[Setting[_], Object](
      GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features -> Set(SemanticFeature.RelationshipPatternPredicates.productPrefix).asJava,
    )
    case _ => Map.empty
  }

  def apply(executionPrefix: String, graphDatabaseFactory: TestDatabaseManagementServiceBuilder,
            dbConfig: collection.Map[Setting[_], Object], useBolt: Boolean): Neo4jAdapter = {
    val enhancedConfig =
      (dbConfig ++ _root_.scala.collection.Map(BoltConnector.enabled -> true) ++ _root_.scala.collection.Map(BoltConnector.listen_address -> new SocketAddress("localhost", 0)))
        .asInstanceOf[Map[Setting[_], Object]]
    val managementService = createManagementService(enhancedConfig, graphDatabaseFactory)
    val config = Config.newBuilder().set(enhancedConfig.asJava).build()
    val executorFactory = if (useBolt) {
      DriverCypherExecutorFactory(managementService, config)
    } else {
      EmbeddedCypherExecutorFactory(managementService, config)
    }
    val dbms = FeatureDatabaseManagementService(managementService, executorFactory)
    new Neo4jAdapter(dbms, executionPrefix)
  }

  private def createManagementService(config: collection.Map[Setting[_], Object], graphDatabaseFactory: TestDatabaseManagementServiceBuilder) = {
    graphDatabaseFactory.impermanent().setConfig(config.asJava).build()
  }
}

class Neo4jAdapter(var dbms: FeatureDatabaseManagementService,
                   executionPrefix: String) extends Graph with Neo4jProcedureAdapter with Neo4jCsvFileCreationAdapter {
  private val explainPrefix = "EXPLAIN\n"

  override def cypher(query: String, params: Map[String, CypherValue], meta: QueryType): Result = {
    val neo4jParams = params.view.mapValues(v => TCKValueToNeo4jValue(v)).toMap

    val queryToExecute = if (meta == ExecQuery) {
      s"$executionPrefix $query"
    } else query
    Try(dbms.execute(queryToExecute, neo4jParams, convertResult)) match {
      case Success(converted) =>
        converted
      case Failure(exception) =>
        val tx = dbms.begin()
        val explainedResult = Try(tx.execute(explainPrefix + queryToExecute, neo4jParams).consume())
        val phase = explainedResult match {
          case Failure(_) => Phase.compile
          case Success(_) => Phase.runtime
        }
        Try(tx.rollback()) match {
          case Failure(exception) => convert(Phase.runtime, exception)
          case Success(_)         => convert(phase, exception)
        }
    }
  }

  def convertResult(result: StatementResult): Result = {
    val header = result.columns().toList
    val rows = result.records().map(record => record.map { case (key, value) => (key, CypherTestValueToString(value)) }).toList
    StringRecords(header, rows)
  }

  override def close(): Unit = {
    deleteTemporaryFiles()
    dbms.shutdown()
  }
}
