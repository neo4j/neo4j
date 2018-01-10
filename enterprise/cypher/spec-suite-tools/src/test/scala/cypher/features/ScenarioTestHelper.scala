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

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util

import org.junit.jupiter.api.DynamicTest
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.opencypher.tools.tck.api.{Graph, Scenario}

import scala.collection.JavaConverters._
import scala.io.Source

object ScenarioTestHelper {

  def createTests(scenarios: Seq[Scenario], blacklist : Set[String] = Set.empty, config: Map[Setting[_], String] = Map()) : util.Collection[DynamicTest] = {
    println("Total number of scenarios: " + scenarios.size)
    val filteredScenarios = scenarios.filter(s => blacklist.contains(s.name))
    println("Blacklisted scenarios: " + blacklist.size) // TODO: add SemanticFailures blacklists to actual blacklists
    println("Executing " + filteredScenarios.size +  " scenarios now")

    val dynamicTests: Seq[DynamicTest] = filteredScenarios.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(createTestGraph(config))
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }


  def parseBlacklist(blacklistFile: String) : Set[String] = {
    def validate(scenarioName : String) : Unit = {
      if(scenarioName.head.isWhitespace || scenarioName.last.isWhitespace) {
        throw new Exception(s"Invalid whitespace in scenario name $scenarioName from file $blacklistFile")
      }
    }
    val uri = new URI("/blacklists/" + blacklistFile)
    // This URI is only valid for the project we are in. currently this is acceptance tests!
    // TODO: move TCKTests to compatability project

    val url = getClass.getResource(uri.getPath)
    if (url == null) throw new FileNotFoundException(s"blacklist file not found at: $blacklistFile")
    val lines = Source.fromFile(url.getPath, StandardCharsets.UTF_8.name()).getLines()
    val scenarios = lines.filterNot(line => line.startsWith("//") || line.isEmpty).toSet // comments in blacklist are being ignored
    scenarios.foreach(validate)
    scenarios
  }

  def createTestGraph(config: Map[Setting[_], String] = Map()): Graph = {
    val db = createGraphDatabase(config)
    new Neo4jAdapter(db)
  }

  protected def createGraphDatabase(config: collection.Map[Setting[_], String]): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
  }
}
