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

import java.util

import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, ExpectError, Graph}

import scala.collection.JavaConverters._

class TCKTest {

  val tckSemanticFailures = Set("Multiple unwinds after each other", "Return all variables",
    "Copying properties from node with ON CREATE", "Copying properties from node with ON MATCH",
    "Using `keys()` on a parameter map", "Create a pattern with multiple hops in the reverse direction",
    "Create a pattern with multiple hops in varying directions",
    "Creating a pattern with multiple hops and changing directions",
    "Calling the same procedure twice using the same outputs in each call",
    "In-query call to VOID procedure that takes no arguments",
    "In-query call to procedure that takes no arguments and yields no results",
    "In-query call to procedure with explicit arguments that drops all result fields",
    "Standalone call to procedure with argument of type INTEGER accepts value of type FLOAT",
    "In-query call to procedure with argument of type INTEGER accepts value of type FLOAT")

  @TestFactory
  def runTCKTests(): util.Collection[DynamicTest] = {
    val scenarios = CypherTCK.allTckScenarios.filterNot(_.steps.exists(_.isInstanceOf[ExpectError]))
      .filterNot(scenario => tckSemanticFailures.contains(scenario.name))

    def createTestGraph: Graph = {
      Neo4jAdapter()
    }

    val dynamicTests = scenarios.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(createTestGraph)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

}
