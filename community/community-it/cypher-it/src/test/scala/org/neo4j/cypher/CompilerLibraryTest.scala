/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.configuration.Config
import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.cypher.internal.CommunityCompilerFactory
import org.neo4j.cypher.internal.Compiler
import org.neo4j.cypher.internal.CompilerLibrary
import org.neo4j.cypher.internal.CypherCurrentCompiler
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.logging.NullLogProvider
import org.neo4j.values.virtual.MapValue

class CompilerLibraryTest extends CypherITTestSuite with GraphDatabaseTestSupport {

  private def newCompilerLibrary = {
    val resolver = graph.getDependencyResolver
    val config = resolver.resolveDependency(classOf[Config])
    val cypherConfig = CypherConfiguration.fromConfig(config)
    val monitors = kernelMonitors
    val queryCaches = new CypherQueryCaches(
      CypherQueryCaches.Config.fromCypherConfiguration(cypherConfig),
      LastCommittedTxIdProvider(graph),
      TestExecutorCaffeineCacheFactory,
      MasterCompiler.CLOCK,
      monitors,
      logProvider
    )
    val nullLogProvider = NullLogProvider.getInstance
    val compilerFactory =
      new CommunityCompilerFactory(
        graph,
        monitors,
        nullLogProvider,
        CypherParsingConfig.fromCypherConfiguration(cypherConfig),
        CypherPlannerConfiguration.fromCypherConfiguration(
          cypherConfig,
          config,
          planSystemCommands = false,
          targetsComposite = false
        ),
        CypherRuntimeConfiguration.fromCypherConfiguration(cypherConfig),
        queryCaches
      )
    new CompilerLibrary(compilerFactory, () => null)
  }

  private def expectedItemsInTheASTCache(compiler: Compiler, expected: Integer) = {
    compiler match {
      case c: CypherCurrentCompiler[_] => c.planner.astCacheSize should be(expected)
      case _                           => fail()
    }
  }

  private def astContains(compiler: Compiler, key: PreParsedQuery, value: BaseState) = {
    val astKey = CypherQueryCaches.astKey(preParsedQuery = key, MapValue.EMPTY, useParameterSizeHint = false)
    compiler match {
      case c: CypherCurrentCompiler[_] => value should be(c.planner.getFromAstCache(astKey).get.parsedQuery)
      case _                           => fail()
    }
  }

  test("CompilerLibrary.insertIntoCache should only insert into the AST cache for the relevant compiler") {
    val compilerLibrary = newCompilerLibrary

    val cDefaultDefault = compilerLibrary.selectCompiler(
      CypherPlannerOption.default,
      CypherRuntimeOption.default,
      materializedEntitiesMode = false
    )
    val cDpDefault = compilerLibrary.selectCompiler(
      CypherPlannerOption.dp,
      CypherRuntimeOption.default,
      materializedEntitiesMode = false
    )
    val cDefaultPipelined = compilerLibrary.selectCompiler(
      CypherPlannerOption.default,
      CypherRuntimeOption.pipelined,
      materializedEntitiesMode = false
    )
    val cDefaultSlotted = compilerLibrary.selectCompiler(
      CypherPlannerOption.default,
      CypherRuntimeOption.slotted,
      materializedEntitiesMode = false
    )

    val preParser = new PreParser(CypherConfiguration.fromConfig(Config.newBuilder().build()))
    val preparedQuery1 = preParser.preParse("MATCH (a:A) RETURN a;", CypherVersion.Cypher25)
    val preparedQuery1_2 = preParser.preParse("MATCH (a:A) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState1 = InitialState("Dummy query string 1", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery2 = preParser.preParse("MATCH (a:B) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState2 = InitialState("Dummy query string 2", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery3 = preParser.preParse("CYPHER planner=dp MATCH (a:A) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState3 = InitialState("Dummy query string 3", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery4 = preParser.preParse("CYPHER planner=dp MATCH (a:B) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState4 = InitialState("Dummy query string 4", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery5 = preParser.preParse("CYPHER planner=dp MATCH (a:C) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState5 = InitialState("Dummy query string 5", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery6 = preParser.preParse("CYPHER runtime=slotted MATCH (a:A) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState6 = InitialState("Dummy query string 6", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery7 = preParser.preParse("CYPHER runtime=slotted MATCH (a:B) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState7 = InitialState("Dummy query string 7", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery8 = preParser.preParse("CYPHER runtime=slotted MATCH (a:C) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState8 = InitialState("Dummy query string 8", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery9 = preParser.preParse("CYPHER runtime=slotted MATCH (a:D) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState9 = InitialState("Dummy query string 9", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery10 = preParser.preParse("CYPHER runtime=pipelined MATCH (a:A) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState10 = InitialState("Dummy query string 10", IDPPlannerName, new AnonymousVariableNameGenerator)

    val preparedQuery11 =
      preParser.preParse("CYPHER planner=dp runtime=slotted MATCH (a:D) RETURN a;", CypherVersion.Cypher25)
    val dummyBaseState11 = InitialState("Dummy query string 11", IDPPlannerName, new AnonymousVariableNameGenerator)

    // cDefaultDefault
    compilerLibrary.insertIntoCache(preparedQuery1, MapValue.EMPTY, dummyBaseState1, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery1_2, MapValue.EMPTY, dummyBaseState1, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery2, MapValue.EMPTY, dummyBaseState2, Set.empty)
    // cDpDefault
    compilerLibrary.insertIntoCache(preparedQuery3, MapValue.EMPTY, dummyBaseState3, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery3, MapValue.EMPTY, dummyBaseState3, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery4, MapValue.EMPTY, dummyBaseState4, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery5, MapValue.EMPTY, dummyBaseState5, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery4, MapValue.EMPTY, dummyBaseState4, Set.empty)
    // cDefaultSlotted
    compilerLibrary.insertIntoCache(preparedQuery6, MapValue.EMPTY, dummyBaseState6, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery7, MapValue.EMPTY, dummyBaseState7, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery8, MapValue.EMPTY, dummyBaseState8, Set.empty)
    compilerLibrary.insertIntoCache(preparedQuery9, MapValue.EMPTY, dummyBaseState9, Set.empty)
    // cDefaultPipelined
    compilerLibrary.insertIntoCache(preparedQuery10, MapValue.EMPTY, dummyBaseState10, Set.empty)
    // cDpSlotted (there is no compiler for this in the compiler library, so it will not be inserted in any cache)
    compilerLibrary.insertIntoCache(preparedQuery11, MapValue.EMPTY, dummyBaseState11, Set.empty)

    expectedItemsInTheASTCache(cDefaultDefault, 2) // 1, 2
    astContains(cDefaultDefault, preparedQuery1, dummyBaseState1)
    astContains(cDefaultDefault, preparedQuery2, dummyBaseState2)

    expectedItemsInTheASTCache(cDpDefault, 3) // 3, 4, 5
    astContains(cDpDefault, preparedQuery3, dummyBaseState3)
    astContains(cDpDefault, preparedQuery4, dummyBaseState4)
    astContains(cDpDefault, preparedQuery5, dummyBaseState5)

    expectedItemsInTheASTCache(cDefaultSlotted, 4) // 6, 7, 8, 9
    astContains(cDefaultSlotted, preparedQuery6, dummyBaseState6)
    astContains(cDefaultSlotted, preparedQuery7, dummyBaseState7)
    astContains(cDefaultSlotted, preparedQuery8, dummyBaseState8)
    astContains(cDefaultSlotted, preparedQuery9, dummyBaseState9)

    expectedItemsInTheASTCache(cDefaultPipelined, 1) // 10
    astContains(cDefaultPipelined, preparedQuery10, dummyBaseState10)
  }
}
