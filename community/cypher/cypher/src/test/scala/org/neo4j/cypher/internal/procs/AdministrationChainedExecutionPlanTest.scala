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
package org.neo4j.cypher.internal.procs

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.graphdb.config.Setting

import scala.jdk.CollectionConverters.MapHasAsJava

class AdministrationChainedExecutionPlanTest extends CommunityCypherTestSuite {

  private def preParserWith(fallback: CypherPipelinedInterpretedPipesFallback): CachingPreParser =
    new CachingPreParser(
      CypherConfiguration.fromConfig(
        Config.defaults(Map[Setting[_], AnyRef](
          GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> fallback
        ).asJava)
      ),
      new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0)
    )

  // ChainedExecutionPlan.formatQuery prepends `runtime=slotted` to every admin sub-query.
  // The operator-configurable internal.cypher.pipelined_interpreted_pipes_fallback can be set
  // to a value the CypherQueryOptions validator rejects in combination with slotted (disabled,
  // whitelisted_plans_only, all). Without `interpretedPipesFallback=default` in the prepended
  // options every admin command would abort on such a DBMS during pre-parse.
  for (
    fallback <- Seq(
      CypherPipelinedInterpretedPipesFallback.DEFAULT,
      CypherPipelinedInterpretedPipesFallback.DISABLED,
      CypherPipelinedInterpretedPipesFallback.WHITELISTED_PLANS_ONLY,
      CypherPipelinedInterpretedPipesFallback.ALL
    )
  ) {
    test(
      s"admin sub-query formatted by formatQuery pre-parses under cypher_pipelined_interpreted_pipes_fallback=$fallback"
    ) {
      val formatted = AdministrationChainedExecutionPlan.formatQuery("CREATE DATABASE foo", CypherVersion.Cypher25)
      val parsed = preParserWith(fallback).preParseQuery(formatted, devNullLogger, CypherVersion.Cypher25)

      parsed.options.queryOptions.runtime shouldEqual CypherRuntimeOption.slotted
      parsed.options.queryOptions.interpretedPipesFallback shouldEqual CypherInterpretedPipesFallbackOption.default
    }
  }
}
