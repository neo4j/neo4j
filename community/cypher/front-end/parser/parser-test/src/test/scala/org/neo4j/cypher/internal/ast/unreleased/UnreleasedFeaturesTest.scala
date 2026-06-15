/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.unreleased

import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_enable_local_callables
import org.neo4j.cypher.internal.ast.CypherParserTestSuite
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.ast.factory.Cypher25ErrorStrategyConf

import scala.jdk.CollectionConverters.SetHasAsScala

class UnreleasedFeaturesTest extends CypherParserTestSuite {

  // When this test breaks: Update Cypher25ErrorStrategyConf().ignoredTokens to make it pass, then remove the test.
  test("ignore DEFINE keyword as long as it's unreleased") {
    val ignoredTokens = new Cypher25ErrorStrategyConf().ignoredTokens.asScala
    if (cypher_enable_local_callables.defaultValue()) {
      withClue(s"You need to remove DEFINE from Cypher25ErrorStrategyConf.ignoredTokens!") {
        ignoredTokens should not contain Cypher25Parser.DEFINE
      }
    }
  }
}
