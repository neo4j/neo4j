/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.frontend.phases

import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.frontend.helpers.{TestContext, TestState}
import org.neo4j.cypher.internal.v4_0.parser.ParserFixture.parser
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations.{V1, V2}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{DeprecatedFunctionNotification, InputPosition, OpenCypherExceptionFactory}

class SyntaxDeprecationWarningsTest extends CypherFunSuite {

  test("should warn about V1 deprecations") {
    check(V1, "RETURN timestamp()") shouldBe empty
  }

  test("should warn about V2 deprecations") {
    // TODO: add some example here once we have any new V2 deprecations
  }

  private def check(deprecations: Deprecations, query: String) = {
    val logger = new RecordingNotificationLogger()
    SyntaxDeprecationWarnings(deprecations).visit(TestState(Some(parse(query))), TestContext(logger))
    logger.notifications
  }

  private def parse(queryText: String): Statement = parser.parse(queryText.replace("\r\n", "\n"), OpenCypherExceptionFactory(None))

}
