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
package org.neo4j.cypher.internal.macros

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RequireTest extends CypherFunSuite {

  test("require with message") {
    assume(ASSERTIONS_ENABLED)

    the[AssertionError] thrownBy checkOnlyWhenAssertionsAreEnabled(false, "wut!") should have message "wut!"
  }

  test("require without message") {
    assume(ASSERTIONS_ENABLED)

    the[AssertionError] thrownBy checkOnlyWhenAssertionsAreEnabled(false) should have message "assertion failed"
  }

  test("require should not throw if assertions are disabled message") {
    assume(!ASSERTIONS_ENABLED)

    noException should be thrownBy checkOnlyWhenAssertionsAreEnabled(false)
  }

}
