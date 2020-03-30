/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.macros

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RequireTest extends CypherFunSuite {

  test("require with message") {
    assume(ASSERTIONS_ENABLED)

    the [AssertionError] thrownBy checkOnlyWhenAssertionsAreEnabled(false, "wut!") should have message "wut!"
  }

  test("require without message") {
    assume(ASSERTIONS_ENABLED)

    the [AssertionError] thrownBy checkOnlyWhenAssertionsAreEnabled(false) should have message "assertion failed"
  }

  test("require should not throw if assertions are disabled message") {
    assume(!ASSERTIONS_ENABLED)

    noException should be thrownBy checkOnlyWhenAssertionsAreEnabled(false)
  }



}
