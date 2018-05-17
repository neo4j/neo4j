/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.v3_5.NoHeaders
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.StringLiteral
import org.neo4j.cypher.internal.v3_5.logical.plans.{AllNodesScan, Eager, LoadCSV}
import org.opencypher.v9_0.util.EagerLoadCsvNotification

class CheckForEagerLoadCsvTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val url = StringLiteral("file:///tmp/foo.csv")(pos)

  test("should notify for EagerPipe on top of LoadCsvPipe") {
    val plan =
      Eager(
        LoadCSV(
          AllNodesScan("a", Set.empty),
          url,
          "foo",
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false
        )
      )

    checkForEagerLoadCsv(plan) should equal(Some(EagerLoadCsvNotification))
  }

  test("should not notify for LoadCsv on top of eager pipe") {
    val plan =
      LoadCSV(
        Eager(
          AllNodesScan("a", Set.empty)
        ),
        url,
        "foo",
        NoHeaders,
        None,
        legacyCsvQuoteEscaping = false
      )

    checkForEagerLoadCsv(plan) should equal(None)
  }
}
