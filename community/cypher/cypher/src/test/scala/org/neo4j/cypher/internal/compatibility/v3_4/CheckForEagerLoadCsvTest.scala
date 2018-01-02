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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.notification.EagerLoadCsvNotification
import org.neo4j.cypher.internal.ir.v3_4.{IdName, NoHeaders}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.StringLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans.{AllNodesScan, Eager, LoadCSV}

class CheckForEagerLoadCsvTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val url = StringLiteral("file:///tmp/foo.csv")(pos)

  test("should notify for EagerPipe on top of LoadCsvPipe") {
    val plan =
      Eager(
        LoadCSV(
          AllNodesScan(IdName("a"), Set.empty)(solved),
          url,
          IdName("foo"),
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false
        )(solved)
      )(solved)

    checkForEagerLoadCsv(plan) should equal(Some(EagerLoadCsvNotification))
  }

  test("should not notify for LoadCsv on top of eager pipe") {
    val plan =
      LoadCSV(
        Eager(
          AllNodesScan(IdName("a"), Set.empty)(solved)
        )(solved),
        url,
        IdName("foo"),
        NoHeaders,
        None,
        legacyCsvQuoteEscaping = false
      )(solved)

    checkForEagerLoadCsv(plan) should equal(None)
  }
}
