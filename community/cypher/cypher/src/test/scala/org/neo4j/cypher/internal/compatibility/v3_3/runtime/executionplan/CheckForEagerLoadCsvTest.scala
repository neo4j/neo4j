/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{AllNodesScanPipe, EagerPipe, LoadCSVPipe}
import org.neo4j.cypher.internal.frontend.v3_4.notification.EagerLoadCsvNotification
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.HasHeaders

class CheckForEagerLoadCsvTest extends CypherFunSuite {

  test("should notify for EagerPipe on top of LoadCsvPipe") {
    val pipe = EagerPipe(LoadCSVPipe(AllNodesScanPipe("a")(), HasHeaders, Literal("foo"), "bar", None, false)())()

    checkForEagerLoadCsv(pipe) should equal(Some(EagerLoadCsvNotification))
  }

  test("should not notify for LoadCsv on top of eager pipe") {
    val pipe = LoadCSVPipe(EagerPipe(AllNodesScanPipe("a")())(), HasHeaders, Literal("foo"), "bar", None, false)()

    checkForEagerLoadCsv(pipe) should equal(None)
  }
}
