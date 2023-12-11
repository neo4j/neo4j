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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.csv.reader.Readables
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.CSVResource
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

class LoadCSVPipeTest extends CypherFunSuite {

  test("with headers: close should close seeker") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWith(
      query = QueryStateHelper.emptyWithResourceManager(resourceManager).query,
      resources = new CSVResources(resourceManager)
    )
    when(state.query.getImportDataConnection(any[URL])).thenAnswer((invocation: InvocationOnMock) =>
      Readables.files(StandardCharsets.UTF_8, Paths.get(invocation.getArgument[URL](0).toURI))
    )

    val input = new FakePipe(Seq(Map("x" -> 0), Map("x" -> 1)))
    val csv = classOf[LoadCSVPipeTest].getResource("/load.csv")
    val pipe = LoadCSVPipe(
      input,
      HasHeaders,
      Literal(Values.stringValue(csv.toExternalForm)),
      "csv",
      None,
      legacyCsvQuoteEscaping = false,
      10
    )()
    val result = pipe.createResults(state)
    result.hasNext // initialize seeker
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: CSVResource => t } should have size (1)
  }

  test("with headers: exhaust should close seeker") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWith(
      query = QueryStateHelper.emptyWithResourceManager(resourceManager).query,
      resources = new CSVResources(resourceManager)
    )
    when(state.query.getImportDataConnection(any[URL])).thenAnswer((invocation: InvocationOnMock) =>
      Readables.files(StandardCharsets.UTF_8, Paths.get(invocation.getArgument[URL](0).toURI))
    )

    val input = new FakePipe(Seq(
      Map("x" -> 0),
      Map("x" -> 1)
    ))
    val csv = classOf[LoadCSVPipeTest].getResource("/load.csv")
    val pipe = LoadCSVPipe(
      input,
      HasHeaders,
      Literal(Values.stringValue(csv.toExternalForm)),
      "csv",
      None,
      legacyCsvQuoteEscaping = false,
      10
    )()
    // exhaust
    val iterator = pipe.createResults(state)
    iterator.next()
    iterator.next()
    iterator.next()
    monitor.closedResources.collect { case t: CSVResource => t } should have size 1
    iterator.next()
    iterator.next()
    iterator.next()
    iterator.hasNext shouldBe false
    monitor.closedResources.collect { case t: CSVResource => t } should have size 2
  }

  test("without headers: close should close seeker") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWith(
      query = QueryStateHelper.emptyWithResourceManager(resourceManager).query,
      resources = new CSVResources(resourceManager)
    )
    when(state.query.getImportDataConnection(any[URL])).thenAnswer((invocation: InvocationOnMock) =>
      Readables.files(StandardCharsets.UTF_8, Paths.get(invocation.getArgument[URL](0).toURI))
    )

    val input = new FakePipe(Seq(Map("x" -> 0), Map("x" -> 1)))
    val csv = classOf[LoadCSVPipeTest].getResource("/load-no-headers.csv")
    val pipe = LoadCSVPipe(
      input,
      NoHeaders,
      Literal(Values.stringValue(csv.toExternalForm)),
      "csv",
      None,
      legacyCsvQuoteEscaping = false,
      10
    )()
    val result = pipe.createResults(state)
    result.hasNext // initialize seeker
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: CSVResource => t } should have size (1)
  }

  test("without headers: exhaust should close seeker") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWith(
      query = QueryStateHelper.emptyWithResourceManager(resourceManager).query,
      resources = new CSVResources(resourceManager)
    )
    when(state.query.getImportDataConnection(any[URL])).thenAnswer((invocation: InvocationOnMock) =>
      Readables.files(StandardCharsets.UTF_8, Paths.get(invocation.getArgument[URL](0).toURI))
    )

    val input = new FakePipe(Seq(
      Map("x" -> 0),
      Map("x" -> 1)
    ))
    val csv = classOf[LoadCSVPipeTest].getResource("/load-no-headers.csv")
    val pipe = LoadCSVPipe(
      input,
      NoHeaders,
      Literal(Values.stringValue(csv.toExternalForm)),
      "csv",
      None,
      legacyCsvQuoteEscaping = false,
      10
    )()
    // exhaust
    val iterator = pipe.createResults(state)
    iterator.next()
    iterator.next()
    iterator.next()
    monitor.closedResources.collect { case t: CSVResource => t } should have size 1
    iterator.next()
    iterator.next()
    iterator.next()
    iterator.hasNext shouldBe false
    monitor.closedResources.collect { case t: CSVResource => t } should have size 2
  }
}
