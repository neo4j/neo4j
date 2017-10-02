/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.spi.v3_3.CSVResources
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{Configs, TestConfiguration}
import org.scalatest.BeforeAndAfterAll
import sun.net.www.protocol.http.HttpURLConnection

class LoadCsvAcceptanceUserAgentTest
  extends ExecutionEngineFunSuite with BeforeAndAfterAll with CypherComparisonSupport {

  val expectedToSucceed: TestConfiguration = Configs.CommunityInterpreted - Configs.Version2_3

  test("should be able to download data from the web") {
    val url = s"http://127.0.0.1:$port/test.csv".cypherEscape

    val result = executeWith(expectedToSucceed, s"LOAD CSV FROM '$url' AS line RETURN count(line)")
    result.columnAs[Long]("count(line)").toList should equal(List(3))
  }

  test("should be able to download from a website when redirected and cookies are set") {
    val url = s"http://127.0.0.1:$port/redirect_test.csv".cypherEscape

    val result = executeWith(expectedToSucceed, s"LOAD CSV FROM '$url' AS line RETURN count(line)")
    result.columnAs[Long]("count(line)").toList should equal(List(3))
  }
  private val CSV_DATA_CONTENT = "1,1,1\n2,2,2\n3,3,3\n".getBytes
  private val CSV_PATH = "/test.csv"
  private val CSV_COOKIE_PATH = "/cookie_test.csv"
  private val CSV_REDIRECT_PATH = "/redirect_test.csv"
  private val MAGIC_COOKIE = "neoCookie=Magic"
  private val NEO_USER_AGENT = s"${CSVResources.NEO_USER_AGENT_PREFIX}${HttpURLConnection.userAgent}"
  private var httpServer: HttpServerTestSupport = _
  private var port = -1

  override def beforeAll() {
    val  builder = new HttpServerTestSupportBuilder()
    builder.onPathReplyWithData(CSV_PATH, CSV_DATA_CONTENT)
    builder.onPathReplyOnlyWhen(CSV_PATH, HttpServerTestSupport.hasUserAgent(NEO_USER_AGENT))

    builder.onPathReplyWithData(CSV_COOKIE_PATH, CSV_DATA_CONTENT)
    builder.onPathReplyOnlyWhen(CSV_COOKIE_PATH, HttpServerTestSupport.hasCookie(MAGIC_COOKIE))
    builder.onPathReplyOnlyWhen(CSV_COOKIE_PATH, HttpServerTestSupport.hasUserAgent(NEO_USER_AGENT))

    builder.onPathRedirectTo(CSV_REDIRECT_PATH, CSV_COOKIE_PATH)
    builder.onPathTransformResponse(CSV_REDIRECT_PATH, HttpServerTestSupport.setCookie(MAGIC_COOKIE))
    builder.onPathReplyOnlyWhen(CSV_REDIRECT_PATH, HttpServerTestSupport.hasUserAgent(NEO_USER_AGENT))

    httpServer = builder.build()
    httpServer.start()
    port = httpServer.boundInfo.getPort
    assert(port > 0)
  }

  override def afterAll() {
    httpServer.stop()
  }
}
