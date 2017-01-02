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
package cypher.feature.parser

import java.util

import cucumber.api.DataTable
import org.neo4j.cypher.internal.{QueryStatistics, javacompat}

class statisticsParserTest extends ParsingTestSupport {

  test("should parse added nodes") {
    statisticsParser(singleRow("+nodes", "1")) should acceptStatistics(stats(QueryStatistics(nodesCreated = 1)))
  }

  test("should parse deleted nodes") {
    statisticsParser(singleRow("-nodes", "1")) should acceptStatistics(stats(QueryStatistics(nodesDeleted = 1)))
  }

  test("should parse added rels") {
    statisticsParser(singleRow("+relationships", "1")) should acceptStatistics(stats(QueryStatistics(relationshipsCreated = 1)))
  }

  test("should parse deleted rels") {
    statisticsParser(singleRow("-relationships", "1")) should acceptStatistics(stats(QueryStatistics(relationshipsDeleted = 1)))
  }

  test("should parse added labels") {
    statisticsParser(singleRow("+labels", "1")) should acceptStatistics(stats(QueryStatistics(labelsAdded = 1)))
  }

  test("should parse deleted labels") {
    statisticsParser(singleRow("-labels", "1")) should acceptStatistics(stats(QueryStatistics(labelsRemoved = 1)))
  }

  test("should parse added properties") {
    statisticsParser(singleRow("+properties", "1")) should acceptStatistics(stats(QueryStatistics(propertiesSet = 1)))
  }

  test("should parse deleted properties") {
    statisticsParser(singleRow("-properties", "1")) should acceptStatistics(stats(QueryStatistics(propertiesSet = 1)))
  }

  test("should parse a mix of stats") {
    statisticsParser(tableOf(Seq("-properties", "1"),
                             Seq("+nodes", "3"),
                             Seq("+labels", "42"))) should acceptStatistics(stats(QueryStatistics(propertiesSet = 1,
                                                                                                  nodesCreated = 3,
                                                                                                  labelsAdded = 42)))
  }

  private def singleRow(strings: String*): DataTable = {
    DataTable.create(util.Arrays.asList(strings.toList.asJava))
  }

  private def tableOf(strings: Seq[String]*): DataTable = {
    DataTable.create(strings.map(_.toList.asJava).toList.asJava)
  }

  private def stats(cypherStats: QueryStatistics): javacompat.QueryStatistics = new javacompat.QueryStatistics(
    cypherStats)

}
