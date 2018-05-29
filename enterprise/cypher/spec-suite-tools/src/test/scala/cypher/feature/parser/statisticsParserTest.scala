/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.feature.parser

import java.util

import cucumber.api.DataTable
import org.neo4j.cypher.internal.{QueryStatistics, javacompat}

class statisticsParserTest extends ParsingTestSupport {

  test("should parse added nodes") {
    statisticsParser(singleRow("+nodes", "1")) should accept(stats(QueryStatistics(nodesCreated = 1)))
  }

  test("should parse deleted nodes") {
    statisticsParser(singleRow("-nodes", "1")) should accept(stats(QueryStatistics(nodesDeleted = 1)))
  }

  test("should parse added rels") {
    statisticsParser(singleRow("+relationships", "1")) should accept(stats(QueryStatistics(relationshipsCreated = 1)))
  }

  test("should parse deleted rels") {
    statisticsParser(singleRow("-relationships", "1")) should accept(stats(QueryStatistics(relationshipsDeleted = 1)))
  }

  test("should parse added labels") {
    statisticsParser(singleRow("+labels", "1")) should accept(stats(QueryStatistics(labelsAdded = 1)))
  }

  test("should parse deleted labels") {
    statisticsParser(singleRow("-labels", "1")) should accept(stats(QueryStatistics(labelsRemoved = 1)))
  }

  test("should parse added properties") {
    statisticsParser(singleRow("+properties", "1")) should accept(stats(QueryStatistics(propertiesSet = 1)))
  }

  test("should parse deleted properties") {
    statisticsParser(singleRow("-properties", "1")) should accept(stats(QueryStatistics(propertiesSet = 1)))
  }

  test("should parse a mix of stats") {
    statisticsParser(tableOf(Seq("-properties", "1"),
                             Seq("+nodes", "3"),
                             Seq("+labels", "42"))) should accept(stats(QueryStatistics(propertiesSet = 1,
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
