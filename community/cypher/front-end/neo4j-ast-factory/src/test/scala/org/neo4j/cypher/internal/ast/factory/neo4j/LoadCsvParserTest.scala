/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.LoadCSV

class LoadCsvParserTest extends JavaccParserAstTestBase[Clause] {
  implicit val parser: JavaccRule[Clause] = JavaccRule.Clause

  private val fileExpressionFailed =
    "Failed to parse the file expression. Please remember to use quotes for string literals."

  test("LOAD CSV WITH HEADERS FROM 'file:///ALL_PLANT_RMs_2.csv' AS l") {
    yields(LoadCSV(withHeaders = true, literalString("file:///ALL_PLANT_RMs_2.csv"), varFor("l"), None))
  }

  test("""LOAD CSV WITH HEADERS FROM "file:///ALL_PLANT_RMs_2.csv" AS l""") {
    yields(LoadCSV(withHeaders = true, literalString("file:///ALL_PLANT_RMs_2.csv"), varFor("l"), None))
  }

  test("LOAD CSV WITH HEADERS FROM `var` AS l") {
    yields(LoadCSV(withHeaders = true, varFor("var"), varFor("l"), None))
  }

  test("LOAD CSV WITH HEADERS FROM '1' + '2' AS l") {
    yields(LoadCSV(withHeaders = true, add(literalString("1"), literalString("2")), varFor("l"), None))
  }

  test("LOAD CSV WITH HEADERS FROM 1+2 AS l") {
    yields(LoadCSV(withHeaders = true, add(literalInt(1), literalInt(2)), varFor("l"), None))
  }

  test("LOAD CSV WITH HEADERS FROM file:///ALL_PLANT_RMs_2.csv AS l") {
    assertFailsWithMessageStart(testName, fileExpressionFailed)
  }

  test("LOAD CSV WITH HEADERS FROM 'file:///ALL_PLANT_RMs_2.csv AS l") {
    assertFailsWithMessageStart(testName, fileExpressionFailed)
  }

  test("""LOAD CSV WITH HEADERS FROM "file:///ALL_PLANT_RMs_2.csv AS l""") {
    assertFailsWithMessageStart(testName, fileExpressionFailed)
  }

  test("LOAD CSV WITH HEADERS FROM `var AS l") {
    assertFailsWithMessageStart(testName, fileExpressionFailed)
  }
}
