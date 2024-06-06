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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport

class LoadCsvParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("LOAD CSV WITH HEADERS FROM 'file:///ALL_PLANT_RMs_2.csv' AS l") {
    parsesTo[Clause](LoadCSV(withHeaders = true, literalString("file:///ALL_PLANT_RMs_2.csv"), varFor("l"), None)(pos))
  }

  test("""LOAD CSV WITH HEADERS FROM "file:///ALL_PLANT_RMs_2.csv" AS l""") {
    parsesTo[Clause](LoadCSV(withHeaders = true, literalString("file:///ALL_PLANT_RMs_2.csv"), varFor("l"), None)(pos))
  }

  test("LOAD CSV WITH HEADERS FROM `var` AS l") {
    parsesTo[Clause](LoadCSV(withHeaders = true, varFor("var"), varFor("l"), None)(pos))
  }

  test("LOAD CSV WITH HEADERS FROM '1' + '2' AS l") {
    parsesTo[Clause](LoadCSV(withHeaders = true, add(literalString("1"), literalString("2")), varFor("l"), None)(pos))
  }

  test("LOAD CSV WITH HEADERS FROM 1+2 AS l") {
    parsesTo[Clause](LoadCSV(withHeaders = true, add(literalInt(1), literalInt(2)), varFor("l"), None)(pos))
  }

  test("LOAD CSV WITH HEADERS FROM file:///ALL_PLANT_RMs_2.csv AS l") {
    // Message is asserted in org.neo4j.internal.cypher.acceptance.ParsingErrorsAcceptanceTest
    // The exception caught in this unit test do not reflect what the user gets
    failsParsing[Statements]
  }

  test("LOAD CSV WITH HEADERS FROM 'file:///ALL_PLANT_RMs_2.csv AS l") {
    // Message is asserted in org.neo4j.internal.cypher.acceptance.ParsingErrorsAcceptanceTest
    // The exception caught in this unit test do not reflect what the user gets
    failsParsing[Statements]
  }

  test("""LOAD CSV WITH HEADERS FROM "file:///ALL_PLANT_RMs_2.csv AS l""") {
    // Message is asserted in org.neo4j.internal.cypher.acceptance.ParsingErrorsAcceptanceTest
    // The exception caught in this unit test do not reflect what the user gets
    failsParsing[Statements]
  }

  test("LOAD CSV WITH HEADERS FROM `var AS l") {
    // Message is asserted in org.neo4j.internal.cypher.acceptance.ParsingErrorsAcceptanceTest
    // The exception caught in this unit test do not reflect what the user gets
    failsParsing[Statements]
  }
}
