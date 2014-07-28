/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.CypherQueryWithOptions
import org.neo4j.cypher.internal.VersionOption

class CypherOptionParserTest extends CypherFunSuite {

  test("should parse version") {
    CypherOptionParser("CYPHER 1.9 MATCH") should equal(CypherQueryWithOptions("MATCH", Seq(VersionOption("1.9"))))
    CypherOptionParser("CYPHER 2.0 THAT") should equal(CypherQueryWithOptions("THAT", Seq(VersionOption("2.0"))))
    CypherOptionParser("CYPHER 2.1 YO") should equal(CypherQueryWithOptions("YO", Seq(VersionOption("2.1"))))
  }

  test("should parse profile") {
    CypherOptionParser("PROFILE THINGS") should equal(CypherQueryWithOptions("THINGS", Seq(ProfileOption)))
  }

  test("should parse explain") {
    CypherOptionParser("EXPLAIN THIS") should equal(CypherQueryWithOptions("THIS", Seq(ExplainOption)))
  }

  test("should parse multiple options") {
    CypherOptionParser("CYPHER 2.1experimental PROFILE PATTERN") should equal(CypherQueryWithOptions("PATTERN", Seq(VersionOption("2.1experimental"), ProfileOption)))
    CypherOptionParser("EXPLAIN PROFILE CYPHER 2.1 YALL") should equal(CypherQueryWithOptions("YALL", Seq(ExplainOption, ProfileOption, VersionOption("2.1"))))
  }

  test("should require whitespace between option and query") {
    CypherOptionParser("explainmatch") should equal(CypherQueryWithOptions("explainmatch"))
    CypherOptionParser("explain match") should equal(CypherQueryWithOptions("match", Seq(ExplainOption)))
  }
}
