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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class JavaCCParserFallbackTest extends CypherFunSuite {

  test("should fall back") {
    Seq(
      "REVOKE SHOW INDEXES ON DATABASE foo FROM bar",
      "MATCH (n) RETURN n // SHOW",
      "MATCH (n) RETURN n.Id as INDEX",
      "MATCH (n) RETURN n.Id as GRANT",
      "MATCH (n) WITH n as SHOW RETURN SHOW as INDEX",
      "DROP ROLE cheeseRoll",
      "CREATE DATABASE store WAIT",
      "SHOW INDEX WHERE name = 'GRANT'",
      "MATCH (n:Label) WHERE n.cypher = 'SHOW INDEXES' and n.access = 'DENY' RETURN n",
      "CREATE(n:Catalog)"
    ).foreach(t => {
      withClue(t) { JavaCCParser.shouldFallback(t) shouldBe true }
    })
  }

  test("should not fall back") {
    Seq(
      "MATCH (n) RETURN n",
      "CREATE (n:Label)",
    ).foreach(t => {
      withClue(t) { JavaCCParser.shouldFallback(t) shouldBe false }
    })
  }

}
