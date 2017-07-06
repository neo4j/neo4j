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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}

class MultipleGraphsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("load graph") {
    val query = "LOAD GRAPH 'test' RETURN 1"

    a[SyntaxException] shouldBe thrownBy {
      executeWithAllPlanners(query)
    }
  }

  test("emit graph") {
    val query = "MATCH ()--() EMIT GRAPH 'test' RETURN *"

    a[SyntaxException] shouldBe thrownBy {
      executeWithAllPlanners(query)
    }
  }

  test("return named graph") {
    val query = "MATCH ()--() RETURN GRAPH 'test'"

    a[SyntaxException] shouldBe thrownBy {
      executeWithAllPlanners(query)
    }
  }

  test("return anonymous graph") {
    val query = "MATCH ()--() RETURN GRAPH"

    a[SyntaxException] shouldBe thrownBy {
      executeWithAllPlanners(query)
    }
  }
}
