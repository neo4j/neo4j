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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport, SyntaxException}

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should provide sensible error message when ordering by mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform ORDER BY on mixed types.")
    }
  }

  test("should provide sensible error message when ordering by list values") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List(List("a"),List("b")))))
      exception.getMessage should startWith("Cannot perform ORDER BY on lists, consider using UNWIND.")
    }
  }

  test("should provide sensible error message when aggregating by min on mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN min(thing)", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform MIN on mixed types.")
    }
  }

  test("should provide sensible error message when aggregating by max on mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN max(thing)", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform MAX on mixed types.")
    }
  }

  test("should provide sensible error message when omitting colon before relationship type on create") {
    withEachPlanner { execute =>
      val exception = intercept[SyntaxException](execute("CREATE (a)-[ASSOCIATED_WITH]->(b)", Seq.empty))
      val exceptionMsg = "Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?"
      exception.getMessage should include(exceptionMsg)
    }
  }

  test("should provide sensible error message when trying to add multiple relationship types on create") {
    withEachPlanner { execute =>
      val exception = intercept[SyntaxException] (execute("CREATE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)", Seq.empty))
      exception.getMessage should include("A single relationship type must be specified for CREATE.")
    }
  }

  test("should provide sensible error message when omitting colon before relationship type on merge") {
    withEachPlanner { execute =>
      val exception = intercept[SyntaxException](execute("MERGE (a)-[ASSOCIATED_WITH]->(b)", Seq.empty))
      val exceptionMsg = "Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?"
      exception.getMessage should include(exceptionMsg)
    }
  }

  test("should provide sensible error message when trying to add multiple relationship types on merge") {
    withEachPlanner { execute =>
      val exception = intercept[SyntaxException] (execute("MERGE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)", Seq.empty))
      exception.getMessage should include("A single relationship type must be specified for MERGE.")
    }
  }
}
