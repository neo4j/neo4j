/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.docgen.tooling.tests

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.docgen.tooling._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Transaction
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.{MatchResult, Matcher}


class QueryRunnerTest extends CypherFunSuite {
  test("invalid query fails") {
    val query = "match n return x"
    val result = runQueries(query)

    result.queryResults should have size 1
    result should haveATestFailureOfClass(query -> classOf[SyntaxException])
  }

  test("assertion failure comes through nicely") {
    val query = "match n return n"
    val result = runQueries(query, ResultAssertions(p => 1 should equal(2)))

    result.queryResults should have size 1
    result should haveATestFailureOfClass(query -> classOf[TestFailedException])
  }

  test("expected exception does not cause a failure") {
    val query = "match n return x"
    val result = runQueries(query, ExpectedException[SyntaxException](_ => {}))

    result.queryResults should have size 1
    result shouldNot haveFailureFor(query)
  }

  test("when expecting an exception, not throwing is an error") {
    val query = "match n return n"
    val expectation = ExpectedException[SyntaxException](_ => {})
    val result = runQueries(query, expectation)

    result.queryResults should have size 1
    result should haveATestFailureOfClass(query -> classOf[ExpectedExceptionNotFound])
  }

  private def runQueries(query: String, assertions: QueryAssertions = NoAssertions, content: Content = NoContent): TestRunResult = {
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    try {
      val formatter = (_: Transaction) => (_: InternalExecutionResult, content: Content) => content
      val runner = new QueryRunner(db, formatter)
      runner.runQueries(init = Seq.empty, queries = Seq(Query(query, assertions, content)))
    }
    finally
      db.shutdown()
  }

  private def haveATestFailureOfClass[EXCEPTION <: Exception](queryAndClass: (String, Class[EXCEPTION])) =
    new HasATestFailureOfClass(queryAndClass)

  private def haveFailureFor(query: String) =
    new HasFailure(query)
}

class HasATestFailureOfClass[EXCEPTION <: Exception](queryAndClass: (String, Class[EXCEPTION]))
  extends Matcher[TestRunResult] {

  def apply(result: TestRunResult) = {

    val (query, expectedType) = queryAndClass
    val testFailure: Option[Exception] = result(query).left.toOption

    if (testFailure.isEmpty)
      MatchResult(
        matches = false,
        s"""Did not contain a test failure for query [<$query>] of type $expectedType""",
        s"""Did contain a test failure for query [<$query>] of type $expectedType"""
      )
    else {
      MatchResult(
        matchesDirectlyOrThroughCause(expectedType, testFailure.get),
        s"""Did not contain a test failure for query [<$query>] of type $expectedType - the failure found had type ${testFailure.get.getClass}""",
        s"""Did contain a test failure for query [<$query>] of type ${testFailure.get.getClass}"""
      )
    }
  }

  private def matchesDirectlyOrThroughCause(expected: Class[EXCEPTION], actual: Throwable): Boolean = {
    if(expected == actual.getClass)
      true
    else if(actual.getCause != null) {
      matchesDirectlyOrThroughCause(expected, actual.getCause)
    } else false
  }
}

class HasFailure(query: String)
  extends Matcher[TestRunResult] {

  def apply(result: TestRunResult) = {
    val testFailure: Option[Exception] = result(query).left.toOption

    val maybeFailure = testFailure.map(_.toString).getOrElse("")
    MatchResult(
      matches = testFailure.nonEmpty,
      s"""Did not contain a test failure for query [<$query>]""",
      s"""Did contain a test failure for query [<$query>]: $maybeFailure"""
    )
  }
}
