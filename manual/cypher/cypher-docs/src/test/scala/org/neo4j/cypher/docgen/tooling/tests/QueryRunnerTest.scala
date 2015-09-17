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
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.{MatchResult, Matcher}


class QueryRunnerTest extends CypherFunSuite {
  test("invalid query fails") {
    val query = "match n return x"
    val result = QueryRunner.runQueries(init = Seq.empty, queries = Seq(Query(query, NoAssertions, NoContent)))

    result should have size 1
    result should haveATestFailureOfClass(query -> classOf[SyntaxException])
  }

  test("assertion failure comes through nicely") {
    val query = "match n return n"
    val result = QueryRunner.runQueries(init = Seq.empty, queries = Seq(Query(query, ResultAssertions(p => 1 should equal(2)), NoContent)))

    result should have size 1
    result should haveATestFailureOfClass(query -> classOf[TestFailedException])
  }

  test("expected exception does not cause a failure") {
    val query = "match n return x"
    val exception = ExpectedException[SyntaxException](_ => {})
    val result = QueryRunner.runQueries(init = Seq.empty, queries = Seq(Query(query, exception, NoContent)))

    result should have size 1
    result shouldNot haveFailureFor(query)
  }

  test("when expecting an exception, not throwing is an error") {
    val query = "match n return n"
    val exception = ExpectedException[SyntaxException](_ => {})
    val result = QueryRunner.runQueries(init = Seq.empty, queries = Seq(Query(query, exception, NoContent)))

    result should have size 1
    result should haveATestFailureOfClass(query -> classOf[ExpectedExceptionNotFound])
  }

  class HasATestFailureOfClass[EXCEPTION <: Exception](queryAndClass: (String, Class[EXCEPTION]))
    extends Matcher[Seq[(String, Option[Exception])]] {

    def apply(result: Seq[(String, Option[Exception])]) = {
      val map: Map[String, Option[Exception]] = result.toMap
      val query: String = queryAndClass._1
      val r = map(query)
      val typ: Class[EXCEPTION] = queryAndClass._2

      if (r.isEmpty)
        MatchResult(
          matches = false,
          s"""Did not contain a test failure for query [<$query>] of type $typ""",
          s"""Did contain a test failure for query [<$query>] of type $typ"""
        )
      else {
        val c1 = r.get.getClass
        val c2 = typ
        MatchResult(
          c1 == c2,
          s"""Did not contain a test failure for query [<$query>] of type $c2 - the failure found had type $c1""",
          s"""Did contain a test failure for query [<$query>] of type $c1"""
        )
      }
    }
  }

  def haveATestFailureOfClass[EXCEPTION <: Exception](queryAndClass: (String, Class[EXCEPTION])) =
    new HasATestFailureOfClass(queryAndClass)

  class HasFailure(query: String)
    extends Matcher[Seq[(String, Option[Exception])]] {

    def apply(result: Seq[(String, Option[Exception])]) = {
      val map: Map[String, Option[Exception]] = result.toMap
      val r = map(query)
      val maybeFailure = r.map(_.toString).getOrElse("")
      MatchResult(
        matches = r.nonEmpty,
        s"""Did not contain a test failure for query [<$query>]""",
        s"""Did contain a test failure for query [<$query>]: $maybeFailure"""
      )
    }
  }

  def haveFailureFor(query: String) =
    new HasFailure(query)
}

