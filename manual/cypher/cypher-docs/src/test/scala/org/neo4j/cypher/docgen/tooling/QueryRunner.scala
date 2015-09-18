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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.ExecutionEngine
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory

import scala.util.{Success, Failure, Try}

/**
 * QueryRunner is used to actually run queries and produce either errors or Content containing the
 */
class QueryRunner(formatter: (InternalExecutionResult, Content, GraphDatabaseService) => Content) {

  def runQueries(init: Seq[String], queries: Seq[Query]): TestRunResult = {
    val db: GraphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)

    init.foreach { q =>
      try {
        engine.execute(q)
      } catch {
        case e: Throwable => throw new RuntimeException(s"Initialising database failed on query: $q", e)
      }
    }

    val results = queries.map { case q@Query(query, assertions, content) =>
      val format = formatter(_: InternalExecutionResult, content, db)
      val result: Either[Exception, Content] =
        try {
          val resultTry = Try(engine.execute(query))
          (assertions, resultTry) match {
            case (e: ExpectedException[_], Success(_)) =>
              Left(new ExpectedExceptionNotFound(s"Expected exception of type ${e.getExceptionClass}"))

            case (expectation: ExpectedException[_], Failure(exception: Exception)) =>
              expectation.handle(exception)
              Right(content)

            case (_, Failure(exception: Exception)) =>
              q.createdAt.initCause(exception)
              Left(q.createdAt)

            case (ResultAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result)
              Right(format(result))

            case (ResultAndDbAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result, db)
              Right(format(result))

            case (NoAssertions, Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              Right(format(result))
          }

        } catch {
          case e: Exception =>
            Left(e)
        }

      QueryRunResult(q, result)
    }
    TestRunResult(results)
  }
}

case class QueryRunResult(query: Query, testResult: Either[Exception,Content])
case class TestRunResult(queryResults: Seq[QueryRunResult]) {
  def foreach[U](f: QueryRunResult => U) = queryResults.foreach(f)

  private val _map = queryResults.map(r => r.query.queryText -> r.testResult).toMap

  def apply(q: String): Either[Exception, Content] = _map(q)
}
class ExpectedExceptionNotFound(m: String) extends Exception(m)