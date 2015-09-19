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
import org.neo4j.cypher.internal.frontend.v2_3.InternalException
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}

import scala.util.{Failure, Success, Try}

/**
 * QueryRunner is used to actually run queries and produce either errors or
 * Content containing the result of the execution
 */
class QueryRunner(db: GraphDatabaseService,
                  formatter: Transaction => (InternalExecutionResult, Content) => Content) extends GraphIcing {

  def runQueries(init: Seq[String], queries: Seq[Query]): TestRunResult = {
    val engine = new ExecutionEngine(db)

    val results = queries.map { case q@Query(query, assertions, content) =>

      val format: (Transaction) => (InternalExecutionResult) => Content = (tx: Transaction) => formatter(tx)(_, content)

      val result: Either[Exception, Transaction => Content] =
        try {
          val resultTry = Try(engine.execute(query))
          (assertions, resultTry) match {
            // *** Success conditions
            case (expectation: ExpectedFailure[_], Failure(exception: Exception)) =>
              expectation.handle(exception)
              Right(_ => content)

            case (ResultAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result)
              Right(format(_)(result))

            case (ResultAndDbAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result, db)
              Right(format(_)(result))

            case (NoAssertions, Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              Right(format(_)(result))

            // *** Error conditions
            case (e: ExpectedFailure[_], _: Success) =>
              Left(new ExpectedExceptionNotFound(s"Expected exception of type ${e.getExceptionClass}"))

            case (_, Failure(exception: Exception)) =>
              q.createdAt.initCause(exception)
              Left(q.createdAt)

            case x =>
              throw new InternalException(s"This not see this one coming $x")
          }
        } catch {
          case e: Exception =>
            Left(e)
        }

      val formattedResult = db.withTx { tx =>
        result fold(
          l => Left(l),
          (r: (Transaction) => Content) => Right(r(tx))
          )
      }


      QueryRunResult(q, formattedResult)
    }
    TestRunResult(results)
  }
}

case class QueryRunResult(query: Query, testResult: Either[Exception, Content])

case class TestRunResult(queryResults: Seq[QueryRunResult]) {
  def foreach[U](f: QueryRunResult => U) = queryResults.foreach(f)

  private val _map = queryResults.map(r => r.query.queryText -> r.testResult).toMap

  def apply(q: String): Either[Exception, Content] = _map(q)
}

class ExpectedExceptionNotFound(m: String) extends Exception(m)