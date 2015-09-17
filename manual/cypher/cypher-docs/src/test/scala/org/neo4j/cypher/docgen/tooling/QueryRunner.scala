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

  def runQueries(init: Seq[String], queries: Seq[Query]): Seq[QueryRunResult] = {
    val db: GraphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)

    init.foreach { q =>
      try {
        engine.execute(q)
      } catch {
        case e: Throwable => throw new RuntimeException(s"Initialising database failed on query: $q", e)
      }
    }

    queries.map { case Query(query, assertions, content) =>
      val format = formatter(_: InternalExecutionResult, content, db)
      val (result, newContent) =
        try {
          (assertions, Try(engine.execute(query))) match {
            case (e: ExpectedException[_], Success(_)) =>
              (Some(new ExpectedExceptionNotFound(s"Expected exception of type ${e.getExceptionClass}")), NoContent)

            case (expectation: ExpectedException[_], Failure(exception: Exception)) =>
              expectation.handle(exception)
              (None, content)

            case (_, Failure(exception: Exception)) =>
              (Some(exception), NoContent)

            case (ResultAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result)
              (None, format(result))

            case (ResultAndDbAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result, db)
              (None, format(result))

            case (NoAssertions, Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              (None, format(result))
          }

        } catch {
          case e: Exception =>
            (Some(e), NoContent)
        }

      QueryRunResult(query, result, newContent)
    }
  }
}

//TODO: instead of testResult AND content, maybe we should only keep an Either[Exception, Content]?
case class QueryRunResult(query: String, testResult: Option[Exception], content: Content)

class ExpectedExceptionNotFound(m: String) extends Exception(m)