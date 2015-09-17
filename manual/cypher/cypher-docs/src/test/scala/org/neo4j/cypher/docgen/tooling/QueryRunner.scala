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
import org.neo4j.test.TestGraphDatabaseFactory

import scala.util.{Success, Failure, Try}

object QueryRunner {


  //  (queryResult, Content) => Content

  def runQueries(init: Seq[String], queries: Seq[Query]): Seq[(String, Option[Exception])] = {
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)
    queries.map { case Query(q, assertions, _) =>
      val result: Option[Exception] =
        try {
          (assertions, Try(engine.execute(q))) match {
            case (e: ExpectedException[_], Success(_)) =>
              Some(new ExpectedExceptionNotFound(s"Expected exception of type ${e.getExceptionClass}"))

            case (expectation: ExpectedException[_], Failure(exception: Exception)) =>
              expectation.handle(exception)
              None

            case (_, Failure(exception: Exception)) =>
              Some(exception)

            case (ResultAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result)
              None

            case (ResultAndDbAssertions(f), Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              f(result, db)
              None

            case (NoAssertions, Success(_)) =>
              None
          }
        } catch {
          case e: Exception =>
            Some(e)
        }
      q -> result
    }
  }
}

class ExpectedExceptionNotFound(m: String) extends Exception(m)