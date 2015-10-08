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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v3_0.InternalException
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}

import scala.collection.immutable.Iterable
import scala.util.{Failure, Success, Try}

/**
 * QueryRunner is used to actually run queries and produce either errors or
 * Content containing the runSingleQuery of the execution
 *
 * It works by grouping queries and graph-vizualisations by the initiation they need, and running all queries with the same
 * init queries together. After running the query, we check if it updated the graph. If a query updates the graph,
 * we drop the database and create a new one. This way we can make sure that two queries don't affect each other more than
 * necessary.
 */
class QueryRunner(formatter: (GraphDatabaseService, Transaction) => (InternalExecutionResult, Content) => Content) extends GraphIcing {

  def runQueries(contentsWithInit: Seq[ContentWithInit], title: String): TestRunResult = {

      val groupedByInits: Map[Seq[String], Seq[Content]] = contentsWithInit.groupBy(_.init).mapValues(_.map(_.query))
      var graphVizCounter = 0

    val results: Iterable[RunResult] = groupedByInits.flatMap {
      case (init, queries) =>

        val db = new RestartableDatabase(init)
        try {
          if (db.failures.nonEmpty) db.failures
          else {
            queries.map {
              case q: Query =>
                val result = runSingleQuery(db, q.queryText, q.assertions, q)
                db.nowIsASafePointToRestartDatabase()

                result

              case gv: GraphVizPlaceHolder =>
                graphVizCounter = graphVizCounter + 1
                GraphVizRunResult(gv, captureStateAsGraphViz(db.getInnerDb, title, graphVizCounter))

              case _ =>
                ???
            }
          }
        } finally db.shutdown()
    }

    TestRunResult(results.toSeq)
  }

  private def runSingleQuery(database: RestartableDatabase, queryText: String, assertions: QueryAssertions, content: Content): QueryRunResult = {
    val format: (Transaction) => (InternalExecutionResult) => Content = (tx: Transaction) => formatter(database.getInnerDb, tx)(_, content)

      val result: Either[Throwable, Transaction => Content] =
        try {
          val resultTry = Try(database.execute(queryText))
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
              f(result, database.getInnerDb)
              Right(format(_)(result))

            case (NoAssertions, Success(inner)) =>
              val result = RewindableExecutionResult(inner)
              Right(format(_)(result))

            // *** Error conditions
            case (e: ExpectedFailure[_], _: Success[_]) =>
              Left(new ExpectedExceptionNotFound(s"Expected exception of type ${e.getExceptionClass}"))

            case (_, Failure(exception: Throwable)) =>
              Left(exception)

            case x =>
              throw new InternalException(s"This not see this one coming $x")
          }
        } catch {
          case e: Throwable =>
            Left(e)
        }

      val formattedResult: Either[Throwable, Content] = database.getInnerDb.withTx { tx =>
        result.right.map {
          case contentBuilder => contentBuilder(tx)
        }
      }

      QueryRunResult(queryText, content, formattedResult)
    }
}

sealed trait RunResult {
  def success: Boolean
  def original: Content
  def newContent: Option[Content]
  def newFailure: Option[Throwable]
}

case class QueryRunResult(queryText: String, original: Content, testResult: Either[Throwable, Content]) extends RunResult {
  override def success = testResult.isRight

  override def newContent: Option[Content] = testResult.right.toOption

  override def newFailure: Option[Throwable] = testResult.left.toOption
}

case class GraphVizRunResult(original: GraphVizPlaceHolder, graphViz: GraphViz) extends RunResult {
  override def success = true
  override def newContent = Some(graphViz)
  override def newFailure = None
}

case class TestRunResult(queryResults: Seq[RunResult]) {
  def success = queryResults.forall(_.success)

  def foreach[U](f: RunResult => U) = queryResults.foreach(f)
}

class ExpectedExceptionNotFound(m: String) extends Exception(m)
