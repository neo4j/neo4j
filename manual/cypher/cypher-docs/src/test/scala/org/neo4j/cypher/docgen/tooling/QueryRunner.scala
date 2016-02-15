/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.GraphDatabaseQueryService

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
class QueryRunner(formatter: (GraphDatabaseQueryService, Transaction) => InternalExecutionResult => Content) extends GraphIcing {

  def runQueries(contentsWithInit: Seq[ContentWithInit], title: String): TestRunResult = {

    val groupedByInits: Map[Seq[String], Seq[(String, QueryResultPlaceHolder)]] =
      contentsWithInit.groupBy(_.initKey).mapValues(_.map(init => init.lastInit -> init.queryResultPlaceHolder))
    var graphVizCounter = 0

    val results: Iterable[RunResult] = groupedByInits.flatMap {
      case (init, placeHolders) =>

        val db = new RestartableDatabase(init)
        try {
          if (db.failures.nonEmpty) db.failures
          else {
            val result = placeHolders.map { queryAndPlaceHolder =>
              try {
                queryAndPlaceHolder match {
                  case (queryText: String, tb: TablePlaceHolder) =>
                    runSingleQuery(db, queryText, tb.assertions, tb)

                  case (queryText: String, gv: GraphVizPlaceHolder) =>
                    graphVizCounter = graphVizCounter + 1
                    Try(db.execute(queryText)) match {
                      case Success(inner) =>
                        GraphVizRunResult(gv, captureStateAsGraphViz(db.getInnerDb, title, graphVizCounter, gv.options))
                      case Failure(error) =>
                        QueryRunResult(queryText, gv, Left(error))
                    }

                  case (queryText: String, placeHolder: ExecutionPlanPlaceHolder) =>
                    explainSingleQuery(db, queryText, placeHolder)

                  case (queryText: String, placeHolder: ProfileExecutionPlanPlaceHolder) =>
                    profileSingleQuery(db, queryText, placeHolder.assertions, placeHolder)

                  case _ =>
                    ???
                }
              } finally {
                db.nowIsASafePointToRestartDatabase()
              }
            }
            result
          }
        } finally db.shutdown()
    }

    TestRunResult(results.toSeq)
  }

  private def runSingleQuery(database: RestartableDatabase, queryText: String, assertions: QueryAssertions, content: TablePlaceHolder): QueryRunResult = {
    val format: (Transaction) => (InternalExecutionResult) => Content = (tx: Transaction) => formatter(database.getInnerDb, tx)(_)

      val result: Either[Throwable, Transaction => Content] =
        try {
          val resultTry = Try(database.execute(queryText))
          (assertions, resultTry) match {
            // *** Success conditions

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
            case (_, Failure(exception: Throwable)) =>
              Left(exception)

            case x =>
              throw new InternalException(s"Did not see this one coming $x")
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

  private def explainSingleQuery(database: RestartableDatabase,
                                 queryText: String,
                                 placeHolder: QueryResultPlaceHolder) = {
    val planString = Try(database.execute(s"EXPLAIN $queryText")) match {
      case Success(inner) =>
        inner.executionPlanDescription().toString
      case x =>
        throw new InternalException(s"Did not see this one coming $x")
    }
    ExecutionPlanRunResult(queryText, placeHolder, ExecutionPlan(planString))
  }

  private def profileSingleQuery(database: RestartableDatabase,
                                 queryText: String,
                                 assertions: QueryAssertions,
                                 placeHolder: QueryResultPlaceHolder) = {
    val profilingAttempt = Try(database.execute(s"PROFILE $queryText"))
    val planString = (assertions, profilingAttempt) match {
      case (ResultAssertions(f), Success(inner)) =>
        val result = RewindableExecutionResult(inner)
        f(result)
        result.executionPlanDescription().toString

      case (ResultAndDbAssertions(f), Success(inner)) =>
        val result = RewindableExecutionResult(inner)
        f(result, database.getInnerDb)
        result.executionPlanDescription().toString

      case (NoAssertions, Success(inner)) =>
        inner.executionPlanDescription().toString

      case x =>
        throw new InternalException(s"Did not see this one coming $x")
    }
    println(planString)
    ExecutionPlanRunResult(queryText, placeHolder, ExecutionPlan(planString))
  }
}

sealed trait RunResult {
  def success: Boolean
  def original: QueryResultPlaceHolder
  def newContent: Option[Content]
  def newFailure: Option[Throwable]
}

case class QueryRunResult(queryText: String, original: QueryResultPlaceHolder, testResult: Either[Throwable, Content]) extends RunResult {
  override def success = testResult.isRight

  override def newContent: Option[Content] = testResult.right.toOption

  override def newFailure: Option[Throwable] = testResult.left.toOption
}

case class GraphVizRunResult(original: GraphVizPlaceHolder, graphViz: GraphViz) extends RunResult {
  override def success = true
  override def newContent = Some(graphViz)
  override def newFailure = None
}

case class ExecutionPlanRunResult(queryText: String, original: QueryResultPlaceHolder, executionPlan: ExecutionPlan) extends RunResult {

  override def success = true

  override def newContent = Some(executionPlan)

  override def newFailure = None
}

case class TestRunResult(queryResults: Seq[RunResult]) {
  def success = queryResults.forall(_.success)

  def foreach[U](f: RunResult => U) = queryResults.foreach(f)
}

class ExpectedExceptionNotFound(m: String) extends Exception(m)
