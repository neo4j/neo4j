/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.testing.impl.http

import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.http.HttpStatementResult.Notification
import org.neo4j.cypher.testing.impl.http.HttpStatementResult.Result
import org.neo4j.cypher.testing.impl.shared.NotificationImpl
import org.neo4j.exceptions.Neo4jException
import org.neo4j.graphdb
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.InputPosition
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.test.server.HTTP

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class HttpStatementResult(result: Result, notifications: Seq[Notification]) extends StatementResult {

  private val cols = result.columns

  override def columns(): Seq[String] = cols

  override def records(): Seq[Record] = result.data.map(r => cols.zip(r.row).toMap)

  override def consume(): Unit = ()

  override def getNotifications(): List[graphdb.Notification] =
    notifications
      .map(n =>
        NotificationImpl.fromRaw(
          n.code,
          n.title,
          n.description,
          n.severity,
          Option(n.position)
            .map(pos => new InputPosition(pos.offset, pos.line, pos.column))
            .getOrElse(InputPosition.empty),
          n.category
        )
      )
      .toList

  override def getGqlStatusObjects(): Iterable[GqlStatusObject] = ???
  override def iterator(): Iterator[Map[String, AnyRef]] = ???
  override def close(): Unit = {}
}

object HttpStatementResult {
  case class Response(results: Seq[Result], errors: Seq[Error], notifications: Option[Seq[Notification]])
  case class Error(code: String, message: String)

  case class Notification(
    code: String,
    title: String,
    description: String,
    severity: String,
    position: Position,
    category: String
  )
  case class Position(offset: Int, line: Int, column: Int)
  case class Result(columns: Seq[String], data: Seq[Row])
  case class Row(row: Seq[AnyRef])

  def fromResponse(response: HTTP.Response): HttpStatementResult = {
    val content = response.rawContent()

    def failUnableToDecode(exception: Throwable): Nothing =
      throw new Exception(s"Unable to decode json: $content", exception)

    def failNoResults(): Nothing =
      throw new Exception(s"Response contained no results: $content")

    def failMultipleResults(): Nothing =
      throw new Exception(s"Response contained multiple results: $content")

    val (result, notifications) = Try(HttpJson.read[Response](content)) match {
      case Failure(exception) => failUnableToDecode(exception)
      case Success(response) => response match {
          case Response(Seq(result), Seq(), notifications) => (result, notifications)
          case Response(_, Seq(error), _)                  => serverError(error)
          case Response(Seq(), _, _)                       => failNoResults()
          case Response(_, _, _)                           => failMultipleResults()
        }
    }

    HttpStatementResult(result, notifications.getOrElse(Seq.empty))
  }

  private def serverError(e: HttpStatementResult.Error): Nothing = {
    Status.Code.all.asScala.find(status => status.code.serialize == e.code) match {
      case Some(statusVal) => throw new Neo4jException(e.message) {
          override def status(): Status = statusVal
        }
      case None => throw new RuntimeException(e.message)
    }
  }
}
