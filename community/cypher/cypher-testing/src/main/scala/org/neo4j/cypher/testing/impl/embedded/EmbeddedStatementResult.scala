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
package org.neo4j.cypher.testing.impl.embedded

import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.Result
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala

case class EmbeddedStatementResult(private val embeddedResult: Result) extends StatementResult {
  override def columns(): Seq[String] = embeddedResult.columns().asScala.toList

  override def records(): Seq[Record] =
    embeddedResult.asScala.map(EmbeddedRecordConverter.convertMap).toList

  override def getNotifications(): List[Notification] =
    Iterables.asList(embeddedResult.getNotifications).asScala.toList

  override def getGqlStatusObjects(): Iterable[GqlStatusObject] =
    Iterables.asList(embeddedResult.getGqlStatusObjects).asScala.toList

  override def iterator(): Iterator[Map[String, AnyRef]] =
    embeddedResult.asScala.map(EmbeddedRecordConverter.convertMap)

  override def close(): Unit = embeddedResult.close()
}
