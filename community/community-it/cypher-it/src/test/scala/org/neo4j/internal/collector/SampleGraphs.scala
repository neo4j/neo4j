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
package org.neo4j.internal.collector

import org.neo4j.cypher.GraphDatabaseTestSupport

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.IteratorHasAsScala

trait SampleGraphs {
  self: GraphDatabaseTestSupport =>

  protected def createSteelfaceGraph(): Unit = {
    graph.withTx(tx => {
      tx.schema().getIndexes.iterator().asScala.foreach(index => index.drop())
      tx.commit()
    })

    val users = (0 until 1000).map(i => createLabeledNode(Map("email" -> s"user$i@mail.com"), "User"))
    graph.withTx(tx => {
      users.take(500).foreach(user => tx.getNodeById(user.getId).setProperty("lastName", "Steelface" + user.getId))
      users.take(300).foreach(user => tx.getNodeById(user.getId).setProperty("firstName", "Bob" + user.getId))
    })

    val cars = (0 until 120).map(i => createLabeledNode(Map("number" -> i), "Car"))

    val rooms = (0 until 150).map(i => createLabeledNode(Map("hotel" -> "Clarion", "number" -> i % 50), "Room"))

    for (source <- users.take(100))
      relate(source, cars.head, "OWNS")

    for (source <- users.take(70))
      relate(source, rooms.head, "OWNS")

    for (source <- users.take(150))
      relate(source, rooms.head, "STAYS_IN")

    graph.createNodeUniquenessConstraint("User", "email")
    graph.createNodeIndex("User", "lastName")
    graph.createNodeIndex("User", "firstName", "lastName")
    graph.createNodeIndex("Room", "hotel", "number")
    graph.createNodeIndex("Car", "number")

    graph.withTx(tx => tx.schema().awaitIndexesOnline(5, TimeUnit.MINUTES))

    // these are added after index uniqueness estimation
    (0 until 8).map(i => createLabeledNode(Map("number" -> i), "Car"))
  }
}
