/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.collector

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineFunSuite

trait SampleGraphs {
  self: ExecutionEngineFunSuite =>

  protected def createSteelfaceGraph(): Unit = {
    val users =
      graph.inTx {
        val users = (0 until 1000).map(i => createLabeledNode(Map("email" -> s"user$i@mail.com"), "User"))
        users.take(500).foreach(user => user.setProperty("lastName", "Steelface"+user.getId))
        users.take(300).foreach(user => user.setProperty("firstName", "Bob"+user.getId))
        users
      }

    val cars =
      graph.inTx {
        (0 until 120).map(i => createLabeledNode(Map("number" -> i), "Car"))
      }

    val rooms =
      graph.inTx {
        (0 until 150).map(i => createLabeledNode(Map("hotel" -> "Clarion", "number" -> i % 50), "Room"))
      }

    graph.inTx(
      for (source <- users.take(100))
        relate(source, cars.head, "OWNS")
    )

    graph.inTx(
      for (source <- users.take(70))
        relate(source, rooms.head, "OWNS")
    )

    graph.inTx(
      for (source <- users.take(150))
        relate(source, rooms.head, "STAYS_IN")
    )

    graph.createConstraint("User", "email")
    graph.createIndex("User", "lastName")
    graph.createIndex("User", "firstName", "lastName")
    graph.createIndex("Room", "hotel", "number")
    graph.createIndex("Car", "number")

    graph.inTx(
      graph.schema().awaitIndexesOnline(5, TimeUnit.MINUTES)
    )

    graph.inTx { // these are added after index uniqueness estimation
      (0 until 8).map(i => createLabeledNode(Map("number" -> i), "Car"))
    }
  }
}
