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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.function.example.JoinFunction
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.proc.Procedures

class UserDefinedFunctionTest extends DocumentingTestBase with QueryStatisticsTestSupport with HardReset {

  override def section = "functions"

  override def hardReset() = {
    super.hardReset()
    db.getDependencyResolver.resolveDependency(classOf[Procedures]).registerFunction(classOf[JoinFunction])
    db.inTx {
      for (name <- List("John", "Paul", "George", "Ringo")) {
        val node = db.createNode(Label.label("Member"))
        node.setProperty("name", name)
      }
    }
  }

  @Test def call_a_udf() {
    testQuery(
      title = "Call a user-defined function",
      text = "This calls the user-defined function `org.neo4j.procedure.example.join()`.",
      queryText = "MATCH (n:Member) RETURN org.neo4j.function.example.join(collect(n.name))",
      optionalResultExplanation = "",
      assertions = (p) => {
        p.toList === List("John,Paul,George,Ringo")
      })
  }
}
