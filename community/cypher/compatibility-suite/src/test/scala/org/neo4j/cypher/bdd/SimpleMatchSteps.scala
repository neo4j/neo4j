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
package org.neo4j.cypher.bdd

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

import scala.collection.JavaConverters._

class SimpleMatchSteps extends ExecutionEngineFunSuite with ScalaDsl with EN {

  var result: InternalExecutionResult = null

  Before() { _ =>
    initTest()
  }

  After() { _ =>
    stopTest()
  }

  Given("""^init: (.*)$""") { (initQuery: String) =>
    execute(initQuery)
  }

  When("""^running: (.*)$""") { (query: String) =>
    result = execute(query)
  }

  Then("""^result:$""") { (names: DataTable) =>
    val expected = names.asMaps(classOf[String], classOf[String]).asScala.toList.map(_.asScala)
    result.toList should equal(expected)
  }
}
