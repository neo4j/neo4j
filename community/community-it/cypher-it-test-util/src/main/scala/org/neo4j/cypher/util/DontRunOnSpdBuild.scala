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
package org.neo4j.cypher.util

import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.test.TestDatabaseManagementServiceFactorySupplier.isSpd
import org.scalatest
import org.scalatest.Args
import org.scalatest.Status

trait DontRunOnSpdBuild extends CypherITTestSuite {

  override protected def runTests(testName: Option[String], args: Args): Status = {
    if (isSpd) scalatest.SucceededStatus
    else super.runTests(testName, args)
  }
}
