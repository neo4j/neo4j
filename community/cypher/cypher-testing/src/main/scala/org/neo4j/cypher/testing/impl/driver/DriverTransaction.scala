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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.driver.Transaction

case class DriverTransaction(private val driverTransaction: Transaction) extends CypherExecutorTransaction {

  override def execute(statement: String, parameters: Map[String, Any]): StatementResult = try {
    DriverStatementResult(driverTransaction.run(statement, DriverParameterConverter.convertParameters(parameters)))
  } catch {
    case t: Throwable => throw DriverRecordConverter.addStatus(t, t)
  }

  override def commit(): Unit = try {
    driverTransaction.commit()
  } catch {
    case t: Throwable => throw DriverRecordConverter.addStatus(t, t)
  }

  override def rollback(): Unit = try {
    driverTransaction.rollback()
  } catch {
    case t: Throwable => throw DriverRecordConverter.addStatus(t, t)
  }
}
