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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.StartsConcurrentTransactions
import org.neo4j.cypher.internal.runtime.StartsNoTransactions
import org.neo4j.cypher.internal.runtime.StartsSerialTransactions
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InterpretedRuntimeTransactionModeTest extends CypherFunSuite {

  private val concurrent = TransactionConcurrency.Concurrent(4)
  private val serial = TransactionConcurrency.Serial

  test("no transactional subquery starts no transactions") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .unwind("[1] AS x")
      .argument()
      .build()

    InterpretedRuntime.calculateTransactionMode(plan) shouldBe StartsNoTransactions
  }

  test("serial transactional subquery starts serial transactions") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionApply(concurrency = serial)
      .|.argument("x")
      .unwind("[1] AS x")
      .argument()
      .build()

    InterpretedRuntime.calculateTransactionMode(plan) shouldBe StartsSerialTransactions
  }

  test("concurrent transactional subquery starts concurrent transactions") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .transactionForeach(concurrency = concurrent)
      .|.argument("x")
      .unwind("[1] AS x")
      .argument()
      .build()

    InterpretedRuntime.calculateTransactionMode(plan) shouldBe StartsConcurrentTransactions
  }

  test("concurrent wins over a serial transactional subquery visited later") {
    // Union visits its left child before its right child, so the serial subquery is folded after
    // the concurrent one and must not downgrade the mode
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .union()
      .|.transactionApply(concurrency = serial)
      .|.|.argument("x")
      .|.unwind("[1] AS x")
      .|.argument()
      .transactionApply(concurrency = concurrent)
      .|.argument("x")
      .unwind("[1] AS x")
      .argument()
      .build()

    InterpretedRuntime.calculateTransactionMode(plan) shouldBe StartsConcurrentTransactions
  }

  test("concurrent wins over a serial transactional subquery visited earlier") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .union()
      .|.transactionApply(concurrency = concurrent)
      .|.|.argument("x")
      .|.unwind("[1] AS x")
      .|.argument()
      .transactionApply(concurrency = serial)
      .|.argument("x")
      .unwind("[1] AS x")
      .argument()
      .build()

    InterpretedRuntime.calculateTransactionMode(plan) shouldBe StartsConcurrentTransactions
  }
}
