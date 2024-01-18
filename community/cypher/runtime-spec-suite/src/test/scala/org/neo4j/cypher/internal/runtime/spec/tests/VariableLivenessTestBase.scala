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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.StaticGraphRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.VariableLivenessTestBase.AssertMemoryFreed
import org.neo4j.values.storable.Values.longValue

abstract class VariableLivenessTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends StaticGraphRuntimeTestSuite[CONTEXT](edition, runtime) {

  override def shouldSetup: Boolean = true
  override protected def createGraph(): Unit = {}

  test("free memory with eager") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "c")
      .prober(AssertMemoryFreed("b"))
      .eager()
      .projection("a+b AS c")
      .projection("1 AS a", "2 AS b")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "c").withSingleRow(1, 3)
  }

  test("free memory with multiple eagers") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "d")
      .prober(AssertMemoryFreed("b", "c"))
      .eager()
      .projection("a+c AS d")
      .prober(AssertMemoryFreed("b"))
      .eager()
      .projection("a+b AS c")
      .projection("1 AS a", "2 AS b")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "d").withSingleRow(1, 4)
  }

  test("free memory in sort") {
    val probe = recordingProbe("b")
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "c")
      .prober(probe)
      .sort()
      .projection("a+b AS c")
      .projection("1 AS a", "2 AS b")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    runtime.correspondingRuntimeOption match {
      case Some(CypherRuntimeOption.slotted) =>
        probe.seenRows shouldBe Array(Array(longValue(2)))
      case _ =>
        probe.seenRows shouldBe Array(Array(null))
    }
    runtimeResult should beColumns("a", "c").withSingleRow(1, 3)
  }

  test("free memory in sort 2") {
    val probe1 = recordingProbe("a", "b", "c")
    val probe2 = recordingProbe("a", "b", "c")
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("d")
      .prober(probe2)
      .sort("b ASC")
      .prober(probe1)
      .sort("a ASC")
      .projection("1 AS a", "2 AS b", "3 AS c", "4 AS d")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    runtime.correspondingRuntimeOption match {
      case Some(CypherRuntimeOption.slotted) =>
        probe1.seenRows shouldBe Array(Array(longValue(1), longValue(2), longValue(3)))
        probe2.seenRows shouldBe Array(Array(longValue(1), longValue(2), longValue(3)))
      case _ =>
        probe1.seenRows shouldBe Array(Array(longValue(1), longValue(2), null))
        probe2.seenRows shouldBe Array(Array(null, longValue(2), null))
    }
    runtimeResult should beColumns("d").withSingleRow(4)
  }

  test("do not discard variables on rhs of apply that are used after the apply") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.projection("a*2 AS c")
      .|.eager()
      .|.argument("a")
      .projection("1 AS a", "2 AS b")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b", "c").withSingleRow(1, 2, 2)
  }

  test("semi apply with eager operator on rhs") {
    givenGraph {
      val (nodes, _) = gridGraph()
      nodes.foreach(_.setProperty("propInt", 1))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("var0", "var1")
      .semiApply()
      .|.distinct("var2.propInt AS var5")
      .|.sort("var2 ASC", "var4 ASC", "var3 ASC")
      .|.allRelationshipsScan("(var2)-[var4]-(var3)", "var0", "var1")
      .unwind("['k'] AS var1")
      .relationshipCountFromCountStore("var0", Some("2,4"), Seq("AB", "BA", "DOWN"), None)
      .build()

    execute(query, runtime) should beColumns("var0", "var1")
      .withSingleRow(1L, "k")
  }
}

object VariableLivenessTestBase {
  case class MemoryNotFreedException(message: String) extends RuntimeException(message)

  case class AssertMemoryFreed(variables: String*) extends Prober.Probe {

    override def onRow(anyRow: AnyRef, state: AnyRef): Unit = {
      val row = anyRow.asInstanceOf[CypherRow]
      val nonNullVars = variables
        .map(name => name -> row.getByName(name))
        .filter { case (_, value) => value ne null }
      if (nonNullVars.nonEmpty) {
        throw MemoryNotFreedException(
          s"""Expected the following variables to be null:
             |${nonNullVars.map { case (name, value) => s"$name=$value" }.mkString("\n")}
             |""".stripMargin
        )
      }
    }
  }
}
