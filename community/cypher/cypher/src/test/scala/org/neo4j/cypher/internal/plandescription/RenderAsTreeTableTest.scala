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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DistinctColumns
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.planDescription
import org.neo4j.cypher.internal.plandescription.asPrettyString.distinctness
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.scalatest.BeforeAndAfterAll

import java.util.Locale

class RenderAsTreeTableTest extends CypherFunSuite with BeforeAndAfterAll with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val defaultLocale = Locale.getDefault

  override def beforeAll(): Unit = {
    // we change locale so we don't need to bother
    // with number format and such here
    Locale.setDefault(Locale.US)
  }

  override def afterAll(): Unit = {
    Locale.setDefault(defaultLocale)
  }

  private val id: Id = Id.INVALID_ID
  implicit override val idGen: IdGen = SameId(Id(1))

  test("node feeding from other node") {
    val leaf = planDescription(id, "LEAF", NoChildren, Seq.empty, Set())
    val plan = planDescription(id, "ROOT", SingleChild(leaf), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+----+
        || Operator | Id |
        |+----------+----+
        || +ROOT    | -1 |
        || |        +----+
        || +LEAF    | -1 |
        |+----------+----+
        |""".stripMargin
    )
  }

  test("node feeding from two nodes") {
    val leaf1 = planDescription(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+----+
        || Operator | Id |
        |+----------+----+
        || +ROOT    | -1 |
        || |\       +----+
        || | +LEAF2 | -1 |
        || |        +----+
        || +LEAF1   | -1 |
        |+----------+----+
        |""".stripMargin
    )
  }

  test("node feeding of node that is feeding of node") {
    val leaf = planDescription(id, "LEAF", NoChildren, Seq.empty, Set())
    val intermediate = planDescription(id, "INTERMEDIATE", SingleChild(leaf), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", SingleChild(intermediate), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+---------------+----+
        || Operator      | Id |
        |+---------------+----+
        || +ROOT         | -1 |
        || |             +----+
        || +INTERMEDIATE | -1 |
        || |             +----+
        || +LEAF         | -1 |
        |+---------------+----+
        |""".stripMargin
    )
  }

  test("root with two leafs, one of which is deep") {
    val leaf1 = planDescription(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, Seq.empty, Set())
    val leaf3 = planDescription(id, "LEAF3", NoChildren, Seq.empty, Set())
    val intermediate = planDescription(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf3, intermediate), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+----+
        || Operator        | Id |
        |+-----------------+----+
        || +ROOT           | -1 |
        || |\              +----+
        || | +INTERMEDIATE | -1 |
        || | |\            +----+
        || | | +LEAF2      | -1 |
        || | |             +----+
        || | +LEAF1        | -1 |
        || |               +----+
        || +LEAF3          | -1 |
        |+-----------------+----+
        |""".stripMargin
    )
  }

  test("root with two intermediate nodes coming from four leaf nodes") {
    val leaf1 = planDescription(id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = planDescription(id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = planDescription(id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = planDescription(id, "LEAF", NoChildren, Seq(), Set("d"))
    val intermediate1 = planDescription(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val intermediate2 = planDescription(id, "INTERMEDIATE", TwoChildren(leaf3, leaf4), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+----+
        || Operator        | Id |
        |+-----------------+----+
        || +ROOT           | -1 |
        || |\              +----+
        || | +INTERMEDIATE | -1 |
        || | |\            +----+
        || | | +LEAF       | -1 |
        || | |             +----+
        || | +LEAF         | -1 |
        || |               +----+
        || +INTERMEDIATE   | -1 |
        || |\              +----+
        || | +LEAF         | -1 |
        || |               +----+
        || +LEAF           | -1 |
        |+-----------------+----+
        |""".stripMargin
    )
  }

  test("complex tree") {
    val leaf1 = planDescription(
      id,
      "LEAF1",
      NoChildren,
      Seq(
        Rows(42),
        DbHits(33),
        PageCacheHits(1),
        PageCacheMisses(2),
        EstimatedRows(1)
      ),
      Set()
    )
    val leaf2 = planDescription(
      id,
      "LEAF2",
      NoChildren,
      Seq(
        Rows(9),
        DbHits(2),
        PageCacheHits(2),
        PageCacheMisses(3),
        EstimatedRows(1)
      ),
      Set()
    )
    val leaf3 = planDescription(
      id,
      "LEAF3",
      NoChildren,
      Seq(
        Rows(9),
        DbHits(2),
        PageCacheHits(3),
        PageCacheMisses(4),
        EstimatedRows(1)
      ),
      Set()
    )
    val pass = planDescription(
      id,
      "PASS",
      SingleChild(leaf2),
      Seq(
        Rows(4),
        DbHits(0),
        PageCacheHits(4),
        PageCacheMisses(1),
        EstimatedRows(4)
      ),
      Set()
    )
    val inner = planDescription(
      id,
      "INNER",
      TwoChildren(leaf1, pass),
      Seq(
        Rows(7),
        DbHits(42),
        PageCacheHits(5),
        PageCacheMisses(2),
        EstimatedRows(6)
      ),
      Set()
    )
    val plan = planDescription(
      id,
      "ROOT",
      TwoChildren(leaf3, inner),
      Seq(
        Rows(3),
        DbHits(0),
        PageCacheHits(7),
        PageCacheMisses(10),
        EstimatedRows(1)
      ),
      Set()
    )
    val parent = planDescription(id, "PARENT", SingleChild(plan), Seq(), Set())

    renderAsTreeTable(parent) should equal(
      """+------------+----+----------------+------+---------+------------------------+
        || Operator   | Id | Estimated Rows | Rows | DB Hits | Page Cache Hits/Misses |
        |+------------+----+----------------+------+---------+------------------------+
        || +PARENT    | -1 |                |      |         |                        |
        || |          +----+----------------+------+---------+------------------------+
        || +ROOT      | -1 |              1 |    3 |       0 |                   7/10 |
        || |\         +----+----------------+------+---------+------------------------+
        || | +INNER   | -1 |              6 |    7 |      42 |                    5/2 |
        || | |\       +----+----------------+------+---------+------------------------+
        || | | +PASS  | -1 |              4 |    4 |       0 |                    4/1 |
        || | | |      +----+----------------+------+---------+------------------------+
        || | | +LEAF2 | -1 |              1 |    9 |       2 |                    2/3 |
        || | |        +----+----------------+------+---------+------------------------+
        || | +LEAF1   | -1 |              1 |   42 |      33 |                    1/2 |
        || |          +----+----------------+------+---------+------------------------+
        || +LEAF3     | -1 |              1 |    9 |       2 |                    3/4 |
        |+------------+----+----------------+------+---------+------------------------+
        |""".stripMargin
    )
  }

  private val argument = plans.Argument()

  test("single node is represented nicely") {
    val arguments = Seq(
      details("details"),
      Rows(42),
      DbHits(33),
      EstimatedRows(1)
    )

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+----+---------+----------------+------+---------+
        || Operator | Id | Details | Estimated Rows | Rows | DB Hits |
        |+----------+----+---------+----------------+------+---------+
        || +NAME    | -1 | details |              1 |   42 |      33 |
        |+----------+----+---------+----------------+------+---------+
        |""".stripMargin
    )
  }

  test("extra details are not a problem") {
    val arguments = Seq(
      details(Seq("a", "b")),
      Rows(42),
      DbHits(33),
      EstimatedRows(1)
    )

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderAsTreeTable(plan) should equal(
      """+----------+----+---------+----------------+------+---------+
        || Operator | Id | Details | Estimated Rows | Rows | DB Hits |
        |+----------+----+---------+----------------+------+---------+
        || +NAME    | -1 | a, b    |              1 |   42 |      33 |
        |+----------+----+---------+----------------+------+---------+
        |""".stripMargin
    )
  }

  test("super many details stretches the column") {
    val arguments = Seq(
      details(Seq("aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff")),
      Rows(42),
      DbHits(33),
      EstimatedRows(1)
    )

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderAsTreeTable(plan) should equal(
      """+----------+----+------------------------------------------+----------------+------+---------+
        || Operator | Id | Details                                  | Estimated Rows | Rows | DB Hits |
        |+----------+----+------------------------------------------+----------------+------+---------+
        || +NAME    | -1 | aaaaa, bbbbb, ccccc, ddddd, eeeee, fffff |              1 |   42 |      33 |
        |+----------+----+------------------------------------------+----------------+------+---------+
        |""".stripMargin
    )
  }

  test("execution plan without profiler stats are not shown") {
    val arguments = Seq(EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+----+----------------+
        || Operator | Id | Estimated Rows |
        |+----------+----+----------------+
        || +NAME    | -1 |              1 |
        |+----------+----+----------------+
        |""".stripMargin
    )
  }

  test("pipeline information is rendered in correct column") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1), Memory(5))
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), PipelineInfo(52, true), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = planDescription(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        || Operator | Id | Details     | Estimated Rows | Rows | DB Hits | Memory (Bytes) | Pipeline             |
        |+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME    | -1 | Index stuff |              1 |    2 |     633 |                | Fused in Pipeline 52 |
        || |        +----+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME    | -1 |             |              1 |   42 |      33 |              5 |                      |
        |+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        |""".stripMargin
    )
  }

  test("pipeline information is rendered correctly with multiple last operators that do not have pipeline info") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1), Memory(5))
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), PipelineInfo(52, true), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME1", NoChildren, args1, Set("a"))
    val plan2 = planDescription(id, "NAME2", SingleChild(plan1), args1, Set("b"))
    val plan3 = planDescription(id, "NAME3", SingleChild(plan2), args2, Set("c"))

    renderAsTreeTable(plan3) should equal(
      """+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        || Operator | Id | Details     | Estimated Rows | Rows | DB Hits | Memory (Bytes) | Pipeline             |
        |+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME3   | -1 | Index stuff |              1 |    2 |     633 |                | Fused in Pipeline 52 |
        || |        +----+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME2   | -1 |             |              1 |   42 |      33 |              5 |                      |
        || |        +----+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME1   | -1 |             |              1 |   42 |      33 |              5 |                      |
        |+----------+----+-------------+----------------+------+---------+----------------+----------------------+
        |""".stripMargin
    )
  }

  test("plan information is rendered on the corresponding row to the tree") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1), Memory(5))
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = planDescription(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+----+-------------+----------------+------+---------+----------------+
        || Operator | Id | Details     | Estimated Rows | Rows | DB Hits | Memory (Bytes) |
        |+----------+----+-------------+----------------+------+---------+----------------+
        || +NAME    | -1 | Index stuff |              1 |    2 |     633 |                |
        || |        +----+-------------+----------------+------+---------+----------------+
        || +NAME    | -1 |             |              1 |   42 |      33 |              5 |
        |+----------+----+-------------+----------------+------+---------+----------------+
        |""".stripMargin
    )
  }

  test("Anonymizes anonymous variables in provided order") {
    val expandPlan =
      Expand(argument, varFor("from"), SemanticDirection.INCOMING, Seq.empty, varFor("to"), varFor("rel"), ExpandAll)
    val providedOrders = new ProvidedOrders
    providedOrders.set(expandPlan.id, DefaultProvidedOrderFactory.asc(varFor("  UNNAMED42")))
    val description = LogicalPlan2PlanDescription(
      readOnly = true,
      new EffectiveCardinalities,
      withRawCardinalities = false,
      withDistinctness = false,
      providedOrders,
      StubExecutionPlan().operatorMetadata,
      CancellationChecker.neverCancelled()
    )

    renderAsTreeTable(description.create(expandPlan)) should equal(
      """+--------------+----+--------------------+-------------+
        || Operator     | Id | Details            | Ordered by  |
        |+--------------+----+--------------------+-------------+
        || +Expand(All) |  1 | (from)<-[rel]-(to) | anon_42 ASC |
        |+--------------+----+--------------------+-------------+
        |""".stripMargin
    )
  }

  test("don't render planner/runtime configs in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"), // should not be rendered
      RuntimeVersion("5.0"), // should not be rendered
      BatchSize(123), // should not be rendered
      Runtime("PIPELINED"), // should not be rendered
      details("`id`"),
      EstimatedRows(1)
    )

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----+---------+----------------+------+---------+
        || Operator | Id | Details | Estimated Rows | Rows | DB Hits |
        |+----------+----+---------+----------------+------+---------+
        || +NAME    | -1 | `id`    |              1 |   42 |      33 |
        |+----------+----+---------+----------------+------+---------+
        |""".stripMargin
    )
  }

  test("round estimated rows to int") {
    val planDescr1 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(0.00123456789)
      ),
      Set.empty
    )
    val planDescr2 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(1.23456789)
      ),
      Set.empty
    )

    renderAsTreeTable(planDescr1) should equal(
      """+------------------+----+---------+----------------+
        || Operator         | Id | Details | Estimated Rows |
        |+------------------+----+---------+----------------+
        || +NodeByLabelScan | -1 | n:Foo   |              0 |
        |+------------------+----+---------+----------------+
        |""".stripMargin
    )

    renderAsTreeTable(planDescr2) should equal(
      """+------------------+----+---------+----------------+
        || Operator         | Id | Details | Estimated Rows |
        |+------------------+----+---------+----------------+
        || +NodeByLabelScan | -1 | n:Foo   |              1 |
        |+------------------+----+---------+----------------+
        |""".stripMargin
    )
  }

  test("Don't round estimated rows when using rawCardinalities") {
    val planDescr1 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(0.00123456789, Some(1))
      ),
      Set.empty
    )
    val planDescr2 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(1.23456789, Some(1))
      ),
      Set.empty
    )

    renderAsTreeTable(planDescr1, withRawCardinalities = true) should equal(
      """+------------------+----+---------+---------------------+
        || Operator         | Id | Details | Estimated Rows      |
        |+------------------+----+---------+---------------------+
        || +NodeByLabelScan | -1 | n:Foo   | 1.0 (0.00123456789) |
        |+------------------+----+---------+---------------------+
        |""".stripMargin
    )

    renderAsTreeTable(planDescr2, withRawCardinalities = true) should equal(
      """+------------------+----+---------+------------------+
        || Operator         | Id | Details | Estimated Rows   |
        |+------------------+----+---------+------------------+
        || +NodeByLabelScan | -1 | n:Foo   | 1.0 (1.23456789) |
        |+------------------+----+---------+------------------+
        |""".stripMargin
    )
  }

  test("Render distinctness if requested") {
    val pD = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        distinctness(DistinctColumns("n"))
      ),
      Set.empty
    )

    renderAsTreeTable(pD, withDistinctness = true) should equal(
      """+------------------+----+---------+--------------+
        || Operator         | Id | Details | Distinctness |
        |+------------------+----+---------+--------------+
        || +NodeByLabelScan | -1 | n:Foo   | n            |
        |+------------------+----+---------+--------------+
        |""".stripMargin
    )
  }

  test("Do not render distinctness if not available") {
    val pD = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo")
      ),
      Set.empty
    )

    renderAsTreeTable(pD, withDistinctness = true) should equal(
      """+------------------+----+---------+
        || Operator         | Id | Details |
        |+------------------+----+---------+
        || +NodeByLabelScan | -1 | n:Foo   |
        |+------------------+----+---------+
        |""".stripMargin
    )
  }

  test("Do not render distinctness if not requested") {
    val pD = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        distinctness(DistinctColumns("n"))
      ),
      Set.empty
    )

    // noinspection RedundantDefaultArgument
    renderAsTreeTable(pD, withDistinctness = false) should equal(
      """+------------------+----+---------+
        || Operator         | Id | Details |
        |+------------------+----+---------+
        || +NodeByLabelScan | -1 | n:Foo   |
        |+------------------+----+---------+
        |""".stripMargin
    )
  }

  test("Output 'unknown' when original cardinality is unknown") {
    val planDescr1 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(0.00123456789)
      ),
      Set.empty
    )
    val planDescr2 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(1.23456789)
      ),
      Set.empty
    )

    renderAsTreeTable(planDescr1, withRawCardinalities = true) should equal(
      """+------------------+----+---------+-------------------------+
        || Operator         | Id | Details | Estimated Rows          |
        |+------------------+----+---------+-------------------------+
        || +NodeByLabelScan | -1 | n:Foo   | Unknown (0.00123456789) |
        |+------------------+----+---------+-------------------------+
        |""".stripMargin
    )

    renderAsTreeTable(planDescr2, withRawCardinalities = true) should equal(
      """+------------------+----+---------+----------------------+
        || Operator         | Id | Details | Estimated Rows       |
        |+------------------+----+---------+----------------------+
        || +NodeByLabelScan | -1 | n:Foo   | Unknown (1.23456789) |
        |+------------------+----+---------+----------------------+
        |""".stripMargin
    )
  }

  /*
   * Tests for the query plan compaction system. This system will perform two types of compaction:
   *  - similar consecutive operators are merged
   *  - long list of variables are truncated
   *
   *  The definition of similar is:
   *  - same operator name
   *  - no 'other' field
   *
   *  When compacting variable lists:
   *  - we truncate the list and append '...'
   *  - total column width, including '...' is limited to threshold
   *  - at least one variable will always be printed (and '...' if necessary)
   *  - newly assigned variables are printed first, separated from older with ' -- '
   */

  test("compact two identical nodes") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set())
    val plan = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+----+
        || Operator | Id |
        |+----------+----+
        || +NODE(2) | -1 |
        |+----------+----+
        |""".stripMargin
    )
  }

  test("compact two similar nodes with variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))

    renderAsTreeTable(plan) should equal(
      """+----------+----+
        || Operator | Id |
        |+----------+----+
        || +NODE(2) | -1 |
        |+----------+----+
        |""".stripMargin
    )
  }

  test("compact two pairs of similar nodes with variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("c"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("d"))

    renderAsTreeTable(p3) should equal(
      """+--------------+----+
        || Operator     | Id |
        |+--------------+----+
        || +OPERATOR(2) | -1 |
        || |            +----+
        || +NODE(2)     | -1 |
        |+--------------+----+
        |""".stripMargin
    )
  }

  test("compact two pairs of similar nodes with same variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b"))

    renderAsTreeTable(p3) should equal(
      """+--------------+----+
        || Operator     | Id |
        |+--------------+----+
        || +OPERATOR(2) | -1 |
        || |            +----+
        || +NODE(2)     | -1 |
        |+--------------+----+
        |""".stripMargin
    )
  }

  test("compact two pairs of similar nodes with one new variable") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b", "c"))

    renderAsTreeTable(p3) should equal(
      """+--------------+----+
        || Operator     | Id |
        |+--------------+----+
        || +OPERATOR(2) | -1 |
        || |            +----+
        || +NODE(2)     | -1 |
        |+--------------+----+
        |""".stripMargin
    )
  }

  test("compact two pairs of similar nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")

    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("var_a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("var_a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, repeating + "var_A" + "var_B")

    renderAsTreeTable(p3) should equal(
      """+--------------+----+
        || Operator     | Id |
        |+--------------+----+
        || +OPERATOR(2) | -1 |
        || |            +----+
        || +NODE(2)     | -1 |
        |+--------------+----+
        |""".stripMargin
    )
  }

  test("compact only the sufficiently similar pair of two similar pairs of nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")

    val leaf = planDescription(id, "NODE", NoChildren, Seq(details("var_a:123")), Set())
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, repeating + "var_A" + "var_B")
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("var_a"))

    renderAsTreeTable(p3) should equal(
      """+--------------+----+-----------+
        || Operator     | Id | Details   |
        |+--------------+----+-----------+
        || +OPERATOR(2) | -1 |           |
        || |            +----+-----------+
        || +NODE        | -1 |           |
        || |            +----+-----------+
        || +NODE        | -1 | var_a:123 |
        |+--------------+----+-----------+
        |""".stripMargin
    )
  }

  test(
    "compact only the sufficiently similar pair of two similar pairs of nodes with many repeating variables and many columns"
  ) {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val l = details("var_a:123")
    val t = Time(12345678)
    val r = Rows(2)
    val d = DbHits(2)
    val leaf = planDescription(id, "NODE", NoChildren, Seq(l, t, r, d), Set())
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq(t, r, d), repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq(t, r, d), Set("var_a"))
    val p3 = planDescription(
      id,
      "OPERATOR",
      SingleChild(p2),
      Seq(t, r, d),
      repeating + "var_A" +
        "var_B"
    )

    renderAsTreeTable(p3) should equal(
      """+--------------+----+-----------+------+---------+-----------+
        || Operator     | Id | Details   | Rows | DB Hits | Time (ms) |
        |+--------------+----+-----------+------+---------+-----------+
        || +OPERATOR(2) | -1 |           |    2 |       4 |    24.691 |
        || |            +----+-----------+------+---------+-----------+
        || +NODE        | -1 |           |    2 |       2 |    12.346 |
        || |            +----+-----------+------+---------+-----------+
        || +NODE        | -1 | var_a:123 |    2 |       2 |    12.346 |
        |+--------------+----+-----------+------+---------+-----------+
        |""".stripMargin
    )
  }

  test(
    "do not compact two similar pairs of nodes with non-empty detail column and many repeating variables and many columns"
  ) {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val l = details("var_a:123")
    val t = Time(12345678)
    val r = Rows(2)
    val d = DbHits(2)
    val leaf = planDescription(id, "NODE", NoChildren, Seq(l, t, r, d), Set())
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq(l, t, r, d), repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq(l, t, r, d), Set())
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq(l, t, r, d), repeating + "var_A" + "var_B")

    renderAsTreeTable(p3) should equal(
      """+-----------+----+-----------+------+---------+-----------+
        || Operator  | Id | Details   | Rows | DB Hits | Time (ms) |
        |+-----------+----+-----------+------+---------+-----------+
        || +OPERATOR | -1 | var_a:123 |    2 |       2 |    12.346 |
        || |         +----+-----------+------+---------+-----------+
        || +OPERATOR | -1 | var_a:123 |    2 |       2 |    12.346 |
        || |         +----+-----------+------+---------+-----------+
        || +NODE     | -1 | var_a:123 |    2 |       2 |    12.346 |
        || |         +----+-----------+------+---------+-----------+
        || +NODE     | -1 | var_a:123 |    2 |       2 |    12.346 |
        |+-----------+----+-----------+------+---------+-----------+
        |""".stripMargin
    )
  }

  test("no compaction on complex plan with no repeated names") {
    val leaf1 = planDescription(id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = planDescription(id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = planDescription(id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = planDescription(id, "LEAF", NoChildren, Seq(), Set("d"))
    val branch1 = planDescription(id, "BRANCH", SingleChild(leaf1), Seq.empty, Set("a"))
    val branch2 = planDescription(id, "BRANCH", SingleChild(leaf2), Seq.empty, Set("b"))
    val branch3 = planDescription(id, "BRANCH", SingleChild(leaf3), Seq.empty, Set("c"))
    val branch4 = planDescription(id, "BRANCH", SingleChild(leaf4), Seq.empty, Set("d"))
    val intermediate1 = planDescription(id, "INTERMEDIATE", TwoChildren(branch1, branch2), Seq.empty, Set())
    val intermediate2 = planDescription(id, "INTERMEDIATE", TwoChildren(branch3, branch4), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+----+
        || Operator        | Id |
        |+-----------------+----+
        || +ROOT           | -1 |
        || |\              +----+
        || | +INTERMEDIATE | -1 |
        || | |\            +----+
        || | | +BRANCH     | -1 |
        || | | |           +----+
        || | | +LEAF       | -1 |
        || | |             +----+
        || | +BRANCH       | -1 |
        || | |             +----+
        || | +LEAF         | -1 |
        || |               +----+
        || +INTERMEDIATE   | -1 |
        || |\              +----+
        || | +BRANCH       | -1 |
        || | |             +----+
        || | +LEAF         | -1 |
        || |               +----+
        || +BRANCH         | -1 |
        || |               +----+
        || +LEAF           | -1 |
        |+-----------------+----+
        |""".stripMargin
    )
  }

  test("compaction on complex plan with repeated names") {
    val leaf1 = planDescription(id, "NODE", NoChildren, Seq(), Set())
    val leaf2 = planDescription(id, "NODE", NoChildren, Seq(), Set())
    val leaf3 = planDescription(id, "NODE", NoChildren, Seq(), Set())
    val leaf4 = planDescription(id, "NODE", NoChildren, Seq(), Set())
    val branch1 = planDescription(id, "NODE", SingleChild(leaf1), Seq.empty, Set())
    val branch2 = planDescription(id, "NODE", SingleChild(leaf2), Seq.empty, Set())
    val branch3 = planDescription(id, "NODE", SingleChild(leaf3), Seq.empty, Set())
    val branch4 = planDescription(id, "NODE", SingleChild(leaf4), Seq.empty, Set())
    val intermediate1 = planDescription(id, "NODE", TwoChildren(branch1, branch2), Seq.empty, Set())
    val intermediate2 = planDescription(id, "NODE", TwoChildren(branch3, branch4), Seq.empty, Set())
    val plan = planDescription(id, "NODE", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+--------------+----+
        || Operator     | Id |
        |+--------------+----+
        || +NODE        | -1 |
        || |\           +----+
        || | +NODE      | -1 |
        || | |\         +----+
        || | | +NODE(2) | -1 |
        || | |          +----+
        || | +NODE(2)   | -1 |
        || |            +----+
        || +NODE        | -1 |
        || |\           +----+
        || | +NODE(2)   | -1 |
        || |            +----+
        || +NODE(2)     | -1 |
        |+--------------+----+
        |""".stripMargin
    )
  }

  test("should write long details on multiple lines") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq(details((0 until 35).map(_.toString))), Set())
    val root = planDescription(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+----+---------------------------------------------------------------------------------------------------+
        || Operator | Id | Details                                                                                           |
        |+----------+----+---------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | 0, 1, 2, 3, 4                                                                                     |
        || |        +----+---------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, |
        ||          |    | 27, 28, 29, 30, 31, 32, 33, 34                                                                    |
        |+----------+----+---------------------------------------------------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("should write details with newlines") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq(details("String\nwith\nline\nbreaks")), Set())
    val root = planDescription(id, "NODE", SingleChild(leaf), Seq(details("foo\r\nbar")), Set())

    renderAsTreeTable(root) should equal(
      """+----------+----+---------+
        || Operator | Id | Details |
        |+----------+----+---------+
        || +NODE    | -1 | foo     |
        || |        |    | bar     |
        || |        +----+---------+
        || +NODE    | -1 | String  |
        ||          |    | with    |
        ||          |    | line    |
        ||          |    | breaks  |
        |+----------+----+---------+
        |""".stripMargin
    )
  }

  test("should write details with newlines, but keep other details on one line as long as possible") {
    val leaf = planDescription(
      id,
      "NODE",
      NoChildren,
      Seq(
        details((0 until 35).map(_.toString) ++ Seq("String\nwith\nline\nbreaks") ++ (0 until 35).map(_.toString))
      ),
      Set()
    )
    val root = planDescription(id, "NODE", SingleChild(leaf), Seq(details(Seq("foo\r\nbar", "baz\r\nboom"))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+----+---------------------------------------------------------------------------------------------------+
        || Operator | Id | Details                                                                                           |
        |+----------+----+---------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | foo                                                                                               |
        || |        |    | bar,                                                                                              |
        || |        |    | baz                                                                                               |
        || |        |    | boom                                                                                              |
        || |        +----+---------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, |
        ||          |    | 27, 28, 29, 30, 31, 32, 33, 34,                                                                   |
        ||          |    | String                                                                                            |
        ||          |    | with                                                                                              |
        ||          |    | line                                                                                              |
        ||          |    | breaks,                                                                                           |
        ||          |    | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, |
        ||          |    | 27, 28, 29, 30, 31, 32, 33, 34                                                                    |
        |+----------+----+---------------------------------------------------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("should split too long word in details rows on multiple lines") {
    val leaf = PlanDescriptionImpl(
      id,
      "NODE",
      NoChildren,
      Seq(details(Seq((0 until 101).map(_ => "a").mkString(""), "b"))),
      Set()
    )
    val root = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+----+------------------------------------------------------------------------------------------------------+
        || Operator | Id | Details                                                                                              |
        |+----------+----+------------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | 0, 1, 2, 3, 4                                                                                        |
        || |        +----+------------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        ||          |    | a, b                                                                                                 |
        |+----------+----+------------------------------------------------------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("should add separator to next row if word exactly fits the row") {
    val leaf = PlanDescriptionImpl(
      id,
      "NODE",
      NoChildren,
      Seq(details(Seq((0 until 100).map(_ => "a").mkString(""), "b"))),
      Set()
    )
    val root = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+----+------------------------------------------------------------------------------------------------------+
        || Operator | Id | Details                                                                                              |
        |+----------+----+------------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | 0, 1, 2, 3, 4                                                                                        |
        || |        +----+------------------------------------------------------------------------------------------------------+
        || +NODE    | -1 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        ||          |    | , b                                                                                                  |
        |+----------+----+------------------------------------------------------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("multiline details on table with child plans") {
    val leaf1 = PlanDescriptionImpl(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf3 = PlanDescriptionImpl(id, "LEAF3", NoChildren, Seq(details("c" * 101)), Set())
    val leaf2 = PlanDescriptionImpl(id, "LEAF2", NoChildren, Seq(details("b" * 101)), Set())
    val intermediate =
      PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq(details("c" * 101)), Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(leaf3, intermediate), Seq(details("a" * 101)), Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+----+------------------------------------------------------------------------------------------------------+
        || Operator        | Id | Details                                                                                              |
        |+-----------------+----+------------------------------------------------------------------------------------------------------+
        || +ROOT           | -1 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        || |               |    | a                                                                                                    |
        || |\              +----+------------------------------------------------------------------------------------------------------+
        || | +INTERMEDIATE | -1 | cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc |
        || | |             |    | c                                                                                                    |
        || | |\            +----+------------------------------------------------------------------------------------------------------+
        || | | +LEAF2      | -1 | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
        || | |             |    | b                                                                                                    |
        || | |             +----+------------------------------------------------------------------------------------------------------+
        || | +LEAF1        | -1 |                                                                                                      |
        || |               +----+------------------------------------------------------------------------------------------------------+
        || +LEAF3          | -1 | cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc |
        ||                 |    | c                                                                                                    |
        |+-----------------+----+------------------------------------------------------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("format empty details") {
    renderAsTreeTable.splitDetails(List(), 10) should be(Seq())
  }

  test("format single short detail") {
    renderAsTreeTable.splitDetails(List("12345678"), 10) should be(Seq("12345678"))
  }

  test("format single exactly fitting detail") {
    renderAsTreeTable.splitDetails(List("1234567890"), 10) should be(Seq("1234567890"))
  }

  test("Should not split single detail into multiple lines if it can fit on a separate line") {
    renderAsTreeTable.splitDetails(List("123456", "123456"), 10) should be(Seq("123456,", "123456"))
  }

  test("format single too long detail") {
    renderAsTreeTable.splitDetails(List("12345678901"), 10) should be(Seq("1234567890", "1"))
  }

  test("format multiple too long detail") {
    renderAsTreeTable.splitDetails(List("1234567890123456789", "1234567890123456789"), 10) should be(Seq(
      "1234567890",
      "123456789,",
      "1234567890",
      "123456789"
    ))
  }

  test("format two short details") {
    renderAsTreeTable.splitDetails(List("abc", "def"), 10) should be(Seq("abc, def"))
  }

  test("format four short details, in sum too long") {
    renderAsTreeTable.splitDetails(List("123", "123", "1234", "12345"), 15) should be(Seq("123, 123, 1234,", "12345"))
  }

  test("format one char details") {
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 3) should be(Seq("1,", "2,", "3,", "4,", "5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 4) should be(Seq("1,", "2,", "3,", "4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 5) should be(Seq("1, 2,", "3, 4,", "5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 6) should be(Seq("1, 2,", "3, 4,", "5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 7) should be(Seq("1, 2,", "3, 4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 8) should be(Seq("1, 2, 3,", "4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 9) should be(Seq("1, 2, 3,", "4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 10) should be(Seq("1, 2, 3,", "4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 11) should be(Seq("1, 2, 3, 4,", "5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 12) should be(Seq("1, 2, 3, 4,", "5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 13) should be(Seq("1, 2, 3, 4, 5"))
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 14) should be(Seq("1, 2, 3, 4, 5"))
  }

  test("format one short and one long detail") {
    renderAsTreeTable.splitDetails(List("123", "1234567890123456789"), 15) should be(Seq(
      "123, 1234567890",
      "123456789"
    ))
  }

  test("does not strip leading and trailing whitespace from details") {
    renderAsTreeTable.splitDetails(List("  123  ", "  1234567890123456789  ", "  123\n  456  "), 10) should be(Seq(
      "  123  ,  ", // 1 separator space and first space of "  1235..."
      " 123456789",
      "0123456789",
      "  ,",
      "  123",
      "  456  "
    ))
  }

  test("format single multi-line detail") {
    renderAsTreeTable.splitDetails(List("123\n1234567890123456789"), 100) should be(Seq(
      "123",
      "1234567890123456789"
    ))
  }

  test("format single multi-line detail with indentation") {
    val detail =
      """
        |[a, b, c]
        |    ^
        |    this is the error
        |""".stripMargin
    renderAsTreeTable.splitDetails(List(detail), 100) should be(Seq(
      "[a, b, c]",
      "    ^",
      "    this is the error"
    ))
  }

  test("format single multi-line detail that needs to be split because of max-line length") {
    renderAsTreeTable.splitDetails(List("123\n1234567890123456789\n12345\n1234567"), 10) should be(Seq(
      "123",
      "1234567890",
      "123456789",
      "12345",
      "1234567"
    ))
  }

  test("format multi-line detail that needs to be split because of max-line length together with other details") {
    renderAsTreeTable.splitDetails(
      List("abc", "123\n1234567890123456789", "abc", "123\n12345678901234567890", "abc"),
      10
    ) should be(Seq(
      "abc,",
      "123",
      "1234567890",
      "123456789,",
      "abc,",
      "123",
      "1234567890",
      "1234567890",
      ", abc"
    ))
  }

  test("format multi-line detail before other detail - do not put on same line") {
    renderAsTreeTable.splitDetails(List("123\n123", "456"), 10) should be(Seq(
      "123",
      "123,",
      "456"
    ))
  }

  test("format multi-line detail before other detail - does not fit on same line") {
    renderAsTreeTable.splitDetails(List("123\n1234567", "123"), 10) should be(Seq(
      "123",
      "1234567,",
      "123"
    ))
  }

  test("format multi-line detail before other detail - does not fit on same line (including separator)") {
    renderAsTreeTable.splitDetails(List("123\n1234567890", "123"), 10) should be(Seq(
      "123",
      "1234567890",
      ", 123"
    ))
  }

  test("format multi-line detail after other detail - do not put on same line") {
    renderAsTreeTable.splitDetails(List("456", "123\n123"), 10) should be(Seq(
      "456,",
      "123",
      "123"
    ))
  }

  test("format multiple multi-line details") {
    renderAsTreeTable.splitDetails(List("123\n1234567890123456789", "1\n2\r\n3"), 100) should be(Seq(
      "123",
      "1234567890123456789,",
      "1",
      "2",
      "3"
    ))
  }

  test("format multi-line details together with single-line details") {
    renderAsTreeTable.splitDetails(
      List("123", "456", "78\n1234567890123456789\n1", "12", "123456789012", "34", "56"),
      10
    ) should be(Seq(
      "123, 456,",
      "78",
      "1234567890",
      "123456789",
      "1,",
      "12, 123456",
      "789012,",
      "34, 56"
    ))
  }

  test("MultiNodeIndexSeek") {
    val logicalPlan = MultiNodeIndexSeek(Seq(
      nodeIndexSeek("x:Label(Prop = 10,Foo = 1,Distance = 6,Name = 'Karoline Getinge')", unique = true).asInstanceOf[
        NodeIndexSeekLeafPlan
      ],
      nodeIndexSeek("y:Label(Prop = 12, Name = 'Foo')").asInstanceOf[NodeIndexSeekLeafPlan],
      nodeIndexSeek("z:Label(Prop > 100, Name = 'Bar')").asInstanceOf[NodeIndexSeekLeafPlan]
    ))
    val effectiveCardinalities = new EffectiveCardinalities
    effectiveCardinalities.set(logicalPlan.id, EffectiveCardinality(2.0))
    val plan = LogicalPlan2PlanDescription.create(
      logicalPlan,
      IDPPlannerName,
      readOnly = true,
      effectiveCardinalities,
      withRawCardinalities = false,
      withDistinctness = false,
      new ProvidedOrders,
      StubExecutionPlan().operatorMetadata,
      CancellationChecker.neverCancelled()
    )

    renderAsTreeTable(plan) should equal(
      """+---------------------+----+------------------------------------------------------------------------------------------------------+----------------+
        || Operator            | Id | Details                                                                                              | Estimated Rows |
        |+---------------------+----+------------------------------------------------------------------------------------------------------+----------------+
        || +MultiNodeIndexSeek |  1 | UNIQUE x:Label(Prop, Foo, Distance, Name) WHERE Prop = 10 AND Foo = 1 AND Distance = 6 AND Name = "K |              2 |
        ||                     |    | aroline Getinge", RANGE INDEX y:Label(Prop, Name) WHERE Prop = 12 AND Name = "Foo",                  |                |
        ||                     |    | RANGE INDEX z:Label(Prop, Name) WHERE Prop > 100 AND Name IS NOT NULL                                |                |
        |+---------------------+----+------------------------------------------------------------------------------------------------------+----------------+
        |""".stripMargin
    )
  }

  test("format merged cells in fused pipelines") {
    val args1 = Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1),
      Memory(5),
      PageCacheHits(5),
      PageCacheMisses(1),
      Time(200000),
      PipelineInfo(52, true)
    )
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), PipelineInfo(52, true), EstimatedRows(1))
    val args3 = Seq(Rows(2), DbHits(633), details("Do other stuff"), PipelineInfo(52, true), EstimatedRows(1))
    val args4 = Seq(
      Rows(2),
      DbHits(633),
      details("Even more stuff"),
      PageCacheHits(1),
      PageCacheMisses(2),
      Time(1000000),
      PipelineInfo(60, true),
      EstimatedRows(1)
    )
    val args5 = Seq(Rows(2), DbHits(633), details("Final stuff"), PipelineInfo(60, true), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME5", NoChildren, args1, Set.empty)
    val plan2 = planDescription(id, "NAME4", SingleChild(plan1), args2, Set.empty)
    val plan3 = planDescription(id, "NAME3", SingleChild(plan2), args3, Set.empty)
    val plan4 = planDescription(id, "NAME2", SingleChild(plan3), args4, Set.empty)
    val plan5 = planDescription(id, "NAME1", SingleChild(plan4), args5, Set.empty)

    renderAsTreeTable(plan5) should equal(
      """+----------+----+-----------------+----------------+------+---------+----------------+------------------------+-----------+----------------------+
        || Operator | Id | Details         | Estimated Rows | Rows | DB Hits | Memory (Bytes) | Page Cache Hits/Misses | Time (ms) | Pipeline             |
        |+----------+----+-----------------+----------------+------+---------+----------------+------------------------+-----------+----------------------+
        || +NAME1   | -1 | Final stuff     |              1 |    2 |     633 |                |                        |           |                      |
        || |        +----+-----------------+----------------+------+---------+----------------+                        |           |                      |
        || +NAME2   | -1 | Even more stuff |              1 |    2 |     633 |                |                    1/2 |     1.000 | Fused in Pipeline 60 |
        || |        +----+-----------------+----------------+------+---------+----------------+------------------------+-----------+----------------------+
        || +NAME3   | -1 | Do other stuff  |              1 |    2 |     633 |                |                        |           |                      |
        || |        +----+-----------------+----------------+------+---------+----------------+                        |           |                      |
        || +NAME4   | -1 | Index stuff     |              1 |    2 |     633 |                |                        |           |                      |
        || |        +----+-----------------+----------------+------+---------+----------------+                        |           |                      |
        || +NAME5   | -1 |                 |              1 |   42 |      33 |              5 |                    5/1 |     0.200 | Fused in Pipeline 52 |
        |+----------+----+-----------------+----------------+------+---------+----------------+------------------------+-----------+----------------------+
        |""".stripMargin
    )
  }

  test("format merged cells in fused pipelines, but not merge across same pipeline when one is not fused") {
    val produceResultsArgs = Seq(PageCacheHits(5), PageCacheMisses(1), Time(200000), PipelineInfo(1, false))
    val projectArgs = Seq()
    val allNodesScanArgs = Seq(PageCacheHits(1), PageCacheMisses(2), Time(1000000), PipelineInfo(1, true))

    val allNodesScan = planDescription(id, "ALLNODESSCAN", NoChildren, allNodesScanArgs, Set.empty)
    val project = planDescription(id, "PROJECT", SingleChild(allNodesScan), projectArgs, Set.empty)
    val produceResults = planDescription(id, "PRODUCERESULTS", SingleChild(project), produceResultsArgs, Set.empty)

    renderAsTreeTable(produceResults) should equal(
      """+-----------------+----+------------------------+-----------+---------------------+
        || Operator        | Id | Page Cache Hits/Misses | Time (ms) | Pipeline            |
        |+-----------------+----+------------------------+-----------+---------------------+
        || +PRODUCERESULTS | -1 |                    5/1 |     0.200 | In Pipeline 1       |
        || |               +----+------------------------+-----------+---------------------+
        || +PROJECT        | -1 |                        |           |                     |
        || |               +----+------------------------+-----------+---------------------+
        || +ALLNODESSCAN   | -1 |                    1/2 |     1.000 | Fused in Pipeline 1 |
        |+-----------------+----+------------------------+-----------+---------------------+
        |""".stripMargin
    )
  }

  test("format merged cells in fused pipeline with branch") {
    val leaf1Args = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      PipelineInfo(1, true)
    )
    val leaf2Args = Seq(Rows(2), DbHits(2), EstimatedRows(2), PipelineInfo(1, true))
    val planArgs = Seq(Rows(10), DbHits(0), EstimatedRows(9), PipelineInfo(1, true))

    val leaf1 = planDescription(id, "LEAF1", NoChildren, leaf1Args, Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, leaf2Args, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf1, leaf2), planArgs, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+----+----------------+------+---------+------------------------+-----------+---------------------+
        || Operator | Id | Estimated Rows | Rows | DB Hits | Page Cache Hits/Misses | Time (ms) | Pipeline            |
        |+----------+----+----------------+------+---------+------------------------+-----------+---------------------+
        || +ROOT    | -1 |              9 |   10 |       0 |                        |           |                     |
        || |\       +----+----------------+------+---------+                        |           |                     |
        || | +LEAF2 | -1 |              2 |    2 |       2 |                        |           |                     |
        || |        +----+----------------+------+---------+                        |           |                     |
        || +LEAF1   | -1 |              4 |    5 |       1 |                   5/10 |     0.200 | Fused in Pipeline 1 |
        |+----------+----+----------------+------+---------+------------------------+-----------+---------------------+
        |""".stripMargin
    )
  }

  test("format merged cells when pipeline fused across two child") {
    val indexSeekArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      PipelineInfo(1, true)
    )
    val filterArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), PipelineInfo(1, true))
    val allNodeScanArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      PipelineInfo(0, true)
    )
    val projectArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), PipelineInfo(0, true))
    val applyArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4))
    val aggregationArgs = Seq(Rows(5), DbHits(1), EstimatedRows(4), PipelineInfo(1, true))
    val produceResultArgs = Seq(
      Rows(5),
      DbHits(1),
      EstimatedRows(4),
      PageCacheHits(5),
      PageCacheMisses(10),
      Time(200000),
      PipelineInfo(2, false)
    )

    val indexSeek = planDescription(id, "INDEXSEEK", NoChildren, indexSeekArgs, Set())
    val filter = planDescription(id, "FILTER", SingleChild(indexSeek), filterArgs, Set())
    val allNodeScan = planDescription(id, "ALLNODESCAN", NoChildren, allNodeScanArgs, Set())
    val project = planDescription(id, "PROJECT", SingleChild(allNodeScan), projectArgs, Set())
    val apply = planDescription(id, "APPLY", TwoChildren(project, filter), applyArgs, Set())
    val aggregation = planDescription(id, "AGGREGATION", SingleChild(apply), aggregationArgs, Set())
    val produceResults = planDescription(id, "PRODUCERESULT", SingleChild(aggregation), produceResultArgs, Set())

    renderAsTreeTable(produceResults) should equal(
      """+----------------+----+----------------+------+---------+------------------------+-----------+---------------------+
        || Operator       | Id | Estimated Rows | Rows | DB Hits | Page Cache Hits/Misses | Time (ms) | Pipeline            |
        |+----------------+----+----------------+------+---------+------------------------+-----------+---------------------+
        || +PRODUCERESULT | -1 |              4 |    5 |       1 |                   5/10 |     0.200 | In Pipeline 2       |
        || |              +----+----------------+------+---------+------------------------+-----------+---------------------+
        || +AGGREGATION   | -1 |              4 |    5 |       1 |                        |           |                     |
        || |              +----+----------------+------+---------+                        |           |                     |
        || +APPLY         | -1 |              4 |    5 |       1 |                        |           |                     |
        || |\             +----+----------------+------+---------+                        |           |                     |
        || | +FILTER      | -1 |              4 |    5 |       1 |                        |           |                     |
        || | |            +----+----------------+------+---------+                        |           |                     |
        || | +INDEXSEEK   | -1 |              4 |    5 |       1 |                   5/10 |     0.200 | Fused in Pipeline 1 |
        || |              +----+----------------+------+---------+------------------------+-----------+---------------------+
        || +PROJECT       | -1 |              4 |    5 |       1 |                        |           |                     |
        || |              +----+----------------+------+---------+                        |           |                     |
        || +ALLNODESCAN   | -1 |              4 |    5 |       1 |                   5/10 |     0.200 | Fused in Pipeline 0 |
        |+----------------+----+----------------+------+---------+------------------------+-----------+---------------------+
        |""".stripMargin
    )
  }

  test("not merge columns of plan without pipeline info if it's surrounded by plans in other pipelines") {
    val node1 = planDescription(id, "Node1", NoChildren, Seq(PipelineInfo(1, fused = true), Time(1000000)))
    val node2 = planDescription(id, "Node2", SingleChild(node1), Seq(PipelineInfo(2, fused = true), Time(1000000)))
    val node3 = planDescription(id, "Node3", SingleChild(node2), Seq(PipelineInfo(3, fused = true), Time(1000000)))
    val node4 = planDescription(id, "Node4", SingleChild(node3), Seq(Time(1000000)))
    val node5 = planDescription(id, "Node5", SingleChild(node4), Seq(PipelineInfo(4, fused = true), Time(1000000)))

    renderAsTreeTable(node5) should equal(
      """+----------+----+-----------+---------------------+
        || Operator | Id | Time (ms) | Pipeline            |
        |+----------+----+-----------+---------------------+
        || +Node5   | -1 |     1.000 | Fused in Pipeline 4 |
        || |        +----+-----------+---------------------+
        || +Node4   | -1 |     1.000 |                     |
        || |        +----+-----------+---------------------+
        || +Node3   | -1 |     1.000 | Fused in Pipeline 3 |
        || |        +----+-----------+---------------------+
        || +Node2   | -1 |     1.000 | Fused in Pipeline 2 |
        || |        +----+-----------+---------------------+
        || +Node1   | -1 |     1.000 | Fused in Pipeline 1 |
        |+----------+----+-----------+---------------------+
        |""".stripMargin
    )
  }

  test(
    "not merge columns without pipeline info of plan with branching if it's surrounded by plans in other pipelines"
  ) {
    val leaf1 = planDescription(
      id,
      "Leaf1",
      NoChildren,
      Seq(PipelineInfo(5, fused = false), PageCacheHits(7), PageCacheMisses(6))
    )
    val leaf2 = planDescription(
      id,
      "Leaf2",
      NoChildren,
      Seq(PipelineInfo(4, fused = false), PageCacheHits(6), PageCacheMisses(5))
    )
    val leaf3 = planDescription(
      id,
      "Leaf3",
      NoChildren,
      Seq(PipelineInfo(3, fused = false), PageCacheHits(4), PageCacheMisses(3))
    )
    val leaf4 = planDescription(
      id,
      "Leaf4",
      NoChildren,
      Seq(PipelineInfo(2, fused = false), PageCacheHits(3), PageCacheMisses(2))
    )
    val intermediate1 =
      planDescription(id, "Intermediate1", TwoChildren(leaf1, leaf2), Seq(PageCacheHits(5), PageCacheMisses(4)))
    val intermediate2 =
      planDescription(id, "Intermediate2", TwoChildren(leaf3, leaf4), Seq(PageCacheHits(2), PageCacheMisses(1)))
    val plan = planDescription(
      id,
      "Root",
      TwoChildren(intermediate1, intermediate2),
      Seq(PipelineInfo(1, fused = false), PageCacheHits(1), PageCacheMisses(0)),
      Set()
    )

    renderAsTreeTable(plan) should equal(
      """+------------------+----+------------------------+---------------+
        || Operator         | Id | Page Cache Hits/Misses | Pipeline      |
        |+------------------+----+------------------------+---------------+
        || +Root            | -1 |                    1/0 | In Pipeline 1 |
        || |\               +----+------------------------+---------------+
        || | +Intermediate2 | -1 |                    2/1 |               |
        || | |\             +----+------------------------+---------------+
        || | | +Leaf4       | -1 |                    3/2 | In Pipeline 2 |
        || | |              +----+------------------------+---------------+
        || | +Leaf3         | -1 |                    4/3 | In Pipeline 3 |
        || |                +----+------------------------+---------------+
        || +Intermediate1   | -1 |                    5/4 |               |
        || |\               +----+------------------------+---------------+
        || | +Leaf2         | -1 |                    6/5 | In Pipeline 4 |
        || |                +----+------------------------+---------------+
        || +Leaf1           | -1 |                    7/6 | In Pipeline 5 |
        |+------------------+----+------------------------+---------------+
        |""".stripMargin
    )
  }

  test("merge columns fused over two apply (ldbc_sf010 read 6)") {
    val nodeUniqueIndexSeek1 = planDescription(
      id,
      "NodeUniqueIndexSeek",
      NoChildren,
      Seq(PipelineInfo(0, fused = false), PageCacheHits(2), PageCacheMisses(0), Time(8850000))
    )

    val nodeUniqueIndexSeek2 = planDescription(
      id,
      "NodeUniqueIndexSeek",
      NoChildren,
      Seq(PipelineInfo(1, fused = true), PageCacheHits(1537), PageCacheMisses(0), Time(21756000))
    )
    val varLengthExpand =
      planDescription(id, "VarLengthExpand(All)", SingleChild(nodeUniqueIndexSeek2), Seq(PipelineInfo(1, fused = true)))
    val filter1 = planDescription(id, "Filter", SingleChild(varLengthExpand), Seq(PipelineInfo(1, fused = true)))

    val cartesianProduct = planDescription(
      id,
      "CartesianProduct",
      TwoChildren(nodeUniqueIndexSeek1, filter1),
      Seq(PipelineInfo(2, fused = false), Time(5483000))
    )

    val distinct = planDescription(
      id,
      "Distinct",
      SingleChild(cartesianProduct),
      Seq(PipelineInfo(2, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(15565000))
    )

    val argument1 = planDescription(
      id,
      "Argument",
      NoChildren,
      Seq(PipelineInfo(3, fused = true), PageCacheHits(10957), PageCacheMisses(0), Time(65466000))
    )
    val expandAll1 = planDescription(id, "Expand(All)", SingleChild(argument1), Seq(PipelineInfo(3, fused = true)))

    val argument2 = planDescription(
      id,
      "Argument",
      NoChildren,
      Seq(PipelineInfo(4, fused = true), PageCacheHits(1211031), PageCacheMisses(0), Time(577688000))
    )
    val expandInto = planDescription(id, "Expand(Into)", SingleChild(argument2), Seq(PipelineInfo(4, fused = true)))
    val limit = planDescription(id, "Limit", SingleChild(expandInto), Seq(PipelineInfo(4, fused = true)))

    val apply1 = planDescription(id, "Apply", TwoChildren(expandAll1, limit))

    val apply2 = planDescription(id, "Apply", TwoChildren(distinct, apply1))

    val expandAll2 = planDescription(id, "Expand(All)", SingleChild(apply2), Seq(PipelineInfo(4, fused = true)))
    val filter2 = planDescription(id, "Filter", SingleChild(expandAll2), Seq(PipelineInfo(4, fused = true)))
    val eagerAggregation =
      planDescription(id, "EagerAggregation", SingleChild(filter2), Seq(PipelineInfo(4, fused = true)))
    val projection = planDescription(
      id,
      "Projection",
      SingleChild(eagerAggregation),
      Seq(PipelineInfo(5, fused = false), PageCacheHits(29), PageCacheMisses(0), Time(1492000))
    )
    val top = planDescription(
      id,
      "Top",
      SingleChild(projection),
      Seq(PipelineInfo(6, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(1726000))
    )
    val produceResult = planDescription(
      id,
      "ProduceResults",
      SingleChild(top),
      Seq(PipelineInfo(6, fused = false), PageCacheHits(0), PageCacheMisses(0), Time(1143000))
    )

    renderAsTreeTable(produceResult) should equal(
      """+-------------------------+----+------------------------+-----------+---------------------+
        || Operator                | Id | Page Cache Hits/Misses | Time (ms) | Pipeline            |
        |+-------------------------+----+------------------------+-----------+---------------------+
        || +ProduceResults         | -1 |                    0/0 |     1.143 |                     |
        || |                       +----+------------------------+-----------+                     |
        || +Top                    | -1 |                    0/0 |     1.726 | In Pipeline 6       |
        || |                       +----+------------------------+-----------+---------------------+
        || +Projection             | -1 |                   29/0 |     1.492 | In Pipeline 5       |
        || |                       +----+------------------------+-----------+---------------------+
        || +EagerAggregation       | -1 |                        |           |                     |
        || |                       +----+                        |           |                     |
        || +Filter                 | -1 |                        |           |                     |
        || |                       +----+                        |           |                     |
        || +Expand(All)            | -1 |                        |           |                     |
        || |                       +----+                        |           |                     |
        || +Apply                  | -1 |                        |           |                     |
        || |\                      +----+                        |           |                     |
        || | +Apply                | -1 |                        |           |                     |
        || | |\                    +----+                        |           |                     |
        || | | +Limit              | -1 |                        |           |                     |
        || | | |                   +----+                        |           |                     |
        || | | +Expand(Into)       | -1 |                        |           |                     |
        || | | |                   +----+                        |           |                     |
        || | | +Argument           | -1 |              1211031/0 |   577.688 | Fused in Pipeline 4 |
        || | |                     +----+------------------------+-----------+---------------------+
        || | +Expand(All)          | -1 |                        |           |                     |
        || | |                     +----+                        |           |                     |
        || | +Argument             | -1 |                10957/0 |    65.466 | Fused in Pipeline 3 |
        || |                       +----+------------------------+-----------+---------------------+
        || +Distinct               | -1 |                    0/0 |    15.565 |                     |
        || |                       +----+------------------------+-----------+                     |
        || +CartesianProduct       | -1 |                        |     5.483 | In Pipeline 2       |
        || |\                      +----+------------------------+-----------+---------------------+
        || | +Filter               | -1 |                        |           |                     |
        || | |                     +----+                        |           |                     |
        || | +VarLengthExpand(All) | -1 |                        |           |                     |
        || | |                     +----+                        |           |                     |
        || | +NodeUniqueIndexSeek  | -1 |                 1537/0 |    21.756 | Fused in Pipeline 1 |
        || |                       +----+------------------------+-----------+---------------------+
        || +NodeUniqueIndexSeek    | -1 |                    2/0 |     8.850 | In Pipeline 0       |
        |+-------------------------+----+------------------------+-----------+---------------------+
        |""".stripMargin
    )
  }

  test("merge order by and pipeline 1") {

    val p1 = PlanDescriptionImpl(
      id,
      "NODE",
      NoChildren,
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(1, false)),
      Set()
    )
    val p2 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p1),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(1, false)),
      Set()
    )
    val p3 = PlanDescriptionImpl(id, "NODE", SingleChild(p2), Seq(details("..."), PipelineInfo(1, false)), Set())
    val p4 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p3),
      Seq(details("..."), Order(PrettyString("A")), PipelineInfo(1, false)),
      Set()
    )
    val p5 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p4),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(1, false)),
      Set()
    )
    val p6 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p5),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(2, false)),
      Set()
    )
    val p7 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p6),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(2, false)),
      Set()
    )
    val root = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p7),
      Seq(details("..."), Order(PrettyString("b")), PipelineInfo(2, false)),
      Set()
    )

    renderAsTreeTable(root) should equal(
      """+----------+----+---------+------------+---------------+
        || Operator | Id | Details | Ordered by | Pipeline      |
        |+----------+----+---------+------------+---------------+
        || +NODE    | -1 | ...     | b          |               |
        || |        +----+---------+------------+               |
        || +NODE    | -1 | ...     |            |               |
        || |        +----+---------+            |               |
        || +NODE    | -1 | ...     |            | In Pipeline 2 |
        || |        +----+---------+            +---------------+
        || +NODE    | -1 | ...     | a          |               |
        || |        +----+---------+------------+               |
        || +NODE    | -1 | ...     | A          |               |
        || |        +----+---------+------------+               |
        || +NODE    | -1 | ...     |            |               |
        || |        +----+---------+------------+               |
        || +NODE    | -1 | ...     |            |               |
        || |        +----+---------+            |               |
        || +NODE    | -1 | ...     | a          | In Pipeline 1 |
        |+----------+----+---------+------------+---------------+
        |""".stripMargin
    )
  }

  test("merge order by and pipeline 2") {

    val p1 = PlanDescriptionImpl(
      id,
      "NODE",
      NoChildren,
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(1, false)),
      Set()
    )
    val p2 = PlanDescriptionImpl(id, "NODE", SingleChild(p1), Seq(details("..."), Order(PrettyString("b"))), Set())
    val p3 = PlanDescriptionImpl(id, "NODE", SingleChild(p2), Seq(details("..."), PipelineInfo(1, false)), Set())
    val p4 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p3),
      Seq(details("..."), Order(PrettyString("b")), PipelineInfo(2, true)),
      Set()
    )
    val p5 = PlanDescriptionImpl(id, "NODE", SingleChild(p4), Seq(details("..."), Order(PrettyString("b"))), Set())
    val p6 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p5),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(2, true)),
      Set()
    )
    val p7 = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p6),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(3, false)),
      Set()
    )
    val root = PlanDescriptionImpl(
      id,
      "NODE",
      SingleChild(p7),
      Seq(details("..."), Order(PrettyString("a")), PipelineInfo(3, false)),
      Set()
    )

    renderAsTreeTable(root) should equal(
      """+----------+----+---------+------------+---------------------+
        || Operator | Id | Details | Ordered by | Pipeline            |
        |+----------+----+---------+------------+---------------------+
        || +NODE    | -1 | ...     |            |                     |
        || |        +----+---------+            |                     |
        || +NODE    | -1 | ...     |            | In Pipeline 3       |
        || |        +----+---------+            +---------------------+
        || +NODE    | -1 | ...     | a          |                     |
        || |        +----+---------+------------+                     |
        || +NODE    | -1 | ...     |            |                     |
        || |        +----+---------+            |                     |
        || +NODE    | -1 | ...     | b          | Fused in Pipeline 2 |
        || |        +----+---------+------------+---------------------+
        || +NODE    | -1 | ...     |            | In Pipeline 1       |
        || |        +----+---------+------------+---------------------+
        || +NODE    | -1 | ...     | b          |                     |
        || |        +----+---------+------------+---------------------+
        || +NODE    | -1 | ...     | a          | In Pipeline 1       |
        |+----------+----+---------+------------+---------------------+
        |""".stripMargin
    )
  }

  test("parseMajorMinor") {
    Arguments.parseMajorMinor("4.1.53") shouldBe "4.1"
    Arguments.parseMajorMinor("5.1.0-SNAPSHOT") shouldBe "5.1"
    Arguments.parseMajorMinor("other version") shouldBe "other version"
  }
}
