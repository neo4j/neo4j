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
package org.neo4j.cypher.internal.plandescription

import java.util.Locale

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.planDescription
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.scalatest.BeforeAndAfterAll

class RenderAsTreeTableTest extends CypherFunSuite with BeforeAndAfterAll with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val defaultLocale = Locale.getDefault
  override def beforeAll() {
    //we change locale so we don't need to bother
    //with number format and such here
    Locale.setDefault(Locale.US)
  }

  override def afterAll(): Unit = {
    Locale.setDefault(defaultLocale)
  }

  private val id: Id = Id.INVALID_ID

  test("node feeding from other node") {
    val leaf = planDescription(id, "LEAF", NoChildren, Seq.empty, Set())
    val plan = planDescription(id, "ROOT", SingleChild(leaf), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+
        || Operator |
        |+----------+
        || +ROOT    |
        || |        +
        || +LEAF    |
        |+----------+
        |""".stripMargin)
  }

  test("node feeding from two nodes") {
    val leaf1 = planDescription(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+
        || Operator |
        |+----------+
        || +ROOT    |
        || |\       +
        || | +LEAF2 |
        || |        +
        || +LEAF1   |
        |+----------+
        |""".stripMargin)
  }

  test("node feeding of node that is feeding of node") {
    val leaf = planDescription(id, "LEAF", NoChildren, Seq.empty, Set())
    val intermediate = planDescription(id, "INTERMEDIATE", SingleChild(leaf), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", SingleChild(intermediate), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+---------------+
        || Operator      |
        |+---------------+
        || +ROOT         |
        || |             +
        || +INTERMEDIATE |
        || |             +
        || +LEAF         |
        |+---------------+
        |""".stripMargin)
  }

  test("root with two leafs, one of which is deep") {
    val leaf1 = planDescription(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, Seq.empty, Set())
    val leaf3 = planDescription(id, "LEAF3", NoChildren, Seq.empty, Set())
    val intermediate = planDescription(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf3, intermediate), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+
        || Operator        |
        |+-----------------+
        || +ROOT           |
        || |\              +
        || | +INTERMEDIATE |
        || | |\            +
        || | | +LEAF2      |
        || | |             +
        || | +LEAF1        |
        || |               +
        || +LEAF3          |
        |+-----------------+
        |""".stripMargin)
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
      """+-----------------+
        || Operator        |
        |+-----------------+
        || +ROOT           |
        || |\              +
        || | +INTERMEDIATE |
        || | |\            +
        || | | +LEAF       |
        || | |             +
        || | +LEAF         |
        || |               +
        || +INTERMEDIATE   |
        || |\              +
        || | +LEAF         |
        || |               +
        || +LEAF           |
        |+-----------------+
        |""".stripMargin)
  }

  test("complex tree") {
    val leaf1 = planDescription(id, "LEAF1", NoChildren, Seq(
      Rows(42),
      DbHits(33),
      PageCacheHits(1),
      PageCacheMisses(2),
      EstimatedRows(1)), Set())
    val leaf2 = planDescription(id, "LEAF2", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      PageCacheHits(2),
      PageCacheMisses(3),
      EstimatedRows(1)), Set())
    val leaf3 = planDescription(id, "LEAF3", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      PageCacheHits(3),
      PageCacheMisses(4),
      EstimatedRows(1)), Set())
    val pass = planDescription(id, "PASS", SingleChild(leaf2), Seq(
      Rows(4),
      DbHits(0),
      PageCacheHits(4),
      PageCacheMisses(1),
      EstimatedRows(4)), Set())
    val inner = planDescription(id, "INNER", TwoChildren(leaf1, pass), Seq(
      Rows(7),
      DbHits(42),
      PageCacheHits(5),
      PageCacheMisses(2),
      EstimatedRows(6)), Set())
    val plan = planDescription(id, "ROOT", TwoChildren(leaf3, inner), Seq(
      Rows(3),
      DbHits(0),
      PageCacheHits(7),
      PageCacheMisses(10),
      EstimatedRows(1)), Set())
    val parent = planDescription(id, "PARENT", SingleChild(plan), Seq(), Set())

    renderAsTreeTable(parent) should equal(
      """+------------+----------------+------+---------+------------------------+
        || Operator   | Estimated Rows | Rows | DB Hits | Page Cache Hits/Misses |
        |+------------+----------------+------+---------+------------------------+
        || +PARENT    |                |      |         |                        |
        || |          +----------------+------+---------+------------------------+
        || +ROOT      |              1 |    3 |       0 |                   7/10 |
        || |\         +----------------+------+---------+------------------------+
        || | +INNER   |              6 |    7 |      42 |                    5/2 |
        || | |\       +----------------+------+---------+------------------------+
        || | | +PASS  |              4 |    4 |       0 |                    4/1 |
        || | | |      +----------------+------+---------+------------------------+
        || | | +LEAF2 |              1 |    9 |       2 |                    2/3 |
        || | |        +----------------+------+---------+------------------------+
        || | +LEAF1   |              1 |   42 |      33 |                    1/2 |
        || |          +----------------+------+---------+------------------------+
        || +LEAF3     |              1 |    9 |       2 |                    3/4 |
        |+------------+----------------+------+---------+------------------------+
        |""".stripMargin)
  }

  private val argument = plans.Argument()

  test("single node is represented nicely") {
    val arguments = Seq(
      details("details"),
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+---------+----------------+------+---------+
        || Operator | Details | Estimated Rows | Rows | DB Hits |
        |+----------+---------+----------------+------+---------+
        || +NAME    | details |              1 |   42 |      33 |
        |+----------+---------+----------------+------+---------+
        |""".stripMargin)
  }

  test("extra details are not a problem") {
    val arguments = Seq(
      details(Seq("a", "b")),
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderAsTreeTable(plan) should equal(
      """+----------+---------+----------------+------+---------+
        || Operator | Details | Estimated Rows | Rows | DB Hits |
        |+----------+---------+----------------+------+---------+
        || +NAME    | a, b    |              1 |   42 |      33 |
        |+----------+---------+----------------+------+---------+
        |""".stripMargin)
  }

  test("super many details stretches the column") {
    val arguments = Seq(
      details(Seq("aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff")),
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderAsTreeTable(plan) should equal(
      """+----------+------------------------------------------+----------------+------+---------+
        || Operator | Details                                  | Estimated Rows | Rows | DB Hits |
        |+----------+------------------------------------------+----------------+------+---------+
        || +NAME    | aaaaa, bbbbb, ccccc, ddddd, eeeee, fffff |              1 |   42 |      33 |
        |+----------+------------------------------------------+----------------+------+---------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats are not shown") {
    val arguments = Seq(EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+----------------+
        || Operator | Estimated Rows |
        |+----------+----------------+
        || +NAME    |              1 |
        |+----------+----------------+
        |""".stripMargin)
  }

  test("pipeline information is rendered in correct column") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1), Memory(5))
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), PipelineInfo(52, true), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = planDescription(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+-------------+----------------+------+---------+----------------+----------------------+
        || Operator | Details     | Estimated Rows | Rows | DB Hits | Memory (Bytes) | Other                |
        |+----------+-------------+----------------+------+---------+----------------+----------------------+
        || +NAME    | Index stuff |              1 |    2 |     633 |                | Fused in Pipeline 52 |
        || |        +-------------+----------------+------+---------+----------------+----------------------+
        || +NAME    |             |              1 |   42 |      33 |              5 |                      |
        |+----------+-------------+----------------+------+---------+----------------+----------------------+
        |""".stripMargin)
  }

  test("plan information is rendered on the corresponding row to the tree") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1), Memory(5))
    val args2 = Seq(Rows(2), DbHits(633), details("Index stuff"), EstimatedRows(1))

    val plan1 = planDescription(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = planDescription(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+-------------+----------------+------+---------+----------------+
        || Operator | Details     | Estimated Rows | Rows | DB Hits | Memory (Bytes) |
        |+----------+-------------+----------------+------+---------+----------------+
        || +NAME    | Index stuff |              1 |    2 |     633 |                |
        || |        +-------------+----------------+------+---------+----------------+
        || +NAME    |             |              1 |   42 |      33 |              5 |
        |+----------+-------------+----------------+------+---------+----------------+
        |""".stripMargin)
  }

  test("Anonymizes fresh ids in provided order") {
    val expandPlan = Expand(argument, "from", SemanticDirection.INCOMING, Seq.empty, "to", "rel", ExpandAll)
    val cardinalities = new Cardinalities
    val providedOrders = new ProvidedOrders
    providedOrders.set(expandPlan.id, ProvidedOrder.asc(varFor("  FRESHID42")))
    val description = LogicalPlan2PlanDescription(readOnly = true, cardinalities, providedOrders, StubExecutionPlan())

    renderAsTreeTable(description.create(expandPlan)) should equal(
      """+--------------+--------------------+-------------+
        || Operator     | Details            | Ordered by  |
        |+--------------+--------------------+-------------+
        || +Expand(All) | (from)<-[rel]-(to) | anon_42 ASC |
        |+--------------+--------------------+-------------+
        |""".stripMargin)
  }

  test("don't render planner in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"),
      details("`id`"),
      EstimatedRows(1))

    val plan = planDescription(id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+---------+----------------+------+---------+
        || Operator | Details | Estimated Rows | Rows | DB Hits |
        |+----------+---------+----------------+------+---------+
        || +NAME    | `id`    |              1 |   42 |      33 |
        |+----------+---------+----------------+------+---------+
        |""".stripMargin)
  }

  test("round estimated rows to int") {
    val planDescr1 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(0.00123456789)),
      Set.empty)
    val planDescr2 = planDescription(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        details("n:Foo"),
        EstimatedRows(1.23456789)),
      Set.empty)

    renderAsTreeTable(planDescr1) should equal(
      """+------------------+---------+----------------+
        || Operator         | Details | Estimated Rows |
        |+------------------+---------+----------------+
        || +NodeByLabelScan | n:Foo   |              0 |
        |+------------------+---------+----------------+
        |""".stripMargin )

    renderAsTreeTable(planDescr2) should equal(
      """+------------------+---------+----------------+
        || Operator         | Details | Estimated Rows |
        |+------------------+---------+----------------+
        || +NodeByLabelScan | n:Foo   |              1 |
        |+------------------+---------+----------------+
        |""".stripMargin )
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
      """+----------+
        || Operator |
        |+----------+
        || +NODE(2) |
        |+----------+
        |""".stripMargin)
  }

  test("compact two similar nodes with variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))

    renderAsTreeTable(plan) should equal(
      """+----------+
        || Operator |
        |+----------+
        || +NODE(2) |
        |+----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("c"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("d"))

    renderAsTreeTable(p3) should equal(
      """+--------------+
        || Operator     |
        |+--------------+
        || +OPERATOR(2) |
        || |            +
        || +NODE(2)     |
        |+--------------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with same variables") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b"))

    renderAsTreeTable(p3) should equal(
      """+--------------+
        || Operator     |
        |+--------------+
        || +OPERATOR(2) |
        || |            +
        || +NODE(2)     |
        |+--------------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with one new variable") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b", "c"))

    renderAsTreeTable(p3) should equal(
      """+--------------+
        || Operator     |
        |+--------------+
        || +OPERATOR(2) |
        || |            +
        || +NODE(2)     |
        |+--------------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")

    val leaf = planDescription(id, "NODE", NoChildren, Seq.empty, Set("var_a"))
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("var_a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, repeating + "var_A" + "var_B")

    renderAsTreeTable(p3) should equal(
      """+--------------+
        || Operator     |
        |+--------------+
        || +OPERATOR(2) |
        || |            +
        || +NODE(2)     |
        |+--------------+
        |""".stripMargin)
  }

  test("compact only the sufficiently similar pair of two similar pairs of nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")

    val leaf = planDescription(id, "NODE", NoChildren, Seq(details("var_a:123")), Set())
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq.empty, repeating + "var_A" + "var_B")
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("var_a"))

    renderAsTreeTable(p3) should equal(
      """+--------------+-----------+
        || Operator     | Details   |
        |+--------------+-----------+
        || +OPERATOR(2) |           |
        || |            +-----------+
        || +NODE        |           |
        || |            +-----------+
        || +NODE        | var_a:123 |
        |+--------------+-----------+
        |""".stripMargin)
  }

  test("compact only the sufficiently similar pair of two similar pairs of nodes with many repeating variables and many columns") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val l = details("var_a:123")
    val t = Time(12345678)
    val r = Rows(2)
    val d = DbHits(2)
    val leaf = planDescription(id, "NODE", NoChildren, Seq(l, t, r, d), Set())
    val p1 = planDescription(id, "NODE", SingleChild(leaf), Seq(t, r, d), repeating)
    val p2 = planDescription(id, "OPERATOR", SingleChild(p1), Seq(t, r, d), Set("var_a"))
    val p3 = planDescription(id, "OPERATOR", SingleChild(p2), Seq(t, r, d), repeating + "var_A" +
      "var_B")

    renderAsTreeTable(p3) should equal(
      """+--------------+-----------+------+---------+-----------+
        || Operator     | Details   | Rows | DB Hits | Time (ms) |
        |+--------------+-----------+------+---------+-----------+
        || +OPERATOR(2) |           |    2 |       4 |    24.691 |
        || |            +-----------+------+---------+-----------+
        || +NODE        |           |    2 |       2 |    12.346 |
        || |            +-----------+------+---------+-----------+
        || +NODE        | var_a:123 |    2 |       2 |    12.346 |
        |+--------------+-----------+------+---------+-----------+
        |""".stripMargin)
  }

  test("do not compact two similar pairs of nodes with non-empty detail column and many repeating variables and many columns") {
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
      """+-----------+-----------+------+---------+-----------+
        || Operator  | Details   | Rows | DB Hits | Time (ms) |
        |+-----------+-----------+------+---------+-----------+
        || +OPERATOR | var_a:123 |    2 |       2 |    12.346 |
        || |         +-----------+------+---------+-----------+
        || +OPERATOR | var_a:123 |    2 |       2 |    12.346 |
        || |         +-----------+------+---------+-----------+
        || +NODE     | var_a:123 |    2 |       2 |    12.346 |
        || |         +-----------+------+---------+-----------+
        || +NODE     | var_a:123 |    2 |       2 |    12.346 |
        |+-----------+-----------+------+---------+-----------+
        |""".stripMargin)
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
    val intermediate1 = planDescription(id, "INTERMEDIATE", TwoChildren(branch1, branch2), Seq.empty,
      Set())
    val intermediate2 = planDescription(id, "INTERMEDIATE", TwoChildren(branch3, branch4), Seq.empty,
      Set())
    val plan = planDescription(id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+
        || Operator        |
        |+-----------------+
        || +ROOT           |
        || |\              +
        || | +INTERMEDIATE |
        || | |\            +
        || | | +BRANCH     |
        || | | |           +
        || | | +LEAF       |
        || | |             +
        || | +BRANCH       |
        || | |             +
        || | +LEAF         |
        || |               +
        || +INTERMEDIATE   |
        || |\              +
        || | +BRANCH       |
        || | |             +
        || | +LEAF         |
        || |               +
        || +BRANCH         |
        || |               +
        || +LEAF           |
        |+-----------------+
        |""".stripMargin)
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
      """+--------------+
        || Operator     |
        |+--------------+
        || +NODE        |
        || |\           +
        || | +NODE      |
        || | |\         +
        || | | +NODE(2) |
        || | |          +
        || | +NODE(2)   |
        || |            +
        || +NODE        |
        || |\           +
        || | +NODE(2)   |
        || |            +
        || +NODE(2)     |
        |+--------------+
        |""".stripMargin)
  }

  test("should write long details on multiple lines") {
    val leaf = planDescription(id, "NODE", NoChildren, Seq(details((0 until 35).map(_.toString))), Set())
    val root = planDescription(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+---------------------------------------------------------------------------------------------------+
        || Operator | Details                                                                                           |
        |+----------+---------------------------------------------------------------------------------------------------+
        || +NODE    | 0, 1, 2, 3, 4                                                                                     |
        || |        +---------------------------------------------------------------------------------------------------+
        || +NODE    | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, |
        ||          | 27, 28, 29, 30, 31, 32, 33, 34                                                                    |
        |+----------+---------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("should split too long word in details rows on multiple lines") {
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(details(Seq((0 until 101).map(_ => "a").mkString(""), "b"))), Set())
    val root = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+------------------------------------------------------------------------------------------------------+
        || Operator | Details                                                                                              |
        |+----------+------------------------------------------------------------------------------------------------------+
        || +NODE    | 0, 1, 2, 3, 4                                                                                        |
        || |        +------------------------------------------------------------------------------------------------------+
        || +NODE    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        ||          | a, b                                                                                                 |
        |+----------+------------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("should add separator to next row if word exactly fits the row") {
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(details(Seq((0 until 100).map(_ => "a").mkString(""), "b"))), Set())
    val root = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(details((0 until 5).map(_.toString))), Set())

    renderAsTreeTable(root) should equal(
      """+----------+------------------------------------------------------------------------------------------------------+
        || Operator | Details                                                                                              |
        |+----------+------------------------------------------------------------------------------------------------------+
        || +NODE    | 0, 1, 2, 3, 4                                                                                        |
        || |        +------------------------------------------------------------------------------------------------------+
        || +NODE    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        ||          | , b                                                                                                  |
        |+----------+------------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("multiline details on table with child plans"){
    val leaf1 = PlanDescriptionImpl(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf3 = PlanDescriptionImpl(id, "LEAF3", NoChildren, Seq(details("c"*101)), Set())
    val leaf2 = PlanDescriptionImpl(id, "LEAF2", NoChildren, Seq(details("b"*101)), Set())
    val intermediate = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq(details("c"*101)), Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(leaf3, intermediate), Seq(details("a"*101)), Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+------------------------------------------------------------------------------------------------------+
        || Operator        | Details                                                                                              |
        |+-----------------+------------------------------------------------------------------------------------------------------+
        || +ROOT           | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
        || |               | a                                                                                                    |
        || |\              +------------------------------------------------------------------------------------------------------+
        || | +INTERMEDIATE | cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc |
        || | |             | c                                                                                                    |
        || | |\            +------------------------------------------------------------------------------------------------------+
        || | | +LEAF2      | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
        || | |             | b                                                                                                    |
        || | |             +------------------------------------------------------------------------------------------------------+
        || | +LEAF1        |                                                                                                      |
        || |               +------------------------------------------------------------------------------------------------------+
        || +LEAF3          | cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc |
        ||                 | c                                                                                                    |
        |+-----------------+------------------------------------------------------------------------------------------------------+
        |""".stripMargin)
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
    renderAsTreeTable.splitDetails(List("1234567890123456789", "1234567890123456789"), 10) should be(Seq("1234567890", "123456789,", "1234567890", "123456789"))
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
    renderAsTreeTable.splitDetails(List("1", "2", "3", "4", "5"), 5) should be(Seq("1, 2,",  "3, 4,",  "5"))
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
    renderAsTreeTable.splitDetails(List("123", "1234567890123456789"), 15) should be(Seq("123, 1234567890", "123456789"))
  }

  test("MultiNodeIndexSeek") {
    val logicalPlan = MultiNodeIndexSeek(Seq(IndexSeek("x:Label(Prop = 10,Foo = 1,Distance = 6,Name = 'Karoline Getinge')", unique = true).asInstanceOf[IndexSeekLeafPlan], IndexSeek("y:Label(Prop = 12, Name = 'Foo')").asInstanceOf[IndexSeekLeafPlan], IndexSeek("z:Label(Prop > 100, Name = 'Bar')").asInstanceOf[IndexSeekLeafPlan]))
    val cardinalities = new Cardinalities
    cardinalities.set(logicalPlan.id, 2.0)
    val plan = LogicalPlan2PlanDescription(logicalPlan, IDPPlannerName, CypherVersion.default, readOnly = true, cardinalities , new ProvidedOrders, StubExecutionPlan())

    renderAsTreeTable(plan) should equal(
      """+---------------------+------------------------------------------------------------------------------------------------------+----------------+
        || Operator            | Details                                                                                              | Estimated Rows |
        |+---------------------+------------------------------------------------------------------------------------------------------+----------------+
        || +MultiNodeIndexSeek | UNIQUE x:Label(Prop, Foo, Distance, Name) WHERE Prop = 10 AND Foo = 1 AND Distance = 6 AND Name = "K |              2 |
        ||                     | aroline Getinge", y:Label(Prop, Name) WHERE Prop = 12 AND Name = "Foo",                              |                |
        ||                     | z:Label(Prop, Name) WHERE Prop > 100 AND exists(Name)                                                |                |
        |+---------------------+------------------------------------------------------------------------------------------------------+----------------+
        |""".stripMargin)
  }
}
