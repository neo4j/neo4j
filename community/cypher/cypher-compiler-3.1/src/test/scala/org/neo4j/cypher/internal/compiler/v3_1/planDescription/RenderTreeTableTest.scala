/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planDescription

import java.util.Locale

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{LengthFunction, Property, Variable}
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.{Equals, HasLabel, Not, PropertyExists}
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v3_1.pipes._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.scalatest.BeforeAndAfterAll

class RenderTreeTableTest extends CypherFunSuite with BeforeAndAfterAll {

  private val defaultLocale = Locale.getDefault
  override def beforeAll() {
    //we change locale so we don't need to bother
    //with number format and such here
    Locale.setDefault(Locale.US)
  }
  override def afterAll() = {
    Locale.setDefault(defaultLocale)
  }

  test("node feeding from other node") {
    val leaf = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq.empty, Set())
    val plan = leaf.andThen(new Id, "ROOT", Set())

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
    val leaf1 = PlanDescriptionImpl(new Id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF2", NoChildren, Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())

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
    val leaf = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(new Id, "INTERMEDIATE", SingleChild(leaf), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", SingleChild(intermediate), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(new Id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF2", NoChildren, Seq.empty, Set())
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF3", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(leaf3, intermediate), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("d"))
    val intermediate1 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf3, leaf4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+-----------+
        || Operator        | Variables |
        |+-----------------+-----------+
        || +ROOT           |           |
        || |\              +-----------+
        || | +INTERMEDIATE |           |
        || | |\            +-----------+
        || | | +LEAF       | d         |
        || | |             +-----------+
        || | +LEAF         | c         |
        || |               +-----------+
        || +INTERMEDIATE   |           |
        || |\              +-----------+
        || | +LEAF         | b         |
        || |               +-----------+
        || +LEAF           | a         |
        |+-----------------+-----------+
        |""".stripMargin)
  }

  test("complex tree") {
    val leaf1 = PlanDescriptionImpl(new Id, "LEAF1", NoChildren, Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1)), Set())
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF2", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      EstimatedRows(1)), Set())
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF3", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      EstimatedRows(1)), Set())
    val pass = PlanDescriptionImpl(new Id, "PASS", SingleChild(leaf2), Seq(
      Rows(4),
      DbHits(0),
      EstimatedRows(4)), Set())
    val inner = PlanDescriptionImpl(new Id, "INNER", TwoChildren(leaf1, pass), Seq(
      Rows(7),
      DbHits(42),
      EstimatedRows(6)), Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(leaf3, inner), Seq(
      Rows(3),
      DbHits(0),
      EstimatedRows(1)), Set())
    val parent = PlanDescriptionImpl(new Id, "PARENT", SingleChild(plan), Seq(), Set())

    renderAsTreeTable(parent) should equal(
      """+------------+----------------+------+---------+
        || Operator   | Estimated Rows | Rows | DB Hits |
        |+------------+----------------+------+---------+
        || +PARENT    |                |      |         |
        || |          +----------------+------+---------+
        || +ROOT      |              1 |    3 |       0 |
        || |\         +----------------+------+---------+
        || | +INNER   |              6 |    7 |      42 |
        || | |\       +----------------+------+---------+
        || | | +PASS  |              4 |    4 |       0 |
        || | | |      +----------------+------+---------+
        || | | +LEAF2 |              1 |    9 |       2 |
        || | |        +----------------+------+---------+
        || | +LEAF1   |              1 |   42 |      33 |
        || |          +----------------+------+---------+
        || +LEAF3     |              1 |    9 |       2 |
        |+------------+----------------+------+---------+
        |""".stripMargin)
  }


  val pipe = SingleRowPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables |
        |+----------+----------------+------+---------+-----------+
        || +NAME    |              1 |   42 |      33 | n         |
        |+----------+----------------+------+---------+-----------+
        |""".stripMargin)
  }

  test("extra variables are not a problem") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables |
        |+----------+----------------+------+---------+-----------+
        || +NAME    |              1 |   42 |      33 | a, b, c   |
        |+----------+----------------+------+---------+-----------+
        |""".stripMargin)
  }

  test("super many variables stretches the column") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables        |
        |+----------+----------------+------+---------+------------------+
        || +NAME    |              1 |   42 |      33 | a, b, c, d, e, f |
        |+----------+----------------+------+---------+------------------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats are not shown") {
    val arguments = Seq(EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))

    renderAsTreeTable(plan) should equal(
      """+----------+----------------+-----------+
        || Operator | Estimated Rows | Variables |
        |+----------+----------------+-----------+
        || +NAME    |              1 | n         |
        |+----------+----------------+-----------+
        |""".stripMargin)
  }

  test("plan information is rendered on the corresponding row to the tree") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1))
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", "Prop"), EstimatedRows(1))

    val plan1 = PlanDescriptionImpl(new Id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(new Id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+----------------+------+---------+-----------+--------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other        |
        |+----------+----------------+------+---------+-----------+--------------+
        || +NAME    |              1 |    2 |     633 | b         | :Label(Prop) |
        || |        +----------------+------+---------+-----------+--------------+
        || +NAME    |              1 |   42 |      33 | a         |              |
        |+----------+----------------+------+---------+-----------+--------------+
        |""".stripMargin)
  }


  test("Expand contains information about its relations") {
    val expandPipe = ExpandAllPipe(pipe, "from", "rel", "to", SemanticDirection.INCOMING, LazyTypes.empty)(Some(1L))(mock[PipeMonitor])

    renderAsTreeTable(expandPipe.planDescription) should equal(
      """+--------------+----------------+-----------+---------------------+
        || Operator     | Estimated Rows | Variables | Other               |
        |+--------------+----------------+-----------+---------------------+
        || +Expand(All) |              1 | rel, to   | (from)<-[rel:]-(to) |
        |+--------------+----------------+-----------+---------------------+
        |""".stripMargin)
  }

  test("Label scan should be just as pretty as you would expect") {
    val pipe = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(1L))(mock[PipeMonitor])

    renderAsTreeTable(pipe.planDescription) should equal(
      """+------------------+----------------+-----------+-------+
        || Operator         | Estimated Rows | Variables | Other |
        |+------------------+----------------+-----------+-------+
        || +NodeByLabelScan |              1 | n         | :Foo  |
        |+------------------+----------------+-----------+-------+
        |""".stripMargin )
  }

  test("Var length expand contains information about its relations") {
    val expandPipe = VarLengthExpandPipe(pipe, "from", "rel", "to", SemanticDirection.INCOMING, SemanticDirection.OUTGOING, LazyTypes.empty, 0, None, nodeInScope = false)(Some(1L))(mock[PipeMonitor])

    renderAsTreeTable(expandPipe.planDescription) should equal(
      """+-----------------------+----------------+-----------+-------------------------+
        || Operator              | Estimated Rows | Variables | Other                   |
        |+-----------------------+----------------+-----------+-------------------------+
        || +VarLengthExpand(All) |              1 | rel, to   | (from)<-[rel:*0..]-(to) |
        |+-----------------------+----------------+-----------+-------------------------+
        |""".stripMargin)
  }

  test("do not show unnamed variables") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("  UNNAMED123", "R", Seq("WHOOP"), "  UNNAMED24", SemanticDirection.OUTGOING, 1, Some(1)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  FRESHID12", "  AGGREGATION255"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------------------------------+------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables                         | Other            |
        |+----------+----------------+------+---------+-----------------------------------+------------------+
        || +NAME    |              1 |   42 |      33 | anon[255], anon[12], anon[123], n | ()-[R:WHOOP]->() |
        |+----------+----------------+------+---------+-----------------------------------+------------------+
        |""".stripMargin)
  }

  test("show multiple relationship types") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("source", "through", Seq("SOME","OTHER","THING"), "target", SemanticDirection.OUTGOING, 1, Some(1)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-------------------------------------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other                                           |
        |+----------+----------------+------+---------+-----------+-------------------------------------------------+
        || +NAME    |              1 |   42 |      33 | n         | (source)-[through:SOME|:OTHER|:THING]->(target) |
        |+----------+----------------+------+---------+-----------+-------------------------------------------------+
        |""".stripMargin)
  }

  test("show nicer output instead of unnamed variables in equals expression") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Not(Equals(Variable("  UNNAMED123"), Variable("  UNNAMED321")))),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-----------------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other                       |
        |+----------+----------------+------+---------+-----------+-----------------------------+
        || +NAME    |              1 |   42 |      33 | n         | NOT(anon[123] == anon[321]) |
        |+----------+----------------+------+---------+-----------+-----------------------------+
        |""".stripMargin)
  }

  test("show hasLabels nicely without token id") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(HasLabel(Variable("x"), KeyToken.Resolved("Artist", 5, TokenType.Label))),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other    |
        |+----------+----------------+------+---------+-----------+----------+
        || +NAME    |              1 |   42 |      33 | n         | x:Artist |
        |+----------+----------------+------+---------+-----------+----------+
        |""".stripMargin)
  }

  test("format length properly") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(LengthFunction(Variable("n"))),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other     |
        |+----------+----------------+------+---------+-----------+-----------+
        || +NAME    |              1 |   42 |      33 | n         | length(n) |
        |+----------+----------------+------+---------+-----------+-----------+
        |""".stripMargin)
  }

  test("don't leak deduped names") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Variable("  id@23")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | id    |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

  test("don't render planner in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"),
      LegacyExpression(Variable("  id@23")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | id    |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

  test("round estimated rows to int") {
    val pipe1 = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(0.00123456789))(mock[PipeMonitor])
    val pipe2 = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(1.23456789))(mock[PipeMonitor])

    renderAsTreeTable(pipe1.planDescription) should equal(
      """+------------------+----------------+-----------+-------+
        || Operator         | Estimated Rows | Variables | Other |
        |+------------------+----------------+-----------+-------+
        || +NodeByLabelScan |              0 | n         | :Foo  |
        |+------------------+----------------+-----------+-------+
        |""".stripMargin )

    renderAsTreeTable(pipe2.planDescription ) should equal(
      """+------------------+----------------+-----------+-------+
        || Operator         | Estimated Rows | Variables | Other |
        |+------------------+----------------+-----------+-------+
        || +NodeByLabelScan |              1 | n         | :Foo  |
        |+------------------+----------------+-----------+-------+
        |""".stripMargin )
  }

  test("properly show Property") {
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ),
      LegacyExpression( Property(Variable( "x" ), KeyToken.Resolved( "Artist", 5, TokenType.PropertyKey ))),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl( new Id, "NAME", NoChildren, arguments, Set( "n") )
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other    |
        |+----------+----------------+------+---------+-----------+----------+
        || +NAME    |              1 |   42 |      33 | n         | x.Artist |
        |+----------+----------------+------+---------+-----------+----------+
        |""".stripMargin )
  }

  test("show hasProp with variable and property") {
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ),
      LegacyExpression( PropertyExists(Variable("x"), KeyToken.Resolved("prop", 42, TokenType.PropertyKey))),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl( new Id, "NAME", NoChildren, arguments, Set( "n") )
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-----------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other           |
        |+----------+----------------+------+---------+-----------+-----------------+
        || +NAME    |              1 |   42 |      33 | n         | hasProp(x.prop) |
        |+----------+----------------+------+---------+-----------+-----------------+
        |""".stripMargin )
  }

  test("don't show unnamed variables in key names") {
    val joinPipe = NodeHashJoinPipe(Set("a", "  UNNAMED45", "  FRESHID77"), pipe, pipe)(Some(42))(mock[PipeMonitor])

    renderAsTreeTable(joinPipe.planDescription) should equal(
      """+---------------+----------------+-----------------------+
        || Operator      | Estimated Rows | Other                 |
        |+---------------+----------------+-----------------------+
        || +NodeHashJoin |             42 | a, anon[45], anon[77] |
        || |\            +----------------+-----------------------+
        || | +EmptyRow   |              1 |                       |
        || |             +----------------+-----------------------+
        || +EmptyRow     |              1 |                       |
        |+---------------+----------------+-----------------------+
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
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set())
    val plan = leaf.andThen(new Id, "NODE", Set())

    renderAsTreeTable(plan) should equal(
      """+----------+
        || Operator |
        |+----------+
        || +NODE(2) |
        |+----------+
        |""".stripMargin)
  }

  test("compact two similar nodes with variables") {
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = leaf.andThen(new Id, "NODE", Set("b"))

    renderAsTreeTable(plan) should equal(
      """+----------+-----------+
        || Operator | Variables |
        |+----------+-----------+
        || +NODE(2) | a, b      |
        |+----------+-----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with variables") {
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = leaf.andThen(new Id, "NODE", Set("b")).andThen(new Id, "OPERATOR", Set("c")).andThen(new Id,
      "OPERATOR", Set("d"))

    renderAsTreeTable(plan) should equal(
      """+--------------+-----------+
        || Operator     | Variables |
        |+--------------+-----------+
        || +OPERATOR(2) | c, d      |
        || |            +-----------+
        || +NODE(2)     | a, b      |
        |+--------------+-----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with same variables") {
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = leaf.andThen(new Id, "NODE", Set("b")).andThen(new Id, "OPERATOR", Set("a")).andThen(new Id,
      "OPERATOR", Set("b"))

    renderAsTreeTable(plan) should equal(
      """+--------------+-----------+
        || Operator     | Variables |
        |+--------------+-----------+
        || +OPERATOR(2) | a, b      |
        || |            +-----------+
        || +NODE(2)     | a, b      |
        |+--------------+-----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with one new variable") {
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = leaf.andThen(new Id, "NODE", Set("b")).andThen(new Id, "OPERATOR", Set("a")).andThen(new Id,
      "OPERATOR", Set("b", "c"))

    renderAsTreeTable(plan) should equal(
      """+--------------+-----------+
        || Operator     | Variables |
        |+--------------+-----------+
        || +OPERATOR(2) | c -- a, b |
        || |            +-----------+
        || +NODE(2)     | a, b      |
        |+--------------+-----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq.empty, Set("var_a"))
    val plan = leaf.andThen(new Id, "NODE", repeating).andThen(new Id, "OPERATOR", Set("var_a"))
      .andThen(new Id, "OPERATOR", repeating + "var_A" + "var_B")

    renderAsTreeTable(plan) should equal(
      """+--------------+--------------------------------------------------------------------------------------------------+
        || Operator     | Variables                                                                                        |
        |+--------------+--------------------------------------------------------------------------------------------------+
        || +OPERATOR(2) | var_A, var_B -- var_a, var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, ... |
        || |            +--------------------------------------------------------------------------------------------------+
        || +NODE(2)     | var_a, var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, var_l, var_m, ...   |
        |+--------------+--------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("compact only the sufficiently similar pair of two simular pairs of nodes with many repeating variables") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(LabelName("123")), Set("var_a"))
    val plan = leaf.andThen(new Id, "NODE", repeating).andThen(new Id, "OPERATOR", Set("var_a"))
      .andThen(new Id, "OPERATOR", repeating + "var_A" + "var_B")

    renderAsTreeTable(plan) should equal(
      """+--------------+--------------------------------------------------------------------------------------------------+-------+
        || Operator     | Variables                                                                                        | Other |
        |+--------------+--------------------------------------------------------------------------------------------------+-------+
        || +OPERATOR(2) | var_A, var_B, var_a -- var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, ... |       |
        || |            +--------------------------------------------------------------------------------------------------+-------+
        || +NODE        | var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, var_l, var_m, var_n, ...   |       |
        || |            +--------------------------------------------------------------------------------------------------+-------+
        || +NODE        | var_a                                                                                            | :123  |
        |+--------------+--------------------------------------------------------------------------------------------------+-------+
        |""".stripMargin)
  }

  test("compact only the sufficiently similar pair of two simular pairs of nodes with many repeating variables and many columns") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val l = LabelName("123")
    val t = Time(12345678)
    val r = Rows(2)
    val d = DbHits(2)
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(l, t, r, d), Set("var_a"))
    val plan = leaf.
      andThen(new Id, "NODE", repeating, t, r, d).
      andThen(new Id, "OPERATOR", Set("var_a"), t, r, d).
      andThen(new Id, "OPERATOR", repeating + "var_A" + "var_B", t, r, d)

    renderAsTreeTable(plan) should equal(
      """+--------------+------+---------+-----------+--------------------------------------------------------------------------------------------------+-------+
        || Operator     | Rows | DB Hits | Time (ms) | Variables                                                                                        | Other |
        |+--------------+------+---------+-----------+--------------------------------------------------------------------------------------------------+-------+
        || +OPERATOR(2) |    2 |       4 |    24.691 | var_A, var_B, var_a -- var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, ... |       |
        || |            +------+---------+-----------+--------------------------------------------------------------------------------------------------+-------+
        || +NODE        |    2 |       2 |    12.346 | var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, var_l, var_m, var_n, ...   |       |
        || |            +------+---------+-----------+--------------------------------------------------------------------------------------------------+-------+
        || +NODE        |    2 |       2 |    12.346 | var_a                                                                                            | :123  |
        |+--------------+------+---------+-----------+--------------------------------------------------------------------------------------------------+-------+
        |""".stripMargin)
  }

  test("do not compact two similar pairs of nodes with non-empty other column and many repeating variables and many columns") {
    val repeating = ('b' to 'z').toSet[Char].map(c => s"var_$c")
    val l = LabelName("123")
    val t = Time(12345678)
    val r = Rows(2)
    val d = DbHits(2)
    val leaf = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(l, t, r, d), Set("var_a"))
    val plan = leaf.
      andThen(new Id, "NODE", repeating, l, t, r, d).
      andThen(new Id, "OPERATOR", Set("var_a"), l, t, r, d).
      andThen(new Id, "OPERATOR", repeating + "var_A" + "var_B", l, t, r, d)

    renderAsTreeTable(plan) should equal(
      """+-----------+------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        || Operator  | Rows | DB Hits | Time (ms) | Variables                                                                                      | Other |
        |+-----------+------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        || +OPERATOR |    2 |       2 |    12.346 | var_A, var_B, var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, var_l, ... | :123  |
        || |         +------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        || +OPERATOR |    2 |       2 |    12.346 | var_a                                                                                          | :123  |
        || |         +------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        || +NODE     |    2 |       2 |    12.346 | var_b, var_c, var_d, var_e, var_f, var_g, var_h, var_i, var_j, var_k, var_l, var_m, var_n, ... | :123  |
        || |         +------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        || +NODE     |    2 |       2 |    12.346 | var_a                                                                                          | :123  |
        |+-----------+------+---------+-----------+------------------------------------------------------------------------------------------------+-------+
        |""".stripMargin)
  }

  test("no compaction on complex plan with no repeated names") {
    val leaf1 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("d"))
    val branch1 = PlanDescriptionImpl(new Id, "BRANCH", SingleChild(leaf1), Seq.empty, Set("a"))
    val branch2 = PlanDescriptionImpl(new Id, "BRANCH", SingleChild(leaf2), Seq.empty, Set("b"))
    val branch3 = PlanDescriptionImpl(new Id, "BRANCH", SingleChild(leaf3), Seq.empty, Set("c"))
    val branch4 = PlanDescriptionImpl(new Id, "BRANCH", SingleChild(leaf4), Seq.empty, Set("d"))
    val intermediate1 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(branch1, branch2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(branch3, branch4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+-----------------+-----------+
        || Operator        | Variables |
        |+-----------------+-----------+
        || +ROOT           |           |
        || |\              +-----------+
        || | +INTERMEDIATE |           |
        || | |\            +-----------+
        || | | +BRANCH     | d         |
        || | | |           +-----------+
        || | | +LEAF       | d         |
        || | |             +-----------+
        || | +BRANCH       | c         |
        || | |             +-----------+
        || | +LEAF         | c         |
        || |               +-----------+
        || +INTERMEDIATE   |           |
        || |\              +-----------+
        || | +BRANCH       | b         |
        || | |             +-----------+
        || | +LEAF         | b         |
        || |               +-----------+
        || +BRANCH         | a         |
        || |               +-----------+
        || +LEAF           | a         |
        |+-----------------+-----------+
        |""".stripMargin)
  }

  test("compaction on complex plan with repeated names") {
    val leaf1 = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(), Set())
    val leaf2 = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(), Set())
    val leaf3 = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(), Set())
    val leaf4 = PlanDescriptionImpl(new Id, "NODE", NoChildren, Seq(), Set())
    val branch1 = PlanDescriptionImpl(new Id, "NODE", SingleChild(leaf1), Seq.empty, Set())
    val branch2 = PlanDescriptionImpl(new Id, "NODE", SingleChild(leaf2), Seq.empty, Set())
    val branch3 = PlanDescriptionImpl(new Id, "NODE", SingleChild(leaf3), Seq.empty, Set())
    val branch4 = PlanDescriptionImpl(new Id, "NODE", SingleChild(leaf4), Seq.empty, Set())
    val intermediate1 = PlanDescriptionImpl(new Id, "NODE", TwoChildren(branch1, branch2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(new Id, "NODE", TwoChildren(branch3, branch4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "NODE", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

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

  test("Variable line compaction with no variables") {
    val line = Line("NODE", Map.empty, Set.empty)
    val compacted = new CompactedLine(line, Set.empty)
    compacted.formattedVariables should be("")
  }

  test("Variable line compaction with only new variables") {
    val line = Line("NODE", Map.empty, Set("a", "b"))
    val compacted = new CompactedLine(line, Set.empty)
    compacted.formattedVariables should be("a, b")
  }

  test("Variable line compaction with only old variables") {
    val line = Line("NODE", Map.empty, Set("a", "b"))
    val compacted = new CompactedLine(line, Set("a", "b"))
    compacted.formattedVariables should be("a, b")
  }

  test("Variable line compaction with old and new variables") {
    val line = Line("NODE", Map.empty, Set("a", "b", "c", "d"))
    val compacted = new CompactedLine(line, Set("a", "b"))
    compacted.formattedVariables should be("c, d -- a, b")
  }

  test("Variable line compaction with many old and new variables and all compaction lengths") {
    val newvars = ('a' to 'e').toSet[Char].map(c => s"var_$c")
    val repeating = ('f' to 'z').toSet[Char].map(c => s"var_$c")
    val line = Line("NODE", Map.empty, repeating ++ newvars, None)
    val compacted = CompactedLine(line, repeating)
    val maxlen = compacted.formatVariables(1000).length
    val mintext = "var_a, ..."
    Range(maxlen, 1, -1).foreach { length =>
      val formatted = compacted.formatVariables(length)
      if (formatted.length < maxlen)
        formatted should endWith("...")
      else
        formatted should endWith("var_z")
      if (length < mintext.length) {
        formatted should be(mintext)
      } else {
        withClue(s"Expected formatted length to be no greater than $length.") {
          formatted.length should be <= length
        }
      }
    }
  }

}
