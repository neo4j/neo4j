/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.planDescription

import java.util.Locale

import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.opencypher.v9_0.util.attribution.{Id, SequentialIdGen}
import org.opencypher.v9_0.util.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.opencypher.v9_0.util.{DummyPosition, LabelId, NonEmptyList, PropertyKeyId}
import org.opencypher.v9_0.expressions.{Expression => ASTExpression, LabelName => ASTLabelName, Range => ASTRange, _}
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.scalatest.BeforeAndAfterAll

class RenderTreeTableTest extends CypherFunSuite with BeforeAndAfterAll {
  implicit val windowsSafe = WindowsStringSafe
  implicit val idGen = new SequentialIdGen()

  private val defaultLocale = Locale.getDefault
  override def beforeAll() {
    //we change locale so we don't need to bother
    //with number format and such here
    Locale.setDefault(Locale.US)
  }

  override def afterAll(): Unit = {
    Locale.setDefault(defaultLocale)
  }

  private val pos = DummyPosition(0)
  private val id = Id.INVALID_ID

  test("node feeding from other node") {
    val leaf = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "ROOT", SingleChild(leaf), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(id, "LEAF2", NoChildren, Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())

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
    val leaf = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(id, "INTERMEDIATE", SingleChild(leaf), Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "ROOT", SingleChild(intermediate), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(id, "LEAF2", NoChildren, Seq.empty, Set())
    val leaf3 = PlanDescriptionImpl(id, "LEAF3", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(leaf3, intermediate), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("d"))
    val intermediate1 = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(leaf3, leaf4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(id, "LEAF1", NoChildren, Seq(
      Rows(42),
      DbHits(33),
      PageCacheHits(1),
      PageCacheMisses(2),
      PageCacheHitRatio(1.0/3),
      EstimatedRows(1)), Set())
    val leaf2 = PlanDescriptionImpl(id, "LEAF2", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      PageCacheHits(2),
      PageCacheMisses(3),
      PageCacheHitRatio(2.0/5),
      EstimatedRows(1)), Set())
    val leaf3 = PlanDescriptionImpl(id, "LEAF3", NoChildren, Seq(
      Rows(9),
      DbHits(2),
      PageCacheHits(3),
      PageCacheMisses(4),
      PageCacheHitRatio(3.0/7),
      EstimatedRows(1)), Set())
    val pass = PlanDescriptionImpl(id, "PASS", SingleChild(leaf2), Seq(
      Rows(4),
      DbHits(0),
      PageCacheHits(4),
      PageCacheMisses(1),
      PageCacheHitRatio(4.0/5),
      EstimatedRows(4)), Set())
    val inner = PlanDescriptionImpl(id, "INNER", TwoChildren(leaf1, pass), Seq(
      Rows(7),
      DbHits(42),
      PageCacheHits(5),
      PageCacheMisses(2),
      PageCacheHitRatio(5.0/7),
      EstimatedRows(6)), Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(leaf3, inner), Seq(
      Rows(3),
      DbHits(0),
      PageCacheHits(7),
      PageCacheMisses(10),
      PageCacheHitRatio(7.0/17),
      EstimatedRows(1)), Set())
    val parent = PlanDescriptionImpl(id, "PARENT", SingleChild(plan), Seq(), Set())

    renderAsTreeTable(parent) should equal(
      """+------------+----------------+------+---------+-----------------+-------------------+----------------------+
        || Operator   | Estimated Rows | Rows | DB Hits | Page Cache Hits | Page Cache Misses | Page Cache Hit Ratio |
        |+------------+----------------+------+---------+-----------------+-------------------+----------------------+
        || +PARENT    |                |      |         |                 |                   |                      |
        || |          +----------------+------+---------+-----------------+-------------------+----------------------+
        || +ROOT      |              1 |    3 |       0 |               7 |                10 |               0.4118 |
        || |\         +----------------+------+---------+-----------------+-------------------+----------------------+
        || | +INNER   |              6 |    7 |      42 |               5 |                 2 |               0.7143 |
        || | |\       +----------------+------+---------+-----------------+-------------------+----------------------+
        || | | +PASS  |              4 |    4 |       0 |               4 |                 1 |               0.8000 |
        || | | |      +----------------+------+---------+-----------------+-------------------+----------------------+
        || | | +LEAF2 |              1 |    9 |       2 |               2 |                 3 |               0.4000 |
        || | |        +----------------+------+---------+-----------------+-------------------+----------------------+
        || | +LEAF1   |              1 |   42 |      33 |               1 |                 2 |               0.3333 |
        || |          +----------------+------+---------+-----------------+-------------------+----------------------+
        || +LEAF3     |              1 |    9 |       2 |               3 |                 4 |               0.4286 |
        |+------------+----------------+------+---------+-----------------+-------------------+----------------------+
        |""".stripMargin)
  }

  private val argument = plans.Argument()

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))

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

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("a", "b", "c"))

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

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

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

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))

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
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", Seq("prop")), EstimatedRows(1))

    val plan1 = PlanDescriptionImpl(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+----------------+------+---------+-----------+--------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other        |
        |+----------+----------------+------+---------+-----------+--------------+
        || +NAME    |              1 |    2 |     633 | b         | :Label(prop) |
        || |        +----------------+------+---------+-----------+--------------+
        || +NAME    |              1 |   42 |      33 | a         |              |
        |+----------+----------------+------+---------+-----------+--------------+
        |""".stripMargin)
  }

  test("composite index rendered correctly") {
    val args1 = Seq(Rows(42), DbHits(33), EstimatedRows(1))
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", Seq("propA", "propB")), EstimatedRows(1))

    val plan1 = PlanDescriptionImpl(id, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(id, "NAME", SingleChild(plan1), args2, Set("b"))

    renderAsTreeTable(plan2) should equal(
      """+----------+----------------+------+---------+-----------+---------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other               |
        |+----------+----------------+------+---------+-----------+---------------------+
        || +NAME    |              1 |    2 |     633 | b         | :Label(propA,propB) |
        || |        +----------------+------+---------+-----------+---------------------+
        || +NAME    |              1 |   42 |      33 | a         |                     |
        |+----------+----------------+------+---------+-----------+---------------------+
        |""".stripMargin)
  }

  test("Expand contains information about its relations") {
    val expandPlan = Expand(argument, "from", SemanticDirection.INCOMING, Seq.empty, "to", "rel", ExpandAll)
    val cardinalities = new Cardinalities
    cardinalities.set(expandPlan.id, 1.0)
    cardinalities.set(argument.id, 1.0)
    val description = LogicalPlan2PlanDescription(true, cardinalities)

    renderAsTreeTable(description.create(expandPlan)) should equal(
      """+--------------+----------------+-----------+---------------------+
        || Operator     | Estimated Rows | Variables | Other               |
        |+--------------+----------------+-----------+---------------------+
        || +Expand(All) |              1 | rel, to   | (from)<-[rel:]-(to) |
        |+--------------+----------------+-----------+---------------------+
        |""".stripMargin)
  }

  test("Label scan should be just as pretty as you would expect") {
    val arguments = Seq(LabelName("Foo"), EstimatedRows(1))
    val pipe = PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, arguments, Set("n"))

    renderAsTreeTable(pipe) should equal(
      """+------------------+----------------+-----------+-------+
        || Operator         | Estimated Rows | Variables | Other |
        |+------------------+----------------+-----------+-------+
        || +NodeByLabelScan |              1 | n         | :Foo  |
        |+------------------+----------------+-----------+-------+
        |""".stripMargin )
  }

  test("Var length expand contains information about its relations") {
    val expandDescr = ExpandExpression("from", "rel", Seq.empty, "to", SemanticDirection.INCOMING, 0, None)
    val estimatedRows = EstimatedRows(1)
    val arguments = Seq(estimatedRows, expandDescr)
    val variables = Set("rel", "to")
    val planDescription = PlanDescriptionImpl(id, "VarLengthExpand(All)", NoChildren, arguments, variables)
    renderAsTreeTable(planDescription) should equal(
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

    val variables = Set("n", "  UNNAMED123", "  FRESHID12", "  AGGREGATION255")
    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, variables)
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

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))
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
      Expression(Not(Equals(Variable("  UNNAMED123")(pos), Variable("  UNNAMED321")(pos))(pos))(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-------------------------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other                         |
        |+----------+----------------+------+---------+-----------+-------------------------------+
        || +NAME    |              1 |   42 |      33 | n         | not `anon[123]` = `anon[321]` |
        |+----------+----------------+------+---------+-----------+-------------------------------+
        |""".stripMargin)
  }

  test("show hasLabels nicely without token id") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Expression(HasLabels(Variable("x")(pos), Seq(ASTLabelName("Artist")(pos)))(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))
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
      Expression(FunctionInvocation(FunctionName("length")(pos), Variable("n")(pos))(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other     |
        |+----------+----------------+------+---------+-----------+-----------+
        || +NAME    |              1 |   42 |      33 | n         | length(n) |
        |+----------+----------------+------+---------+-----------+-----------+
        |""".stripMargin)
  }

  test("format index range seek properly") {
    val rangeQuery = RangeQueryExpression(InequalitySeekRangeWrapper(
      RangeLessThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("12")(pos))))
    )(pos))
    val seekPlan = NodeIndexSeek(
      "a",
      LabelToken("Person", LabelId(0)),
      Seq(PropertyKeyToken(PropertyKeyName("age")(pos), PropertyKeyId(0))),
      rangeQuery,
      Set.empty)(idGen)
    val cardinalities = new Cardinalities
    cardinalities.set(seekPlan.id, 1.0)
    cardinalities.set(argument.id, 1.0)
    val description = LogicalPlan2PlanDescription(readOnly = true, cardinalities)

    renderAsTreeTable(description.create(seekPlan)) should equal(
      """+-----------------------+----------------+-----------+-------------------+
        || Operator              | Estimated Rows | Variables | Other             |
        |+-----------------------+----------------+-----------+-------------------+
        || +NodeIndexSeekByRange |              1 | a         | :Person(age) < 12 |
        |+-----------------------+----------------+-----------+-------------------+
        |""".stripMargin)
  }

  test("format index range seek by bounds") {
    val greaterThan = RangeGreaterThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("12")(pos))))
    val lessThan = RangeLessThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("21")(pos))))
    val between = RangeBetween(greaterThan, lessThan)
    val rangeQuery = RangeQueryExpression(InequalitySeekRangeWrapper(between)(pos))
    val seekPlan = NodeIndexSeek(
      "a",
      LabelToken("Person", LabelId(0)),
      Seq(PropertyKeyToken(PropertyKeyName("age")(pos), PropertyKeyId(0))),
      rangeQuery,
      Set.empty)(idGen)
    val cardinalities = new Cardinalities
    cardinalities.set(seekPlan.id, 1.0)
    cardinalities.set(argument.id, 1.0)
    val description = LogicalPlan2PlanDescription(readOnly = true, cardinalities)

    renderAsTreeTable(description.create(seekPlan)) should equal(
      """+-----------------------+----------------+-----------+-----------------------------------------+
        || Operator              | Estimated Rows | Variables | Other                                   |
        |+-----------------------+----------------+-----------+-----------------------------------------+
        || +NodeIndexSeekByRange |              1 | a         | :Person(age) > 12 AND :Person(age) < 21 |
        |+-----------------------+----------------+-----------+-----------------------------------------+
        |""".stripMargin)
  }

  test("don't leak deduped names") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Expression(Variable("  id@23")(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | `id`  |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

  test("don't render planner in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"),
      Expression(Variable("  id@23")(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set("n"))
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | `id`  |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

  test("round estimated rows to int") {
    val planDescr1 = PlanDescriptionImpl(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        LabelName("Foo"),
        EstimatedRows(0.00123456789)),
      Set("n"))
    val planDescr2 = PlanDescriptionImpl(
      id,
      "NodeByLabelScan",
      NoChildren,
      Seq(
        LabelName("Foo"),
        EstimatedRows(1.23456789)),
      Set("n"))

    renderAsTreeTable(planDescr1) should equal(
      """+------------------+----------------+-----------+-------+
        || Operator         | Estimated Rows | Variables | Other |
        |+------------------+----------------+-----------+-------+
        || +NodeByLabelScan |              0 | n         | :Foo  |
        |+------------------+----------------+-----------+-------+
        |""".stripMargin )

    renderAsTreeTable(planDescr2) should equal(
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
      Expression( Property(Variable( "x" )(pos), PropertyKeyName("Artist")(pos))(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl( id, "NAME", NoChildren, arguments, Set( "n") )
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+----------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other    |
        |+----------+----------------+------+---------+-----------+----------+
        || +NAME    |              1 |   42 |      33 | n         | x.Artist |
        |+----------+----------------+------+---------+-----------+----------+
        |""".stripMargin )
  }

  test("show exists with variable and property") {
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ),
      Expression( FunctionInvocation(FunctionName("exists")(pos), Property(Variable("x")(pos), PropertyKeyName("prop")(pos))(pos))(pos)),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl( id, "NAME", NoChildren, arguments, Set( "n") )
    renderAsTreeTable(plan) should equal(
      """+----------+----------------+------+---------+-----------+----------------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other          |
        |+----------+----------------+------+---------+-----------+----------------+
        || +NAME    |              1 |   42 |      33 | n         | exists(x.prop) |
        |+----------+----------------+------+---------+-----------+----------------+
        |""".stripMargin )
  }

  test("don't show unnamed variables in key names") {
    val sr1 = PlanDescriptionImpl(id, "EmptyRow", NoChildren, Seq(EstimatedRows(1)), Set.empty)
    val sr2 = PlanDescriptionImpl(id, "EmptyRow", NoChildren, Seq(EstimatedRows(1)), Set.empty)
    val description = PlanDescriptionImpl(id, "NodeHashJoin", TwoChildren(sr1, sr2), Seq(EstimatedRows
    (42)), Set("a",
      "  UNNAMED45", "  FRESHID77"))

    renderAsTreeTable(description) should equal(
      """+---------------+----------------+-----------------------+
        || Operator      | Estimated Rows | Variables             |
        |+---------------+----------------+-----------------------+
        || +NodeHashJoin |             42 | anon[77], anon[45], a |
        || |\            +----------------+-----------------------+
        || | +EmptyRow   |              1 |                       |
        || |             +----------------+-----------------------+
        || +EmptyRow     |              1 |                       |
        |+---------------+----------------+-----------------------+
        |""".stripMargin)
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
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, Set())

    renderAsTreeTable(plan) should equal(
      """+----------+
        || Operator |
        |+----------+
        || +NODE(2) |
        |+----------+
        |""".stripMargin)
  }

  test("compact two similar nodes with variables") {
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val plan = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))

    renderAsTreeTable(plan) should equal(
      """+----------+-----------+
        || Operator | Variables |
        |+----------+-----------+
        || +NODE(2) | a, b      |
        |+----------+-----------+
        |""".stripMargin)
  }

  test("compact two pairs of similar nodes with variables") {
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("c"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("d"))

    renderAsTreeTable(p3) should equal(
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
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b"))

    renderAsTreeTable(p3) should equal(
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
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set("a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, Set("b"))
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("a"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("b", "c"))

    renderAsTreeTable(p3) should equal(
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

    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq.empty, Set("var_a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq.empty, Set("var_a"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq.empty, repeating + "var_A" + "var_B")

    renderAsTreeTable(p3) should equal(
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

    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(LabelName("123")), Set("var_a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq.empty, repeating)
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq.empty, repeating + "var_A" + "var_B")
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq.empty, Set("var_a"))

    renderAsTreeTable(p3) should equal(
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
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(l, t, r, d), Set("var_a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(t, r, d), repeating)
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq(t, r, d), Set("var_a"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq(t, r, d), repeating + "var_A" +
      "var_B")

    renderAsTreeTable(p3) should equal(
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
    val leaf = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(l, t, r, d), Set("var_a"))
    val p1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf), Seq(l, t, r, d), repeating)
    val p2 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p1), Seq(l, t, r, d), Set("var_a"))
    val p3 = PlanDescriptionImpl(id, "OPERATOR", SingleChild(p2), Seq(l, t, r, d), repeating + "var_A" +
      "var_B")

    renderAsTreeTable(p3) should equal(
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
    val leaf1 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = PlanDescriptionImpl(id, "LEAF", NoChildren, Seq(), Set("d"))
    val branch1 = PlanDescriptionImpl(id, "BRANCH", SingleChild(leaf1), Seq.empty, Set("a"))
    val branch2 = PlanDescriptionImpl(id, "BRANCH", SingleChild(leaf2), Seq.empty, Set("b"))
    val branch3 = PlanDescriptionImpl(id, "BRANCH", SingleChild(leaf3), Seq.empty, Set("c"))
    val branch4 = PlanDescriptionImpl(id, "BRANCH", SingleChild(leaf4), Seq.empty, Set("d"))
    val intermediate1 = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(branch1, branch2), Seq.empty,
      Set())
    val intermediate2 = PlanDescriptionImpl(id, "INTERMEDIATE", TwoChildren(branch3, branch4), Seq.empty,
      Set())
    val plan = PlanDescriptionImpl(id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

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
    val leaf1 = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(), Set())
    val leaf2 = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(), Set())
    val leaf3 = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(), Set())
    val leaf4 = PlanDescriptionImpl(id, "NODE", NoChildren, Seq(), Set())
    val branch1 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf1), Seq.empty, Set())
    val branch2 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf2), Seq.empty, Set())
    val branch3 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf3), Seq.empty, Set())
    val branch4 = PlanDescriptionImpl(id, "NODE", SingleChild(leaf4), Seq.empty, Set())
    val intermediate1 = PlanDescriptionImpl(id, "NODE", TwoChildren(branch1, branch2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(id, "NODE", TwoChildren(branch3, branch4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(id, "NODE", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())

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
    val compacted = CompactedLine(line, Set.empty)
    compacted.formattedVariables should be("")
  }

  test("Variable line compaction with only new variables") {
    val line = Line("NODE", Map.empty, Set("a", "b"))
    val compacted = CompactedLine(line, Set.empty)
    compacted.formattedVariables should be("a, b")
  }

  test("Variable line compaction with only old variables") {
    val line = Line("NODE", Map.empty, Set("a", "b"))
    val compacted = CompactedLine(line, Set("a", "b"))
    compacted.formattedVariables should be("a, b")
  }

  test("Variable line compaction with old and new variables") {
    val line = Line("NODE", Map.empty, Set("a", "b", "c", "d"))
    val compacted = CompactedLine(line, Set("a", "b"))
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
