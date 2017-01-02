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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import java.util.Locale

import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Property, Identifier, LengthFunction}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.graphdb.Direction
import org.scalatest.BeforeAndAfterAll

class RenderPlanDescriptionDetailsTest extends CypherFunSuite with BeforeAndAfterAll {

  private val defaultLocale = Locale.getDefault
  override def beforeAll() {
    //we change locale so we don't need to bother
    //with number format and such here
    Locale.setDefault(Locale.US)
  }

  override def afterAll() = {
    Locale.setDefault(defaultLocale)
  }

  val pipe = SingleRowPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |           n |       |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("extra identifiers are not a problem") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |     a, b, c |       |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("super many identifiers stretches the column") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+------------------+-------+
        || Operator | EstimatedRows | Rows | DbHits |      Identifiers | Other |
        |+----------+---------------+------+--------+------------------+-------+
        ||     NAME |             1 |   42 |     33 | a, b, c, d, e, f |       |
        |+----------+---------------+------+--------+------------------+-------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats are not shown") {
    val arguments = Seq()

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))

    renderDetails(plan) should equal(
      """+----------+---------------+-------------+-------+
        || Operator | EstimatedRows | Identifiers | Other |
        |+----------+---------------+-------------+-------+
        ||     NAME |             1 |           n |       |
        |+----------+---------------+-------------+-------+
        |""".stripMargin)
  }

  test("two plans with the same name get unique-ified names") {
    val args1 = Seq(Rows(42), DbHits(33))
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", "Prop"))

    val plan1 = PlanDescriptionImpl(pipe, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(pipe, "NAME", SingleChild(plan1), args2, Set("b"))

    renderDetails(plan2) should equal(
      """+----------+---------------+------+--------+-------------+--------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |        Other |
        |+----------+---------------+------+--------+-------------+--------------+
        ||  NAME(0) |             1 |    2 |    633 |           b | :Label(Prop) |
        ||  NAME(1) |             1 |   42 |     33 |           a |              |
        |+----------+---------------+------+--------+-------------+--------------+
        |""".stripMargin)
  }


  test("Expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = ExpandAllPipe(pipe, "from", "rel", "to", Direction.INCOMING, LazyTypes.empty)(Some(1L))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+-------------+---------------+-------------+---------------------+
        ||    Operator | EstimatedRows | Identifiers |               Other |
        |+-------------+---------------+-------------+---------------------+
        || Expand(All) |             1 |     rel, to | (from)<-[rel:]-(to) |
        |+-------------+---------------+-------------+---------------------+
        |""".stripMargin)
  }

  test("Label scan should be just as pretty as you would expect") {
    val pipe = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(1L))(mock[PipeMonitor])

    renderDetails( pipe.planDescription ) should equal(
      """+-----------------+---------------+-------------+-------+
        ||        Operator | EstimatedRows | Identifiers | Other |
        |+-----------------+---------------+-------------+-------+
        || NodeByLabelScan |             1 |           n |  :Foo |
        |+-----------------+---------------+-------------+-------+
        |""".stripMargin )
  }

  test("Var length expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = VarLengthExpandPipe(pipe, "from", "rel", "to", Direction.INCOMING, Direction.OUTGOING, LazyTypes.empty, 0, None, nodeInScope = false)(Some(1L))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+----------------------+---------------+-------------+----------------------+
        ||             Operator | EstimatedRows | Identifiers |                Other |
        |+----------------------+---------------+-------------+----------------------+
        || VarLengthExpand(All) |             1 |     rel, to | (from)-[rel:*]->(to) |
        |+----------------------+---------------+-------------+----------------------+
        |""".stripMargin)
  }

  test("do not show unnamed identifiers") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("  UNNAMED123", "R", Seq("WHOOP"), "  UNNAMED24", Direction.OUTGOING)
    )

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  FRESHID12", "  AGGREGATION255"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-----------------------------------+------------------+
        || Operator | EstimatedRows | Rows | DbHits |                       Identifiers |            Other |
        |+----------+---------------+------+--------+-----------------------------------+------------------+
        ||     NAME |             1 |   42 |     33 | anon[255], anon[12], anon[123], n | ()-[R:WHOOP]->() |
        |+----------+---------------+------+--------+-----------------------------------+------------------+
        |""".stripMargin)
  }

  test("show multiple relationship types") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("source", "through", Seq("SOME","OTHER","THING"), "target", Direction.OUTGOING)
    )

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------------------------------------------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |                                           Other |
        |+----------+---------------+------+--------+-------------+-------------------------------------------------+
        ||     NAME |             1 |   42 |     33 |           n | (source)-[through:SOME|:OTHER|:THING]->(target) |
        |+----------+---------------+------+--------+-------------+-------------------------------------------------+
        |""".stripMargin)
  }

  test("show nicer output instead of unnamed identifiers in equals expression") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Not(Equals(Identifier("  UNNAMED123"), Identifier("  UNNAMED321")))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-----------------------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |                       Other |
        |+----------+---------------+------+--------+-------------+-----------------------------+
        ||     NAME |             1 |   42 |     33 |           n | NOT(anon[123] == anon[321]) |
        |+----------+---------------+------+--------+-------------+-----------------------------+
        |""".stripMargin)
  }

  test("show hasLabels nicely without token id") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(HasLabel(Identifier("x"), KeyToken.Resolved("Artist", 5, TokenType.Label))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+----------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |    Other |
        |+----------+---------------+------+--------+-------------+----------+
        ||     NAME |             1 |   42 |     33 |           n | x:Artist |
        |+----------+---------------+------+--------+-------------+----------+
        |""".stripMargin)
  }

  test("format length properly") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(LengthFunction(Identifier("n"))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-----------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |     Other |
        |+----------+---------------+------+--------+-------------+-----------+
        ||     NAME |             1 |   42 |     33 |           n | length(n) |
        |+----------+---------------+------+--------+-------------+-----------+
        |""".stripMargin)
  }

  test("don't leak deduped names") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Identifier("  id@23")))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderDetails(plan)
    details should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |           n |    id |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("don't render planner in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"),
      LegacyExpression(Identifier("  id@23")))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers | Other |
        |+----------+---------------+------+--------+-------------+-------+
        ||     NAME |             1 |   42 |     33 |           n |    id |
        |+----------+---------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("round estimated rows to int") {
    val pipe1 = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(0.00123456789))(mock[PipeMonitor])
    val pipe2 = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Some(1.23456789))(mock[PipeMonitor])

    renderDetails(pipe1.planDescription) should equal(
      """+-----------------+---------------+-------------+-------+
        ||        Operator | EstimatedRows | Identifiers | Other |
        |+-----------------+---------------+-------------+-------+
        || NodeByLabelScan |             0 |           n |  :Foo |
        |+-----------------+---------------+-------------+-------+
        |""".stripMargin )

    renderDetails(pipe2.planDescription ) should equal(
      """+-----------------+---------------+-------------+-------+
        ||        Operator | EstimatedRows | Identifiers | Other |
        |+-----------------+---------------+-------------+-------+
        || NodeByLabelScan |             1 |           n |  :Foo |
        |+-----------------+---------------+-------------+-------+
        |""".stripMargin )
  }

  test("properly show Property") {
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ),
      LegacyExpression( Property(Identifier( "x" ), KeyToken.Resolved( "Artist", 5, TokenType.PropertyKey ))))

    val plan = PlanDescriptionImpl( pipe, "NAME", NoChildren, arguments, Set( "n") )
    renderDetails(plan) should equal(
                 """+----------+---------------+------+--------+-------------+----------+
                   || Operator | EstimatedRows | Rows | DbHits | Identifiers |    Other |
                   |+----------+---------------+------+--------+-------------+----------+
                   ||     NAME |             1 |   42 |     33 |           n | x.Artist |
                   |+----------+---------------+------+--------+-------------+----------+
                   |""".stripMargin )
  }

  test("show hasProp with identifier and property") {
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ),
      LegacyExpression( PropertyExists(Identifier("x"), KeyToken.Resolved("prop", 42, TokenType.PropertyKey))))

    val plan = PlanDescriptionImpl( pipe, "NAME", NoChildren, arguments, Set( "n") )
    renderDetails(plan) should equal(
      """+----------+---------------+------+--------+-------------+-----------------+
        || Operator | EstimatedRows | Rows | DbHits | Identifiers |           Other |
        |+----------+---------------+------+--------+-------------+-----------------+
        ||     NAME |             1 |   42 |     33 |           n | hasProp(x.prop) |
        |+----------+---------------+------+--------+-------------+-----------------+
        |""".stripMargin )
  }

  test("don't show unnamed identifiers in key names") {
    val joinPipe = NodeHashJoinPipe(Set("a", "  UNNAMED45", "  FRESHID77"), pipe, pipe)(Some(42))(mock[PipeMonitor])
    val arguments = Seq(
      Rows( 42 ),
      DbHits( 33 ))

    renderDetails(joinPipe.planDescription) should equal(
      """+--------------+---------------+-------------+-----------------------+
        ||     Operator | EstimatedRows | Identifiers |                 Other |
        |+--------------+---------------+-------------+-----------------------+
        || NodeHashJoin |            42 |             | a, anon[45], anon[77] |
        ||  Argument(1) |             - |             |                       |
        ||  Argument(1) |             - |             |                       |
        |+--------------+---------------+-------------+-----------------------+
        |""".stripMargin )
  }
}
