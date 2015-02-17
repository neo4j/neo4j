/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import java.util.Locale

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, LengthFunction}
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
      """+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers | Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n |       |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("extra identifiers are not a problem") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c"))

    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers | Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |     a, b, c |       |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("super many identifiers stretches the column") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("a", "b", "c", "d", "e", "f"))

    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+------------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits |      Identifiers | Other |
        |+----------+------------------------------+-----------------------+------+--------+------------------+-------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 | a, b, c, d, e, f |       |
        |+----------+------------------------------+-----------------------+------+--------+------------------+-------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats uses question marks") {
    val arguments = Seq()

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n"))

    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+-------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Identifiers | Other |
        |+----------+------------------------------+-----------------------+-------------+-------+
        ||     NAME |                        1.000 |                 1.000 |           n |       |
        |+----------+------------------------------+-----------------------+-------------+-------+
        |""".stripMargin)
  }

  test("two plans with the same name get unique-ified names") {
    val args1 = Seq(Rows(42), DbHits(33))
    val args2 = Seq(Rows(2), DbHits(633), Index("Label", "Prop"))

    val plan1 = PlanDescriptionImpl(pipe, "NAME", NoChildren, args1, Set("a"))
    val plan2 = PlanDescriptionImpl(pipe, "NAME", SingleChild(plan1), args2, Set("b"))

    renderDetails(plan2) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+--------------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |        Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+--------------+
        ||  NAME(0) |                        1.000 |                 1.000 |    2 |    633 |           b | :Label(Prop) |
        ||  NAME(1) |                        1.000 |                 1.000 |   42 |     33 |           a |              |
        |+----------+------------------------------+-----------------------+------+--------+-------------+--------------+
        |""".stripMargin)
  }


  test("Expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = ExpandAllPipe(pipe, "from", "rel", "to", Direction.INCOMING, LazyTypes.empty)(Estimation(Some(1.5), Some(1.6)))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+-------------+------------------------------+-----------------------+-------------+---------------------+
        ||    Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Identifiers |               Other |
        |+-------------+------------------------------+-----------------------+-------------+---------------------+
        || Expand(All) |                        1.500 |                 1.600 |     rel, to | (from)<-[rel:]-(to) |
        |+-------------+------------------------------+-----------------------+-------------+---------------------+
        |""".stripMargin)
  }

  test("Label scan should be just as pretty as you would expect") {
    val pipe = NodeByLabelScanPipe("n", LazyLabel("Foo"))(Estimation(Some(1.5), Some(1.6)))(mock[PipeMonitor])

    renderDetails( pipe.planDescription ) should equal(
      """+-----------------+------------------------------+-----------------------+-------------+-------+
        ||        Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Identifiers | Other |
        |+-----------------+------------------------------+-----------------------+-------------+-------+
        || NodeByLabelScan |                        1.500 |                 1.600 |           n |  :Foo |
        |+-----------------+------------------------------+-----------------------+-------------+-------+
        |""".stripMargin )
  }

  test("Var length expand contains information about its relations") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))
    val expandPipe = VarLengthExpandPipe(pipe, "from", "rel", "to", Direction.INCOMING, Direction.OUTGOING, LazyTypes.empty, 0, None, nodeInScope = false)(Estimation(Some(1.5), Some(1.6)))(mock[PipeMonitor])

    renderDetails(expandPipe.planDescription) should equal(
      """+----------------------+------------------------------+-----------------------+-------------+----------------------+
        ||             Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Identifiers |                Other |
        |+----------------------+------------------------------+-----------------------+-------------+----------------------+
        || VarLengthExpand(All) |                        1.500 |                 1.600 |     rel, to | (from)-[rel:*]->(to) |
        |+----------------------+------------------------------+-----------------------+-------------+----------------------+
        |""".stripMargin)
  }

  test("do not show unnamed identifiers") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("  UNNAMED123", "R", Seq("WHOOP"), "  UNNAMED24", Direction.OUTGOING)
    )

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+------------------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |            Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+------------------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n | ()-[R:WHOOP]->() |
        |+----------+------------------------------+-----------------------+------+--------+-------------+------------------+
        |""".stripMargin)
  }

  test("show multiple relationship types") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      ExpandExpression("source", "through", Seq("SOME","OTHER","THING"), "target", Direction.OUTGOING)
    )

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-------------------------------------------------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |                                           Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------------------------------------------------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n | (source)-[through:SOME|:OTHER|:THING]->(target) |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------------------------------------------------+
        |""".stripMargin)
  }

  test("show nicer output instead of unnamed identifiers in equals expression") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Not(Equals(Identifier("  UNNAMED123"), Identifier("  UNNAMED321")))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-----------------------------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |                       Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-----------------------------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n | NOT(anon[123] == anon[321]) |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-----------------------------+
        |""".stripMargin)
  }

  test("show hasLabels nicely without token id") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(HasLabel(Identifier("x"), KeyToken.Resolved("Artist", 5, TokenType.Label))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+--------------------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |              Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+--------------------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n | hasLabel(x:Artist) |
        |+----------+------------------------------+-----------------------+------+--------+-------------+--------------------+
        |""".stripMargin)
  }

  test("format length properly") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(LengthFunction(Identifier("n"))))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-----------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers |     Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-----------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n | length(n) |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-----------+
        |""".stripMargin)
  }

  test("don't leak deduped names") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      LegacyExpression(Identifier("  id@23")))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers | Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n |    id |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("don't render planner in Other") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Planner("COST"),
      LegacyExpression(Identifier("  id@23")))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments, Set("n", "  UNNAMED123", "  UNNAMED2", "  UNNAMED24"))
    renderDetails(plan) should equal(
      """+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        || Operator | EstimatedOperatorCardinality | EstimatedProducedRows | Rows | DbHits | Identifiers | Other |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        ||     NAME |                        1.000 |                 1.000 |   42 |     33 |           n |    id |
        |+----------+------------------------------+-----------------------+------+--------+-------------+-------+
        |""".stripMargin)
  }
}
