/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planDescription

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{NullPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments._

class RenderPlanDescriptionDetailsTest extends CypherFunSuite {

  val pipe = NullPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    val arguments = Seq(
      IntroducedIdentifier("n"),
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments)

    renderDetails(plan) should equal(
      """+----------+------+--------+-------------+-------+
        || Operator | Rows | DbHits | Identifiers | Other |
        |+----------+------+--------+-------------+-------+
        ||     NAME |   42 |     33 |           n |       |
        |+----------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("extra identifiers are not a problem") {
    val arguments = Seq(
      IntroducedIdentifier("a"),
      IntroducedIdentifier("b"),
      IntroducedIdentifier("c"),
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments)

    renderDetails(plan) should equal(
      """+----------+------+--------+-------------+-------+
        || Operator | Rows | DbHits | Identifiers | Other |
        |+----------+------+--------+-------------+-------+
        ||     NAME |   42 |     33 |     a, b, c |       |
        |+----------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("super many identifiers stretches the column") {
    val arguments = Seq(
      IntroducedIdentifier("a"),
      IntroducedIdentifier("b"),
      IntroducedIdentifier("c"),
      IntroducedIdentifier("d"),
      IntroducedIdentifier("e"),
      IntroducedIdentifier("f"),
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments)

    renderDetails(plan) should equal(
      """+----------+------+--------+------------------+-------+
        || Operator | Rows | DbHits |      Identifiers | Other |
        |+----------+------+--------+------------------+-------+
        ||     NAME |   42 |     33 | a, b, c, d, e, f |       |
        |+----------+------+--------+------------------+-------+
        |""".stripMargin)
  }

  test("execution plan without profiler stats uses question marks") {
    val arguments = Seq(IntroducedIdentifier("n"))

    val plan = PlanDescriptionImpl(pipe, "NAME", NoChildren, arguments)

    renderDetails(plan) should equal(
      """+----------+------+--------+-------------+-------+
        || Operator | Rows | DbHits | Identifiers | Other |
        |+----------+------+--------+-------------+-------+
        ||     NAME |    ? |      ? |           n |       |
        |+----------+------+--------+-------------+-------+
        |""".stripMargin)
  }

  test("two plans with the same name get uniquefied names") {
    val args1 = Seq(IntroducedIdentifier("a"), Rows(42), DbHits(33))
    val args2 = Seq(IntroducedIdentifier("b"), Rows(2), DbHits(633), Index("Label", "Prop"))

    val plan1 = PlanDescriptionImpl(pipe, "NAME", NoChildren, args1)
    val plan2 = PlanDescriptionImpl(pipe, "NAME", SingleChild(plan1), args2)

    renderDetails(plan2) should equal(
      """+----------+------+--------+-------------+--------------+
        || Operator | Rows | DbHits | Identifiers |        Other |
        |+----------+------+--------+-------------+--------------+
        ||  NAME(0) |    2 |    633 |           b | :Label(Prop) |
        ||  NAME(1) |   42 |     33 |           a |              |
        |+----------+------+--------+-------------+--------------+
        |""".stripMargin)
  }
}
