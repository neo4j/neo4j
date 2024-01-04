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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class extractRuntimeConstantsTest extends CypherFunSuite with LogicalPlanningTestSupport with ProcedureTestSupport {

  test("should rewrite datetime({date: $d}))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("date", parameter("d1", CTAny))))),
      greaterThan(datetime(mapOf(("date", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", datetime(mapOf(("date", parameter("d1", CTAny)))))
      ),
      greaterThan(RuntimeConstant(v"  UNNAMED1", datetime(mapOf(("date", parameter("d2", CTAny))))), v"n")
    )
  }

  test("should rewrite datetime({date: $d, year: $y}))") {
    val expr = datetime(mapOf(("date", parameter("d", CTAny)), ("year", parameter("y", CTAny))))
    rewrite(expr) shouldBe RuntimeConstant(v"  UNNAMED0", expr)
  }

  test("should not rewrite datetime({date: $d, year: randomUDF()}))") {
    val expr = datetime(mapOf(("date", parameter("d", CTAny)), ("year", randomUDF())))
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite datetime({date: randomUDF()}))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("date", randomUDF())))),
      greaterThan(datetime(mapOf(("date", randomUDF()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should rewrite datetime({datetime: $d}))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("datetime", parameter("d1", CTAny))))),
      greaterThan(datetime(mapOf(("datetime", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", datetime(mapOf(("datetime", parameter("d1", CTAny)))))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", datetime(mapOf(("datetime", parameter("d2", CTAny))))),
        v"n"
      )
    )
  }

  test("should rewrite datetime({time: $d}))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("time", parameter("d1", CTAny))))),
      greaterThan(datetime(mapOf(("time", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", datetime(mapOf(("time", parameter("d1", CTAny)))))
      ),
      greaterThan(RuntimeConstant(v"  UNNAMED1", datetime(mapOf(("time", parameter("d2", CTAny))))), v"n")
    )
  }

  test("should rewrite datetime({year: $y}))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("year", parameter("y1", CTAny))))),
      greaterThan(datetime(mapOf(("year", parameter("y2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", datetime(mapOf(("year", parameter("y1", CTAny)))))
      ),
      greaterThan(RuntimeConstant(v"  UNNAMED1", datetime(mapOf(("year", parameter("y2", CTAny))))), v"n")
    )
  }

  test("should rewrite datetime('datestring'))") {
    val expr = ors(
      greaterThan(v"n", datetime(literalString("2015-07-21T21:40:32.142+0100"))),
      greaterThan(datetime(literalString("2010-07-21T21:40:32.142+0100")), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", datetime(literalString("2015-07-21T21:40:32.142+0100")))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", datetime(literalString("2010-07-21T21:40:32.142+0100"))),
        v"n"
      )
    )
  }

  test("should not rewrite datetime(randomUDF))") {
    val expr = ors(
      greaterThan(v"n", datetime(randomUDF())),
      greaterThan(datetime(randomUDF()), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite datetime({timezone: 'America/Los Angeles'))") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("timezone", literalString("America/Los Angeles"))))),
      greaterThan(datetime(mapOf(("timezone", literalString("America/Los Angeles")))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should rewrite datetime({timezone: 'America/Los Angeles', year: 1980))") {
    val expr = datetime(mapOf(("timezone", literalString("America/Los Angeles")), ("year", literalInt(1980))))
    rewrite(expr) shouldBe RuntimeConstant(v"  UNNAMED0", expr)
  }

  test("should not rewrite datetime())") {
    val expr = ors(
      greaterThan(v"n", datetime()),
      greaterThan(datetime(), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite datetime({date: date()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("date", date())))),
      greaterThan(datetime(mapOf(("date", date()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite datetime({time: time()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("time", time())))),
      greaterThan(datetime(mapOf(("time", time()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite datetime({datetime: datetime()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", datetime(mapOf(("datetime", datetime())))),
      greaterThan(datetime(mapOf(("datetime", datetime()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  /////////
  test("should rewrite localdatetime({date: $d}))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("date", parameter("d1", CTAny))))),
      greaterThan(localdatetime(mapOf(("date", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", localdatetime(mapOf(("date", parameter("d1", CTAny)))))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", localdatetime(mapOf(("date", parameter("d2", CTAny))))),
        v"n"
      )
    )
  }

  test("should rewrite localdatetime({datetime: $d}))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("datetime", parameter("d1", CTAny))))),
      greaterThan(localdatetime(mapOf(("datetime", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", localdatetime(mapOf(("datetime", parameter("d1", CTAny)))))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", localdatetime(mapOf(("datetime", parameter("d2", CTAny))))),
        v"n"
      )
    )
  }

  test("should rewrite localdatetime({time: $d}))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("time", parameter("d1", CTAny))))),
      greaterThan(localdatetime(mapOf(("time", parameter("d2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", localdatetime(mapOf(("time", parameter("d1", CTAny)))))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", localdatetime(mapOf(("time", parameter("d2", CTAny))))),
        v"n"
      )
    )
  }

  test("should rewrite localdatetime({year: $y}))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("year", parameter("y1", CTAny))))),
      greaterThan(localdatetime(mapOf(("year", parameter("y2", CTAny)))), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", localdatetime(mapOf(("year", parameter("y1", CTAny)))))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", localdatetime(mapOf(("year", parameter("y2", CTAny))))),
        v"n"
      )
    )
  }

  test("should rewrite  localdatetime('datestring'))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(literalString("2015-07-21T21:40:32.142+0100"))),
      greaterThan(localdatetime(literalString("2010-07-21T21:40:32.142+0100")), v"n")
    )
    rewrite(expr) shouldBe ors(
      greaterThan(
        v"n",
        RuntimeConstant(v"  UNNAMED0", localdatetime(literalString("2015-07-21T21:40:32.142+0100")))
      ),
      greaterThan(
        RuntimeConstant(v"  UNNAMED1", localdatetime(literalString("2010-07-21T21:40:32.142+0100"))),
        v"n"
      )
    )
  }

  test("should not rewrite localdatetime({timezone: 'America/Los Angeles'))") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("timezone", literalString("America/Los Angeles"))))),
      greaterThan(localdatetime(mapOf(("timezone", literalString("America/Los Angeles")))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localdatetime())") {
    val expr = ors(
      greaterThan(v"n", localdatetime()),
      greaterThan(localdatetime(), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localdatetime({date: date()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("date", date())))),
      greaterThan(localdatetime(mapOf(("date", date()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localdatetime({time: time()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("time", time())))),
      greaterThan(localdatetime(mapOf(("time", time()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localdatetime({datetime: datetime()})) (inner not constant)") {
    val expr = ors(
      greaterThan(v"n", localdatetime(mapOf(("datetime", datetime())))),
      greaterThan(localdatetime(mapOf(("datetime", datetime()))), v"n")
    )
    rewrite(expr) shouldBe expr
  }

  test("should rewrite date({year: 1980})") {
    val expr = equals(v"n", date(mapOf(("year", literalInt(1980)))))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", date(mapOf(("year", literalInt(1980)))))
    )
  }

  test("should not rewrite date({year: randomUDF()})") {
    val expr = equals(v"n", date(mapOf(("year", randomUDF()))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite date('1980-03-11')") {
    val expr = equals(v"n", date(literalString("1980-03-11")))
    rewrite(expr) shouldBe equals(v"n", RuntimeConstant(v"  UNNAMED0", date(literalString("1980-03-11"))))
  }

  test("should not rewrite date()") {
    val expr = equals(v"n", date())
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite date(randomUDF())") {
    val expr = equals(v"n", date(randomUDF()))
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite date when just a timezone is provided") {
    val expr = equals(v"n", date(mapOf(("timezone", literalString("America/Los Angeles")))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite time({hour: 23})") {
    val expr = equals(v"n", time(mapOf(("hour", literalInt(23)))))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", time(mapOf(("hour", literalInt(23)))))
    )
  }

  test("should not rewrite time({year: randomUDF()})") {
    val expr = equals(v"n", time(mapOf(("hour", randomUDF()))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite time('21:40:32.142+0100')") {
    val expr = equals(v"n", time(literalString("21:40:32.142+0100")))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", time(literalString("21:40:32.142+0100")))
    )
  }

  test("should not rewrite time()") {
    val expr = equals(v"n", time())
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite time(randomUDF())") {
    val expr = equals(v"n", time(randomUDF()))
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite time when just a timezone is provided") {
    val expr = equals(v"n", time(mapOf(("timezone", literalString("America/Los Angeles")))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite localtime({hour: 23})") {
    val expr = equals(v"n", localtime(mapOf(("hour", literalInt(23)))))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", localtime(mapOf(("hour", literalInt(23)))))
    )
  }

  test("should not rewrite localtime({year: randomUDF()})") {
    val expr = equals(v"n", localtime(mapOf(("hour", randomUDF()))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite localtime('21:40:32.142+0100')") {
    val expr = equals(v"n", localtime(literalString("21:40:32.142+0100")))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", localtime(literalString("21:40:32.142+0100")))
    )
  }

  test("should not rewrite localtime()") {
    val expr = equals(v"n", localtime())
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localtime(randomUDF())") {
    val expr = equals(v"n", localtime(randomUDF()))
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite localtime when just a timezone is provided") {
    val expr = equals(v"n", localtime(mapOf(("timezone", literalString("America/Los Angeles")))))
    rewrite(expr) shouldBe expr
  }

  test("should rewrite duration({years: 23})") {
    val expr = equals(v"n", duration(mapOf(("years", literalInt(23)))))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", duration(mapOf(("years", literalInt(23)))))
    )
  }

  test("should rewrite duration('P14DT16H12M')") {
    val expr = equals(v"n", duration(literalString("P14DT16H12M")))
    rewrite(expr) shouldBe equals(
      v"n",
      RuntimeConstant(v"  UNNAMED0", duration(literalString("P14DT16H12M")))
    )
  }

  test("should not rewrite duration(randomUDF())") {
    val expr = equals(v"n", duration(randomUDF()))
    rewrite(expr) shouldBe expr
  }

  test("should handle multiple constants") {
    val expr = datetime(mapOf(("date", date(mapOf(("year", literalInt(1980)))))))
    rewrite(expr) shouldBe RuntimeConstant(
      v"  UNNAMED1",
      datetime(mapOf(("date", RuntimeConstant(v"  UNNAMED0", date(mapOf(("year", literalInt(1980))))))))
    )
  }

  private def rewrite(e: Expression): Expression =
    e.endoRewrite(extractRuntimeConstants(new AnonymousVariableNameGenerator))

  private def datetime(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "datetime"), None, expressions.toIndexedSeq)(InputPosition.NONE)
  }

  private def localdatetime(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "localdatetime"), None, expressions.toIndexedSeq)(
      InputPosition.NONE
    )
  }

  private def time(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "time"), None, expressions.toIndexedSeq)(InputPosition.NONE)
  }

  private def localtime(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "localtime"), None, expressions.toIndexedSeq)(
      InputPosition.NONE
    )
  }

  private def date(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "date"), None, expressions.toIndexedSeq)(InputPosition.NONE)
  }

  private def duration(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "duration"), None, expressions.toIndexedSeq)(InputPosition.NONE)
  }

  private def randomUDF(expressions: Expression*): ResolvedFunctionInvocation = {
    ResolvedFunctionInvocation(QualifiedName(Seq.empty, "myRandom"), None, expressions.toIndexedSeq)(InputPosition.NONE)
  }
}
