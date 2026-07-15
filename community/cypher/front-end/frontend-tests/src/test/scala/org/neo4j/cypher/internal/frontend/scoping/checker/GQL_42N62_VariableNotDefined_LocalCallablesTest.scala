/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.E42N62
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

import scala.util.Random

/**
 * Test for 42N62 - Variable Not Defined w.r.t local callables
 */
class GQL_42N62_VariableNotDefined_LocalCallablesTest extends LocalCallableVariableCheckingTestSuite {

  implicit val rand: Random = new Random(0)

  /*
   * Variable not defined error w.r.t in a local callables body
   *
   * This generates
   * - tests for the error with a variable that does not reference a parameter of the callable
   * - tests against the error with a variable that references a parameter of the callable
   */
  {
    val testQueries = for {
      // basics
      kind <- Seq(Procedure, Function)
      numParameters <- Seq(0, 1, 5)
      parameters = variableNames(numParameters)
      // variable to check and what to check
      isDefined <- Seq(true, false)
      if !isDefined || parameters.nonEmpty
      variable = if (isDefined) pickOne(parameters) else 'x'
      // callable definition
      body <- Seq(
        Query(s"RETURN $variable AS y"),
        Query(
          s"""LET z = $variable
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, $variable, 3] AS z
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""MATCH (z {prop: $variable})
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""MATCH (z:$$($variable))
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |LET z = $variable
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |RETURN y
             |NEXT
             |LET z = $variable
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |CALL ($variable) {
             |  RETURN 1 AS z
             |}
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |WITH y, $variable * 2 AS z
             |RETURN z AS y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |ORDER BY y % $variable
             |RETURN y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |FILTER y > $variable
             |RETURN y""".stripMargin
        ),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |RETURN y ORDER BY $variable DESC""".stripMargin
        ),
        Expression(s"2 * $variable", "y"),
        Expression(s"toString($variable)", "y"),
        Expression(s"[y IN [1, $variable, 3] | 2 * y]", "y"),
        Expression(s"[y IN [1, 2, 3] | $variable * y]", "y")
      )
      definition =
        s"""DEFINE ${kind.keyword} foo(${parameters.mkString(", ")}) ${body.renderFor(kind)}""".stripMargin
      // query after callable definition
      outerVariable = if (isDefined) "outer" else s"$variable"
      before <- Seq(
        "// omitted before",
        s"""LET $outerVariable = 1""".stripMargin,
        s"""UNWIND [1, 2, 3] AS w
           |LET $outerVariable = 1""".stripMargin,
        s"""UNWIND [1, 2, 3] AS w
           |LET $outerVariable = 1
           |FILTER $outerVariable = w""".stripMargin
      )
      call = {
        def arg(i: Int) =
          if (before.startsWith("//")) s"$i" // omitted before
          else s"$i * $outerVariable"
        val invoc = s"foo(${(1 to numParameters).map(arg).mkString(", ")})"
        kind match {
          case Procedure =>
            s"CALL $invoc"
          case Function =>
            s"LET y = $invoc"
        }
      }
      query = s"""$definition
                 |$before
                 |$call
                 |RETURN y""".stripMargin
    } yield {
      // If test with duplicate names are created the following variable printouts help to find the reason
      // println(s"numParameters = $numParameters")
      // println(s"isDefined = $isDefined")
      // println(s"variable = $variable")
      // println(query)
      // println()
      (query, isDefined, variable.toString)
    }

    for {
      (innerQuery, isDefined, variable) <- testQueries
      query <-
        if (!isDefined) outerQuery(innerQuery, variable).map(_._1)
        else outerQuery(innerQuery)
      outcome = if (isDefined) Passes else E42N62(variable)
    } {
      test(query) {
        check(ignoreBeforeCypher25(outcome))
      }
    }
  }

  /*
   * variable not defined after a local callables invocation
   */

  /*
   * Variable not defined error w.r.t before and after an invocation of a local callables body
   *
   * This generates
   * - tests for the error with a reference before a local callable invocation referencing a variable returned by the callable
   * - tests for the error with a reference after a local callable invocation referencing a variable is not returned by the callable
   * - tests against the error with a reference after a local callable invocation referencing a variable is returned by the callable
   */
  {
    val testQueries = for {
      // basics
      kind <- Seq(Procedure, Function)
      numParameters <- Seq(0, 1, 5)
      parameters = variableNames(numParameters)
      // variable in the callable body
      numVariables <- Seq(0, 6, 10)
      variables = variableNames(numVariables, numParameters / 2)
      // variables returned or not returned
      numReturns <- Seq(0, 1, 5)
      if numVariables > numReturns
      (returns, nonReturns) = pickDistinct(variables, numReturns)
      // callable definition
      definition = {
        val withItems = variables.zipWithIndex.collect {
          case (v, i) if !(parameters contains v) =>
            s"$i AS $v" // new variable binding, the others are parameters with no projection needed
        }
        val withClause = s"WITH ${withItems.mkString(", ")}"
        val returnClause =
          if (returns.isEmpty) "FINISH"
          else s"RETURN ${returns.mkString(", ")}"
        val body = Query(
          s"""  $withClause
             |  $returnClause""".stripMargin
        )
        s"DEFINE ${kind.keyword} foo(${parameters.mkString(", ")}) ${body.renderFor(kind)}"
      }
      // variable to check and what to check and where to check
      testBefore <- Seq(true, false)
      shouldTestDefined =
        // testing a variable being defined is only interesting after a procedure call for this test
        !testBefore && kind == Procedure
      isDefined <- Set(
        shouldTestDefined,
        false
      )
      testOnReturns = testBefore || isDefined
      if !testOnReturns || returns.nonEmpty
      variable = pickOne(if (testOnReturns) returns else nonReturns)
      testClause = s"FILTER $variable > 0"
      // query after callable definition
      before = {
        val otherVariables = variableNames(3).map(_.toString + "x")
        val letItems = otherVariables.zipWithIndex.map {
          case (r, i) => s"$r = $i"
        }
        s"LET ${letItems.mkString(", ")}"
      }
      call = {
        val invoc = s"foo(${(1 to numParameters).map(i => s"$i").mkString(", ")})"
        kind match {
          case Procedure =>
            s"CALL $invoc"
          case Function =>
            s"LET y = $invoc"
        }
      }
      query =
        if (testBefore)
          s"""$definition
             |$before
             |$testClause
             |$call
             |RETURN 123 AS y""".stripMargin
        else
          s"""$definition
             |$before
             |$call
             |$testClause
             |RETURN 123 AS y""".stripMargin
    } yield {
      // If test with duplicate names are created the following variable printouts help to find the reason
      // println(s"numParameters = $numParameters")
      // println(s"numVariables = $numVariables")
      // println(s"numReturns = $numReturns")
      // println(s"isDefined = $isDefined")
      // println(s"variable = $variable")
      // println(query)
      // println()
      (query, isDefined, variable.toString)
    }
    for {
      (innerQuery, isDefined, variable) <- testQueries
      query <- outerQuery(innerQuery)
      outcome = if (isDefined) Passes else E42N62(variable)
    } {
      test(query) {
        check(ignoreBeforeCypher25(outcome))
      }
    }
  }
}
