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

import org.neo4j.cypher.internal.frontend.scoping.E42N07
import org.neo4j.cypher.internal.frontend.scoping.E42N59
import org.neo4j.cypher.internal.frontend.scoping.Outcome
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

import scala.util.Random

/**
 * Test for
 * - 42N59 - Variable Already Declared
 * - 42N07 - Variable is shadowing a variable with the same name
 * w.r.t local callables
 */
class GQL_42N59_VariableAlreadyDeclared_LocalCallablesTest extends LocalCallableVariableCheckingTestSuite {

  implicit val rand: Random = new Random(0)

  /*
   * Variable already declared error in a local callables body
   *
   * This generates
   * - tests for the error with a variable declaration that re-declares a parameter of the callable
   */
  {
    val testQueries = for {
      // basics
      kind <- Seq(Procedure, Function)
      numParameters <- Seq(1, 5)
      parameters = variableNames(numParameters)
      numOtherVariables <- Seq(0, 3)
      otherVariables = variableNames(numOtherVariables, numParameters)
      // variable to check and what to check
      variable <- pickDistinct(parameters, Math.min(numParameters, 2))._1.map(_.toString)
      // callable definition
      projections = otherVariables.zipWithIndex.map { case (v, i) => (v, s"$i") } :+ (variable, "1")
      ((body, withProjection), error) <- Seq(
        Query(
          s"""LET ${projections.map(p => s"${p._1} = ${p._2}").mkString(", ")}
             |RETURN $variable AS y""".stripMargin
        ) -> true -> E42N07(variable),
        Query(
          s"""UNWIND [1, 2, 3] AS $variable
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N07(variable),
        Query(
          s"""MATCH $variable = ()-->()
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N07(variable),
        Query(
          s"""MATCH (x)--(y)(($variable)-->(t)){1,5}()-->(z)
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N59(variable),
        Query(
          s"""MATCH (movie:Movie)
             |  SEARCH movie IN (
             |    VECTOR INDEX moviePlots
             |    FOR [1, 2, 3]
             |    LIMIT 5
             |  ) SCORE AS $variable
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N07(variable),
        Query(
          s"""CALL test.my.proc(null) YIELD $variable
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N07(variable),
        Query(
          s"""CALL test.my.proc(null) YIELD res AS $variable
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N07(variable),
        Query(
          s"""CREATE ($variable)
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N59(variable),
        Query(
          s"""CREATE ()-[$variable]->()
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N59(variable),
        Query(
          s"""MERGE ($variable {p:$variable.p})
             |RETURN $variable AS y""".stripMargin
        ) -> false -> E42N59(variable),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |WITH ${projections.map(p => s"${p._2} AS ${p._1}").mkString(", ")}
             |RETURN $variable AS y""".stripMargin
        ) -> true -> E42N07(variable),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |RETURN y
             |UNION
             |LET ${projections.map(p => s"${p._1} = ${p._2}").mkString(", ")}
             |RETURN $variable AS y""".stripMargin
        ) -> true -> E42N07(variable),
        Query(
          s"""UNWIND [1, 2, 3] AS y
             |RETURN y
             |NEXT
             |LET ${projections.map(p => s"${p._1} = ${p._2}").mkString(", ")}
             |RETURN z AS y""".stripMargin
        ) -> true -> E42N07(variable),
        Query(
          s"""WHEN y = 0 THEN {
             |  LET ${projections.map(p => s"${p._1} = ${p._2}").mkString(", ")}
             |  RETURN $variable AS y
             |}
             |ELSE RETURN 0 AS y""".stripMargin
        ) -> true -> E42N07(variable),
        Query(
          s"""USE myGraph {
             |  LET ${projections.map(p => s"${p._1} = ${p._2}").mkString(", ")}
             |  RETURN $variable AS y
             |}""".stripMargin
        ) -> true -> E42N07(variable)
      )
      if withProjection || numOtherVariables == 0
      definition =
        s"""DEFINE ${kind.keyword} foo(${parameters.mkString(", ")}) ${body.renderFor(kind)}""".stripMargin
      // query after callable definition
      outerVariable <- Seq("outer", s"$variable")
      before <- Seq(
        "// omitted before",
        s"""LET $outerVariable = 1""".stripMargin,
        s"""UNWIND [1, 2, 3] AS w
           |LET $outerVariable = 1""".stripMargin,
        s"""UNWIND [1, 2, 3] AS w
           |LET $outerVariable = 1
           |FILTER $outerVariable = w""".stripMargin
      )
      if !(before.startsWith("//") && outerVariable == "outer")
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
      // println(s"numOtherVariables = $numOtherVariables")
      // println(s"outerVariable = $outerVariable")
      // println(s"variable = $variable")
      // println(query)
      // println()
      (query, error, variable)
    }

    for {
      (innerQuery, error, _) <- testQueries
      query <- outerQuery(innerQuery)
    } {
      test(query) {
        check(ignoreBeforeCypher25(error))
      }
    }
  }

  /*
   * Variable already error when invoking a local callables
   *
   * This generates
   * - tests for the error with a local procedure call return a variable already declared in the same single query
   * - tests for the error with a local procedure call return a variable already declared in out scope
   * - tests against the error with a local procedure call return a variable not already declared
   */
  {
    val testQueries = for {
      // basics
      kind <- Seq(Procedure, Function)
      numParameters <- Seq(0, 1, 5)
      parameters = variableNames(numParameters)
      // variable in the callable body
      numVariables <- Seq(3, 8)
      variables = rand.shuffle(variableNames(numVariables, numParameters / 2))
      // variables returned or not returned
      numReturns <- Seq(0, 1, 5)
      if numVariables > numReturns
      (returns, nonReturns) = pickDistinct(variables, numReturns)
      // callable definition
      definition = {
        val withItems = variables.zipWithIndex.collect {
          case (v, i) if !(parameters contains v) =>
            s"$i AS $v" // new variable binding, the others are parameters with no projection needed
        } :+ s"'abc' AS extra"
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
      // variable to check and what to check
      isDefined <- Set(
        true,
        false
      )
      variable = if (returns.nonEmpty) pickOne(returns) else pickOne(nonReturns)
      isReturned = kind == Procedure && returns.nonEmpty
      // original variable declaration
      before = {
        val variablesToDeclare =
          if (isDefined) nonReturns.toSet + variable
          else nonReturns.toSet - variable
        val letItems = rand.shuffle(variablesToDeclare).zipWithIndex.map {
          case (r, i) => s"$r = $i"
        }
        s"LET ${letItems.mkString(", ")}"
      }
      // local call
      withYield <- Set(isReturned, false)
      call = {
        val invoc = s"foo(${(1 to numParameters).map(i => s"$i").mkString(", ")})"
        kind match {
          case Procedure if withYield =>
            s"CALL $invoc YIELD ${returns.mkString(", ")}"
          case Procedure =>
            s"CALL $invoc"
          case Function =>
            s"LET y = $invoc"
        }
      }
      // query after callable definition
      query =
        s"""$definition
           |$before
           |$call
           |RETURN 123 AS y""".stripMargin
    } yield {
      // If test with duplicate names are created the following variable printouts help to find the reason
      // println(s"numParameters = $numParameters")
      // println(s"numVariables = $numVariables")
      // println(s"numReturns = $numReturns")
      // println(s"isDefined = $isDefined")
      // println(s"isReturned = $isReturned")
      // println(s"variable = $variable")
      // println(query)
      // println()
      (query, isDefined, isReturned, variable.toString)
    }
    for {
      (innerQuery, isDefined, isReturned, variable) <- testQueries
      (query, variableCircumstance) <-
        if (isDefined) outerQuery(innerQuery).map(q => (q, UndefinedInOuterScope))
        else outerQuery(innerQuery, variable)
      outcome = (variableCircumstance, isDefined, isReturned) match {
        case (ParameterOfOuterCallable, _, true) | (ParameterOfOuterCallable, true, _)         => E42N07(variable)
        case (DefinedInOuterScope, _, true) | (DefinedInOuterScope, true, _) | (_, true, true) => E42N59(variable)
        case _                                                                                 => Passes
      }
    } {
      test(query) {
        check(ignoreBeforeCypher25(outcome))
      }
    }
  }

  for {
    (query, outcomeModifier) <- Seq[(String, Outcome => Outcome)](
      """WITH [1, 2, 3] AS list, 0 AS i
        |FOREACH (i IN list | CREATE (:A {p: i}))
        |CALL foo(0)""".stripMargin -> identity,
      """LET list = [1, 2, 3], i = 0
        |FOREACH (i IN list | CREATE (:A {p: i}))
        |CALL foo(0)""".stripMargin -> ignoreBeforeCypher25,
      """WITH 0 AS i
        |CALL (i) {
        |  WITH [1, 2, 3] AS list
        |  FOREACH (i IN list | CREATE (:A {p: i}))
        |}""".stripMargin -> identity,
      """LET i = 0
        |CALL (i) {
        |  LET list = [1, 2, 3]
        |  FOREACH (i IN list | CREATE (:A {p: i}))
        |}""".stripMargin -> ignoreBeforeCypher25,
      """DEFINE PROCEDURE foo(i) {
        |  LET list = [1, 2, 3]
        |  FOREACH (i IN list | CREATE (:A {p: i}))
        |}
        |CALL foo(0)""".stripMargin -> ignoreBeforeCypher25,
      // the following query is invalid outside the VariableChecker
      // since local function shall contain updates
      """DEFINE FUNCTION foo(i) {
        |  LET list = [1, 2, 3]
        |  FOREACH (i IN list | CREATE (:A {p: i}))
        |  RETURN size(list) AS numCreated
        |}
        |CALL foo(0)""".stripMargin -> ignoreBeforeCypher25
    )
  } {
    test(query) {
      check(outcomeModifier(Passes))
    }
  }
}
