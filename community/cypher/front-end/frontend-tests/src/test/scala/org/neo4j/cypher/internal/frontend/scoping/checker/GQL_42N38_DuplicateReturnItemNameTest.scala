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

import org.neo4j.cypher.internal.frontend.scoping.E42N38
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.checker.Clause.LET
import org.neo4j.cypher.internal.frontend.scoping.checker.Clause.RETURN
import org.neo4j.cypher.internal.frontend.scoping.checker.Clause.WITH
import org.neo4j.cypher.internal.frontend.scoping.checker.ProjItem.StringAsOps

/**
 * Test for 42N38 - Duplicate Return Item Name
 */
class GQL_42N38_DuplicateReturnItemNameTest extends VariableCheckingWithLocalCallablesTestSuite
    with LocalCallableGenHelpers {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  private def variableSets = Seq(
    ("a", "b", "c"),
    ("x", "y", "z")
  )

  private def _positiveProjection: Seq[Proj] = {
    Seq(
      Proj(Seq("1" AS "a", "10" AS "z"), Set.empty)
    ) ++
      (for {
        vars <- variableSets
      } yield Seq(
        Proj(Seq(vars._1 AS "a", vars._1 AS "z"), Set(vars._1)),
        Proj(Seq(vars._1 AS "a", "10" AS "z"), Set(vars._1)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", vars._1 AS "z"), Set(vars._1, vars._2)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", vars._3 AS "z"), Set(vars._1, vars._2, vars._3)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", "1" AS "z", vars._3 AS "d"), Set(vars._1, vars._2, vars._3))
      )).flatten
  }

  private def _negativeProjection: Seq[Proj] = {
    Seq(
      Proj(Seq("1" AS "a", "10" AS "a"), Set.empty)
    ) ++
      (for {
        vars <- variableSets
      } yield Seq(
        Proj(Seq(vars._1 AS "a", "a" AS "a"), Set(vars._1, "a")),
        Proj(Seq(vars._1 AS "a", "10" AS "a"), Set(vars._1)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", vars._1 AS "a"), Set(vars._1, vars._2)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", vars._3 AS "a"), Set(vars._1, vars._2, vars._3)),
        Proj(Seq(vars._1 AS "a", vars._2 AS "b", "1" AS "a", vars._3 AS "d"), Set(vars._1, vars._2, vars._3))
      )).flatten
  }

  private def _clauseToTest: Seq[Clause] = Seq(LET, WITH, RETURN)

  private type ReturnCols = Seq[String]

  private def _clausesToEndQuery: Seq[(String, ReturnCols)] = Seq(
    "RETURN 123 AS foo" -> Seq("foo"),
    "FINISH" -> Seq.empty
  )

  private type IgnoreInCypher5 = Boolean

  private def _innerQuery(isNegativeTest: Boolean): Seq[(String, ReturnCols, IgnoreInCypher5)] = for {
    p <- if (isNegativeTest) _negativeProjection else _positiveProjection
    clauseToTest <- _clauseToTest
    if !(clauseToTest == LET) || (p.items.map(_.variable).toSet intersect p.referencedVariables).isEmpty
    (after, returnCol) <- clauseToTest match {
      case RETURN => Seq("// end of query" -> p.items.map(_.variable))
      case _      => _clausesToEndQuery
    }
    before =
      if (p.referencedVariables.isEmpty) {
        "// not variables declared"
      } else {
        s"WITH ${
            p.referencedVariables.zipWithIndex.map {
              case (v, i) => s"$i AS $v"
            }.mkString(", ")
          }"
      }
    itemList = p.items.map {
      case ProjItem(e, v) => clauseToTest match {
          case LET => s"$v = $e"
          case _   => s"$e AS $v"
        }
    }
  } yield {
    (
      s"""$before
         |${clauseToTest.keyword} ${itemList.mkString(", ")}
         |$after""".stripMargin,
      returnCol,
      clauseToTest == LET
    )
  }

  private def _outerQuery: Seq[(String, ReturnCols, IgnoreInCypher5) => (String, ReturnCols, IgnoreInCypher5)] = Seq(
    (inner: String, returnCols: ReturnCols, ignoreInCypher5: IgnoreInCypher5) => (inner, returnCols, ignoreInCypher5),
    (inner: String, _: ReturnCols, ignoreInCypher5: IgnoreInCypher5) =>
      (
        s"""MATCH (n)
           |RETURN EXISTS {
           |${indent(inner, "  ")}
           |} AS bar""".stripMargin,
        Seq("bar"),
        ignoreInCypher5
      ),
    (inner: String, _: ReturnCols, _: IgnoreInCypher5) =>
      (
        s"""MATCH (n)
           |RETURN EXISTS {
           |${indent(inner, "  ")}
           |  NEXT
           |  RETURN 0 AS foo
           |} AS bar""".stripMargin,
        Seq("bar"),
        true
      ),
    (inner: String, _: ReturnCols, _: IgnoreInCypher5) =>
      (
        s"""MATCH (n)
           |CALL (n) {
           |${indent(inner, "  ")}
           |  NEXT
           |  RETURN 0 AS foo
           |}
           |RETURN n""".stripMargin,
        Seq("n"),
        true
      )
  )

  private def _query(isNegativeTest: Boolean): Seq[TestQuery] = for {
    outer <- _outerQuery
    (inner, innerReturnCols, innerIgnoreInCypher5) <- _innerQuery(isNegativeTest)
    (query, queryReturnCols, ignoreInCypher5) = outer(inner, innerReturnCols, innerIgnoreInCypher5)
    outcomeUnversioned = if (isNegativeTest) E42N38 else Passes
    outcome = if (ignoreInCypher5) ignoreBeforeCypher25(outcomeUnversioned) else outcomeUnversioned
  } yield TestQuery(query, outcome, queryReturnCols)

  override def testCases(): Seq[TestQuery] =
    // Negative tests
    _query(true) ++
      // Positive tests
      _query(false)

}

sealed trait Clause {
  val keyword: String
}

object Clause {

  case object LET extends Clause {
    override val keyword: String = "LET"
  }

  case object WITH extends Clause {
    override val keyword: String = "WITH"
  }

  case object RETURN extends Clause {
    override val keyword: String = "RETURN"
  }
}

case class ProjItem(exp: String, variable: String)

object ProjItem {

  implicit class StringAsOps(private val exp: String) extends AnyVal {
    def AS(variable: String): ProjItem = ProjItem(exp, variable)
  }
}

case class Proj(items: Seq[ProjItem], referencedVariables: Set[String])
