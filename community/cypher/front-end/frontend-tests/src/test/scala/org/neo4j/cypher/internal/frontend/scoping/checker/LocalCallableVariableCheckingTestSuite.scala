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

import org.neo4j.cypher.internal.frontend.scoping.VariableCheckingTestSuite

trait LocalCallableVariableCheckingTestSuite extends VariableCheckingTestSuite with LocalCallableGenHelpers {

  /*
   * some interesting surroundings for the queries that are tested
   */
  def outerQuery(innerQuery: String): Seq[String] = outerQuery(innerQuery, "xBar").map(_._1)

  sealed trait VariableCircumstance
  case object UndefinedInOuterScope extends VariableCircumstance
  case object DefinedInOuterScope extends VariableCircumstance
  case object ParameterOfOuterCallable extends VariableCircumstance

  def outerQuery(innerQuery: String, variable: String): Seq[(String, VariableCircumstance)] =
    Seq(
      innerQuery -> UndefinedInOuterScope,
      s"""RETURN 1 AS $variable
         |NEXT
         |{
         |${indent(innerQuery, "  ")}
         |}""".stripMargin -> DefinedInOuterScope,
      s"""RETURN 1 AS $variable
         |NEXT
         |RETURN 0 AS y
         |UNION
         |{
         |${indent(innerQuery, "  ")}
         |}""".stripMargin -> DefinedInOuterScope,
      s"""LET $variable = 1
         |CALL ($variable) {
         |${indent(innerQuery, "  ")}
         |}""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |${indent(innerQuery, "  ")}
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  WHEN $variable = 0 THEN RETURN 0 AS y
         |  ELSE {
         |${indent(innerQuery, "    ")}
         |  }
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  WHEN $variable = 0 THEN RETURN 0 AS y
         |  WHEN $variable = 1 THEN {
         |${indent(innerQuery, "    ")}
         |  }
         |  ELSE RETURN 0 AS y
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  {
         |${indent(innerQuery, "    ")}
         |  }
         |  UNION
         |  RETURN 0 AS y
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  RETURN 0 AS y
         |  UNION
         |  {
         |${indent(innerQuery, "    ")}
         |  }
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  {
         |${indent(innerQuery, "    ")}
         |  }
         |  NEXT
         |  RETURN y AS y
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE PROCEDURE bar($variable) {
         |  RETURN $variable AS somethingElse
         |  NEXT
         |  {
         |${indent(innerQuery, "    ")}
         |  }
         |}
         |CALL bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE FUNCTION bar($variable) {
         |${indent(innerQuery, "  ")}
         |}
         |LET y = bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE FUNCTION bar($variable) = EXISTS {
         |${indent(innerQuery, "  ")}
         |}
         |LET y = bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE FUNCTION bar($variable) = COUNT {
         |${indent(innerQuery, "  ")}
         |}
         |LET y = bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable,
      s"""DEFINE FUNCTION bar($variable) = COLLECT {
         |${indent(innerQuery, "  ")}
         |}
         |LET y = bar(1)
         |RETURN y""".stripMargin -> ParameterOfOuterCallable
    )
}
