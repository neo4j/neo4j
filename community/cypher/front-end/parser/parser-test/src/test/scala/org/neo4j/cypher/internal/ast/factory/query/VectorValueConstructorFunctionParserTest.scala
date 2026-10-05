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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.VectorValueConstructor
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher25
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.BFloat16Type
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.Float16Type
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class VectorValueConstructorFunctionParserTest extends AstParsingTestBase {

  private val int64: CypherType = IntegerType(isNullable = false)(pos)
  private val int32: CypherType = Integer32Type(isNullable = false)(pos)
  private val int16: CypherType = Integer16Type(isNullable = false)(pos)
  private val int8: CypherType = Integer8Type(isNullable = false)(pos)
  private val float64: CypherType = FloatType(isNullable = false)(pos)
  private val float32: CypherType = Float32Type(isNullable = false)(pos)
  private val float16: CypherType = Float16Type(isNullable = false)(pos)
  private val bfloat16: CypherType = BFloat16Type(isNullable = false)(pos)

  // All Vector coordinate types are accepted (Cypher 25)
  val vectorCoordinateTypes: Seq[(String, CypherType)] = Seq(
    ("INT", int64),
    ("INT!", int64),
    ("INT NOT NULL", int64),
    ("INT64", int64),
    ("INT64!", int64),
    ("INT64 NOT NULL", int64),
    ("SIGNED INTEGER", int64),
    ("SIGNED INTEGER!", int64),
    ("SIGNED INTEGER NOT NULL", int64),
    ("INTEGER", int64),
    ("INTEGER!", int64),
    ("INTEGER NOT NULL", int64),
    ("INTEGER64", int64),
    ("INTEGER64!", int64),
    ("INTEGER64 NOT NULL", int64),
    ("INTEGER32", int32),
    ("INTEGER32!", int32),
    ("INTEGER32 NOT NULL", int32),
    ("INT32", int32),
    ("INT32!", int32),
    ("INT32 NOT NULL", int32),
    ("INTEGER16", int16),
    ("INTEGER16!", int16),
    ("INTEGER16 NOT NULL", int16),
    ("INT16", int16),
    ("INT16!", int16),
    ("INT16 NOT NULL", int16),
    ("INTEGER8", int8),
    ("INTEGER8!", int8),
    ("INTEGER8 NOT NULL", int8),
    ("INT8", int8),
    ("INT8!", int8),
    ("INT8 NOT NULL", int8),
    ("FLOAT", float64),
    ("FLOAT!", float64),
    ("FLOAT NOT NULL", float64),
    ("FLOAT64", float64),
    ("FLOAT64!", float64),
    ("FLOAT64 NOT NULL", float64),
    ("FLOAT32", float32),
    ("FLOAT32!", float32),
    ("FLOAT32 NOT NULL", float32),
    ("FLOAT16 NOT NULL", float16),
    ("FLOAT16", float16),
    ("FLOAT16!", float16),
    ("BFLOAT16 NOT NULL", bfloat16),
    ("BFLOAT16", bfloat16),
    ("BFLOAT16!", bfloat16)
  )

  vectorCoordinateTypes.foreach { case (typeName, cypherType) =>
    test(s"VECTOR(foo, 2, ${typeName})") {
      parsesIn[Expression] {
        case Cypher5 if typeName.contains("!") || typeName.contains(" ") =>
          _.withMessageStart("Invalid input")
        case Cypher5 => _.toAstIgnorePos { function("vector", varFor("foo"), literalInt(2), varFor(typeName)) }
        case Cypher25 =>
          _.toAstIgnorePos {
            VectorValueConstructor(varFor("foo"), literalInt(2), cypherType)(pos)
          }
      }
    }
    test(s"RETURN my.own.vector(foo, 2, $typeName)") {
      parsesIn[Statements] {
        case _ if typeName.contains("!") || typeName.contains(" ") =>
          _.withMessageStart("Invalid input")
        case _ => _.toAstIgnorePos {
            Statements(Seq(singleQuery(return_(returnItem(
              function(Seq("my", "own"), "vector", varFor("foo"), literalInt(2), varFor(typeName)),
              s"my.own.vector(foo, 2, $typeName)"
            )))))
          }
      }
    }
  }

  test("vector with namespace") {
    "RETURN my.own.vector('1', 2, FLOAT)" should parseTo[Statements] {
      Statements(Seq(singleQuery(return_(returnItem(
        function(Seq("my", "own"), "vector", literal("1"), literal(2), varFor("FLOAT")),
        "my.own.vector('1', 2, FLOAT)"
      )))))
    }
  }

  // Failing tests
  test("RETURN vector(foo, 2, STRING)") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstIgnorePos {
          Statements(Seq(singleQuery(return_(returnItem(
            function("vector", varFor("foo"), literalInt(2), varFor("STRING")),
            "vector(foo, 2, STRING)"
          )))))
        }
      case Cypher25 =>
        _.withSyntaxErrorContaining(
          """Invalid vector inner type, expected INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32, FLOAT16 or BFLOAT16 (line 1, column 23 (offset: 22))
            |"RETURN vector(foo, 2, STRING)"
            |                       ^""".stripMargin,
          GqlStatusInfoCodes.STATUS_42I53,
          "error: syntax error or access rule violation - unsupported coordinate type. Unknown coordinate type: 'STRING'.",
          position = Some(InputPosition(22, 1, 23))
        )
    }
  }

  test("RETURN vector(foo, 2, null)") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstIgnorePos {

          Statements(Seq(singleQuery(return_(returnItem(
            function("vector", varFor("foo"), literalInt(2), nullLiteral),
            "vector(foo, 2, null)"
          )))))
        }
      case Cypher25 =>
        _.withSyntaxErrorContaining(
          """Invalid vector inner type, expected INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32, FLOAT16 or BFLOAT16 (line 1, column 23 (offset: 22))
            |"RETURN vector(foo, 2, null)"
            |                       ^""".stripMargin,
          GqlStatusInfoCodes.STATUS_42I53,
          "error: syntax error or access rule violation - unsupported coordinate type. Unknown coordinate type: 'NULL'.",
          position = Some(InputPosition(22, 1, 23))
        )
    }
  }

  test("RETURN vector(foo, 2, INTEGE)") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstIgnorePos {
          Statements(Seq(singleQuery(return_(returnItem(
            function("vector", varFor("foo"), literalInt(2), varFor("INTEGE")),
            "vector(foo, 2, INTEGE)"
          )))))
        }
      case Cypher25 =>
        _.withSyntaxErrorContaining(
          """Invalid vector inner type, expected INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32, FLOAT16 or BFLOAT16 (line 1, column 23 (offset: 22))
            |"RETURN vector(foo, 2, INTEGE)"
            |                       ^""".stripMargin,
          GqlStatusInfoCodes.STATUS_42I53,
          "error: syntax error or access rule violation - unsupported coordinate type. Unknown coordinate type: 'INTEGE'.",
          position = Some(InputPosition(22, 1, 23))
        )
    }
  }

  test("vector(\"hello\", 2, FLOAT, anotherVar)") {
    parsesIn[Expression] {
      case _ => _.withoutErrors
    }
  }

  test("vector()") {
    parsesIn[Expression] {
      case _ => _.withoutErrors
    }
  }
}
