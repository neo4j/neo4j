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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap

/* Tests for creating and dropping indexes */
class IndexCommandsParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Create node index (old syntax)

  test("CREATE INDEX ON :Person(name)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(
          "Invalid create index syntax, use `CREATE INDEX FOR ...` instead.",
          testName,
          InputPosition(13, 1, 14)
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 17 (offset: 16))
            |"CREATE INDEX ON :Person(name)"
            |                 ^""".stripMargin
        )
    }
  }

  test("CREATE INDEX ON :Person(name,age)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(
          "Invalid create index syntax, use `CREATE INDEX FOR ...` instead.",
          testName,
          InputPosition(13, 1, 14)
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 17 (offset: 16))
            |"CREATE INDEX ON :Person(name,age)"
            |                 ^""".stripMargin
        )
    }
  }

  test("CREATE INDEX my_index ON :Person(name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE INDEX my_index ON :Person(name)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE INDEX my_index ON :Person(name,age)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE INDEX my_index ON :Person(name,age)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE OR REPLACE INDEX ON :Person(name)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """'REPLACE' is not allowed for this index syntax (line 1, column 11 (offset: 10))
            |"CREATE OR REPLACE INDEX ON :Person(name)"
            |           ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 28 (offset: 27))
            |"CREATE OR REPLACE INDEX ON :Person(name)"
            |                            ^""".stripMargin
        )
    }
  }

  // Create index

  test("CrEATe INDEX FOR (n1:Person) ON (n2.name)") {
    assertAst(
      rangeNodeIndex(
        List(prop("n2", "name")),
        None,
        posN2(testName),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromDefault = true
      )(pos),
      comparePosition = false
    )
  }

  // default type loop
  Seq(
    ("(n1:Person)", rangeNodeIndex: CreateRangeIndexFunction),
    ("()-[n1:R]-()", rangeRelIndex: CreateRangeIndexFunction),
    ("()-[n1:R]->()", rangeRelIndex: CreateRangeIndexFunction),
    ("()<-[n1:R]-()", rangeRelIndex: CreateRangeIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateRangeIndexFunction) =>
      test(s"CREATE INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE INDEX FOR $pattern ON (n2.name)") {
        assertAstVersionBased(
          fromCypher5 =>
            createIndex(
              List(prop("n2", "name")),
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions,
              true
            ).withGraph(Some(use(List("neo4j"), !fromCypher5))),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX ON FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("ON"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0'}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        // will fail in options converter
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("native-btree-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'native-btree-1.0'}"
      ) {
        // will fail in options converter
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("native-btree-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos),
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX $$my_index FOR $pattern ON (n.name)") {
        assertAst(
          createIndex(
            List(prop("n", "name")),
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE INDEX FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(defaultPos)
        )
      }

      test(s"CREATE INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            Some("my_index"),
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          )(defaultPos)
        )
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            true
          )(defaultPos)
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) {indexProvider : 'range-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }
  }

  // range loop
  Seq(
    ("(n1:Person)", rangeNodeIndex: CreateRangeIndexFunction),
    ("()-[n1:R]-()", rangeRelIndex: CreateRangeIndexFunction),
    ("()-[n1:R]->()", rangeRelIndex: CreateRangeIndexFunction),
    ("()<-[n1:R]-()", rangeRelIndex: CreateRangeIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateRangeIndexFunction) =>
      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE RANGE INDEX FOR $pattern ON (n2.name)") {
        assertAstVersionBased(
          fromCypher5 =>
            createIndex(
              List(prop("n2", "name")),
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions,
              false
            ).withGraph(Some(use(List("neo4j"), !fromCypher5))),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0'}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0', indexConfig : {someConfig: 'toShowItCanBeParsed'}}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("range-1.0"),
              "indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))
            ))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed'}, indexProvider : 'range-1.0'}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("range-1.0"),
              "indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))
            ))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {}}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf()))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos),
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX $$my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(defaultPos)
        )
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            pos,
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            pos,
            ast.IfExistsInvalidSyntax,
            ast.NoOptions,
            false
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) {indexProvider : 'range-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }
  }

  // btree loop (fails with nice error in Cypher 5 and just fails to parse in Cypher 25)
  Seq(
    "(n1:Person)",
    "()-[n1:R]-()",
    "()-[n1:R]->()",
    "()<-[n1:R]-()"
  ).foreach { pattern =>
    test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"USE neo4j CREATE BTREE INDEX FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name, n3.age)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"CREATE OR REPLACE BTREE INDEX FOR $pattern ON (n2.name)") {
      assertFailsOnBtree(true)
    }

    test(s"CREATE OR REPLACE BTREE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
      assertFailsOnBtree(true)
    }

    test(s"CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
      assertFailsOnBtree(true)
    }

    test(s"CREATE OR REPLACE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
      assertFailsOnBtree(true)
    }

    test(s"CREATE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
      assertFailsOnBtree()
    }

    test(
      s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
    ) {
      assertFailsOnBtree()
    }

    test(
      s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'native-btree-1.0'}"
    ) {
      assertFailsOnBtree()
    }

    test(
      s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
    ) {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX $$my_index FOR $pattern ON (n2.name)") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON n2.name") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX my_index FOR $pattern ON n2.name") {
      assertFailsOnBtree()
    }

    test(s"CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
      assertFailsOnBtree(true)
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) {indexProvider : 'native-btree-1.0'}") {
      assertFailsOnBtree()
    }

    test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS") {
      failsParsing[ast.Statements].in {
        case Cypher5 => _.withMessageStart("Invalid input ''")
        case _       => _.withSyntaxErrorContaining("Invalid input 'INDEX': expected a graph pattern")
      }
    }
  }

  // lookup loop
  Seq(
    ("(n1)", "labels(n2)", lookupNodeIndex: CreateLookupIndexFunction),
    ("()-[r1]-()", "type(r2)", lookupRelIndex: CreateLookupIndexFunction),
    ("()-[r1]->()", "type(r2)", lookupRelIndex: CreateLookupIndexFunction),
    ("()<-[r1]-()", "type(r2)", lookupRelIndex: CreateLookupIndexFunction)
  ).foreach {
    case (pattern, function, createIndex: CreateLookupIndexFunction) =>
      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        assertAstVersionBased(
          fromCypher5 =>
            createIndex(None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions)
              .withGraph(Some(use(List("neo4j"), !fromCypher5))),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX `$$my_index` FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(Some("$my_index"), posN2(testName), ast.IfExistsThrowError, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(None, posN2(testName), ast.IfExistsReplace, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(Some("my_index"), posN2(testName), ast.IfExistsReplace, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(None, posN2(testName), ast.IfExistsDoNothing, ast.NoOptions)(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS {anyOption : 42}") {
        assertAst(
          createIndex(
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("anyOption" -> literalInt(42)))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function OPTIONS {}") {
        assertAst(
          createIndex(
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX $$my_index FOR $pattern ON EACH $function") {
        assertAst(
          createIndex(
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function {indexProvider : 'range-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }
  }

  // fulltext loop
  Seq(
    ("(n1:Person)", true, List("Person")),
    ("(n1:Person|Colleague|Friend)", true, List("Person", "Colleague", "Friend")),
    ("()-[n1:R]->()", false, List("R")),
    ("()<-[n1:R|S]-()", false, List("R", "S"))
  ).foreach {
    case (pattern, isNodeIndex: Boolean, labelsOrTypes: List[String]) =>
      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name]") {
        assertAstVersionBased(
          fromCypher5 =>
            fulltextIndex(
              isNodeIndex,
              List(prop("n2", "name")),
              labelsOrTypes,
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions
            ).withGraph(Some(use(List("neo4j"), !fromCypher5))),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name, n3.age]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name, n3.age]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX `$$my_index` FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name, n3.age]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n2.name, n3.age]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexProvider : 'fulltext-1.0'}") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("fulltext-1.0")))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.analyzer`: 'some_analyzer'}}"
      ) {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("fulltext-1.0"),
              "indexConfig" -> mapOf("fulltext.analyzer" -> literalString("some_analyzer"))
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}, indexProvider : 'fulltext-1.0'}"
      ) {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("fulltext-1.0"),
              "indexConfig" -> mapOf("fulltext.eventually_consistent" -> falseLiteral)
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true}}"
      ) {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf(
              "fulltext.analyzer" -> literalString("some_analyzer"),
              "fulltext.eventually_consistent" -> trueLiteral
            )))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {nonValidOption : 42}") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name] OPTIONS {}") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name] OPTIONS $$options") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX $$my_index FOR $pattern ON EACH [n2.name]") {
        assertAst(
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] {indexProvider : 'fulltext-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH (n2.name)") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '('")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH n2.name") {
        failsParsing[ast.Statements].withMessageStart("Invalid input 'n2'")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH []") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ']'")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON [n2.name]") {
        failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '[': expected 'EACH'")
      }

      test(s"CREATE INDEX FOR $pattern ON EACH [n2.name]") {
        failsParsing[ast.Statements]
          // different failures depending on pattern
          .withMessageStart("Invalid input")
      }

      // Missing escaping around `fulltext.analyzer`
      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {fulltext.analyzer: 'some_analyzer'}}"
      ) {
        failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '.': expected ':'")
      }
  }

  // text loop
  Seq(
    ("(n1:Person)", textNodeIndex: CreateIndexFunction),
    ("()-[n1:R]-()", textRelIndex: CreateIndexFunction),
    ("()-[n1:R]->()", textRelIndex: CreateIndexFunction),
    ("()<-[n1:R]-()", textRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE TEXT INDEX FOR $pattern ON (n2.name)") {
        assertAstVersionBased(
          fromCypher5 =>
            createIndex(
              List(prop("n2", "name")),
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions
            ).withGraph(
              Some(use(List("neo4j"), !fromCypher5))
            ),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE TEXT INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'text-1.0'}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'text-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("text-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'text-1.0'}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("text-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX $$my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos)
        )
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            Some("my_index"),
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos)
        )
      }

      test(s"CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(defaultPos)
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n.name, n.age") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ','")
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) {indexProvider : 'text-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }
  }

  // point loop
  Seq(
    ("(n1:Person)", pointNodeIndex: CreateIndexFunction),
    ("()-[n1:R]-()", pointRelIndex: CreateIndexFunction),
    ("()-[n1:R]->()", pointRelIndex: CreateIndexFunction),
    ("()<-[n1:R]-()", pointRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"USE neo4j CREATE POINT INDEX FOR $pattern ON (n2.name)") {
        assertAstVersionBased(
          fromCypher5 =>
            createIndex(
              List(prop("n2", "name")),
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions
            ).withGraph(
              Some(use(List("neo4j"), !fromCypher5))
            ),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE POINT INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE POINT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE POINT INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE POINT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          createIndex(
            List(prop("n2", "name"), prop("n3", "age")),
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'point-1.0'}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("point-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'point-1.0'}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("point-1.0"),
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
      ) {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos)
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX $$my_index FOR $pattern ON (n.name)") {
        assertAst(
          createIndex(
            List(prop("n", "name")),
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos)
        )
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            Some("my_index"),
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos)
        )
      }

      test(s"CREATE OR REPLACE POINT INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name", posN2(testName))),
            None,
            posN1(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(defaultPos)
        )
      }
      test(s"CREATE POINT INDEX FOR $pattern ON n2.name, n3.age") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ','")
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n.name) {indexProvider : 'point-1.0'}") {
        failsParsing[ast.Statements].withMessageStart("Invalid input '{'")
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsParsing[ast.Statements].withMessageStart("Invalid input ''")
      }
  }

  // vector loop
  Seq(
    ("(n1:Person)", true, List("Person"), true),
    ("(n1:Person|Colleague|Friend)", true, List("Person", "Colleague", "Friend"), false),
    ("()-[n1:R]-()", false, List("R"), true),
    ("()-[n1:R|S]->()", false, List("R", "S"), false)
  ).foreach {
    case (pattern, isNodeIndex: Boolean, labelsOrTypes: List[String], availableInCypher5: Boolean) =>
      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"USE neo4j CREATE VECTOR INDEX FOR $pattern ON (n2.name)") {
        assertAstVersionBased(
          fromCypher5 =>
            vectorIndex(
              isNodeIndex,
              List(prop("n2", "name")),
              List.empty,
              labelsOrTypes,
              None,
              posN2(testName),
              ast.IfExistsThrowError,
              ast.NoOptions
            ).withGraph(
              Some(use(List("neo4j"), !fromCypher5))
            ),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) WITH [n3.prop1, n4.prop2]") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List(prop("n3", "prop1"), prop("n4", "prop2")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = false
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            Some("$my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name) WITH [n3.prop1, n4.prop2, n5.prop3, n6.prop1]") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List(prop("n3", "prop1"), prop("n4", "prop2"), prop("n5", "prop3"), prop("n6", "prop1")),
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = false
        )
      }

      test(s"CREATE OR REPLACE VECTOR INDEX FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE OR REPLACE VECTOR INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsReplace,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE OR REPLACE VECTOR INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE OR REPLACE VECTOR INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name"), prop("n3", "age")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX IF NOT EXISTS FOR $pattern ON (n2.name) WITH [n3.prop1]") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List(prop("n3", "prop1")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = false
        )
      }

      test(s"CREATE VECTOR INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsDoNothing,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'vector-1.0'}") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("vector-1.0")))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'vector-1.0', indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'euclidean' }}"
      ) {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("vector-1.0"),
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(50),
                "vector.similarity_function" -> literalString("euclidean")
              )
            ))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'cosine' }, indexProvider : 'vector-1.0'}"
      ) {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexProvider" -> literalString("vector-1.0"),
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(50),
                "vector.similarity_function" -> literalString("cosine")
              )
            ))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'cosine' }}"
      ) {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(50),
              "vector.similarity_function" -> literalString("cosine")
            )))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsParam(parameter("options", CTMap))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map.empty)(pos)
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX $$my_index FOR $pattern ON (n.name)") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n", "name")),
            List.empty,
            labelsOrTypes,
            Some(stringParam("my_index")),
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
          comparePosition = false,
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON n2.name") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name", posN2(testName))),
            List.empty,
            labelsOrTypes,
            None,
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos),
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name", posN2(testName))),
            List.empty,
            labelsOrTypes,
            Some("my_index"),
            posN1(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(defaultPos),
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE OR REPLACE VECTOR INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          vectorIndex(
            isNodeIndex,
            List(prop("n2", "name", posN2(testName))),
            List.empty,
            labelsOrTypes,
            None,
            posN1(testName),
            ast.IfExistsInvalidSyntax,
            ast.NoOptions
          )(defaultPos),
          supportedInCypher5 = availableInCypher5
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON n2.name, n3.age") {
        failsParsing[ast.Statements].in {
          case Cypher5 if !availableInCypher5 => _.withSyntaxErrorContaining("Invalid input")
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input ',': expected 'OPTIONS' or <EOF>")
          case _       => _.withSyntaxErrorContaining("Invalid input ',': expected 'OPTIONS', 'WITH' or <EOF> ")
        }
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n.name) {indexProvider : 'vector-1.0'}") {
        failsParsing[ast.Statements].in {
          case Cypher5 if !availableInCypher5 => _.withSyntaxErrorContaining("Invalid input")
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input '{': expected 'OPTIONS' or <EOF>")
          case _       => _.withSyntaxErrorContaining("Invalid input '{': expected 'OPTIONS', 'WITH' or <EOF>")

        }
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsParsing[ast.Statements].in {
          case Cypher5 if !availableInCypher5 => _.withSyntaxErrorContaining("Invalid input")
          case _                              => _.withSyntaxErrorContaining("Invalid input ''")
        }
      }
  }

  test("CREATE INDEX $ FOR (n1:Label) ON (n2.name)") {
    // Missing parameter name (or backticks)
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 20 (offset: 19))
        |"CREATE INDEX $ FOR (n1:Label) ON (n2.name)"
        |                    ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR (x1) ON EACH labels(x2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("x1"),
        isNodeIndex = true,
        function(Labels.name, varFor("x2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[x1]-() ON EACH type(x2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("x1"),
        isNodeIndex = false,
        function(Type.name, varFor("x2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON EACH count(n2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("n1"),
        isNodeIndex = true,
        function(Count.name, varFor("n2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON EACH type(n2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("n1"),
        isNodeIndex = true,
        function(Type.name, varFor("n2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH labels(x)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("n"),
        isNodeIndex = true,
        function(Labels.name, varFor("x")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON EACH count(r2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("r1"),
        isNodeIndex = false,
        function(Count.name, varFor("r2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON EACH labels(r2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("r1"),
        isNodeIndex = false,
        function(Labels.name, varFor("r2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(x)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("r"),
        isNodeIndex = false,
        function(Type.name, varFor("x")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON type(r2)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("r1"),
        isNodeIndex = false,
        function(Type.name, varFor("r2")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR (x) ON EACH EACH(x)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("x"),
        isNodeIndex = true,
        function("EACH", varFor("x")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)") {
    assertAst(
      ast.CreateIndex.createLookupIndex(
        varFor("x"),
        isNodeIndex = false,
        function("EACH", varFor("x")),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos),
      comparePosition = false
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)") {
    // Thinks it is missing the function name since `EACH` is parsed as keyword
    failsParsing[ast.Statements].withSyntaxError(
      """Missing function name for the LOOKUP INDEX (line 1, column 42 (offset: 41))
        |"CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)"
        |                                          ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH my.labels(n)") {
    failsParsing[ast.Statements].withMessageStart("Invalid input '.'")
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH my.type(r)") {
    failsParsing[ast.Statements].withMessageStart("Invalid input '.'")
  }

  test("CREATE INDEX FOR n1:Person ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n1': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 18 (offset: 17))
        |"CREATE INDEX FOR n1:Person ON (n2.name)"
        |                  ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 21 (offset: 20))
        |"CREATE INDEX FOR (n1) ON (n2.name)"
        |                     ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 24 (offset: 23))
        |"CREATE INDEX FOR ()-[n1]-() ON (n2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 18 (offset: 17))
        |"CREATE INDEX FOR -[r1:R]-() ON (r2.name)"
        |                  ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected '(' or '>' (line 1, column 29 (offset: 28))
        |"CREATE INDEX FOR ()-[r1:R]- ON (r2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR -[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 18 (offset: 17))
        |"CREATE INDEX FOR -[r1:R]- ON (r2.name)"
        |                  ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR [r1:R] ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '[': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 18 (offset: 17))
        |"CREATE INDEX FOR [r1:R] ON (r2.name)"
        |                  ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 19 (offset: 18))
        |"CREATE INDEX FOR (:A)-[n1:R]-() ON (n2.name)"
        |                   ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 29 (offset: 28))
        |"CREATE INDEX FOR ()-[n1:R]-(:A) ON (n2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 21 (offset: 20))
        |"CREATE INDEX FOR (n2)-[n1:R]-() ON (n2.name)"
        |                     ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 29 (offset: 28))
        |"CREATE INDEX FOR ()-[n1:R]-(n2) ON (n2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected 'ON' (line 1, column 24 (offset: 23))
        |"CREATE INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 29 (offset: 28))
        |"CREATE INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR n1:Person ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n1': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE TEXT INDEX FOR n1:Person ON (n2.name)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 26 (offset: 25))
        |"CREATE TEXT INDEX FOR (n1) ON (n2.name)"
        |                          ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 29 (offset: 28))
        |"CREATE TEXT INDEX FOR ()-[n1]-() ON (n2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE TEXT INDEX FOR -[r1:R]-() ON (r2.name)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR ()-[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected '(' or '>' (line 1, column 34 (offset: 33))
        |"CREATE TEXT INDEX FOR ()-[r1:R]- ON (r2.name)"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR -[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE TEXT INDEX FOR -[r1:R]- ON (r2.name)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR [r1:R] ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '[': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 23 (offset: 22))
        |"CREATE TEXT INDEX FOR [r1:R] ON (r2.name)"
        |                       ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 24 (offset: 23))
        |"CREATE TEXT INDEX FOR (:A)-[n1:R]-() ON (n2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE TEXT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 26 (offset: 25))
        |"CREATE TEXT INDEX FOR (n2)-[n1:R]-() ON (n2.name)"
        |                          ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE TEXT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected 'ON' (line 1, column 29 (offset: 28))
        |"CREATE TEXT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)"
        |                             ^""".stripMargin
    )
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE TEXT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR n1:Person ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n1': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 24 (offset: 23))
        |"CREATE POINT INDEX FOR n1:Person ON (n2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 27 (offset: 26))
        |"CREATE POINT INDEX FOR (n1) ON (n2.name)"
        |                           ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 30 (offset: 29))
        |"CREATE POINT INDEX FOR ()-[n1]-() ON (n2.name)"
        |                              ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 24 (offset: 23))
        |"CREATE POINT INDEX FOR -[r1:R]-() ON (r2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR ()-[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected '(' or '>' (line 1, column 35 (offset: 34))
        |"CREATE POINT INDEX FOR ()-[r1:R]- ON (r2.name)"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR -[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 24 (offset: 23))
        |"CREATE POINT INDEX FOR -[r1:R]- ON (r2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR [r1:R] ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '[': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 24 (offset: 23))
        |"CREATE POINT INDEX FOR [r1:R] ON (r2.name)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 25 (offset: 24))
        |"CREATE POINT INDEX FOR (:A)-[n1:R]-() ON (n2.name)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 35 (offset: 34))
        |"CREATE POINT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 27 (offset: 26))
        |"CREATE POINT INDEX FOR (n2)-[n1:R]-() ON (n2.name)"
        |                           ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 35 (offset: 34))
        |"CREATE POINT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected 'ON' (line 1, column 30 (offset: 29))
        |"CREATE POINT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)"
        |                              ^""".stripMargin
    )
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 35 (offset: 34))
        |"CREATE POINT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR n1:Person ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n1': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE VECTOR INDEX FOR n1:Person ON (n2.name)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 28 (offset: 27))
        |"CREATE VECTOR INDEX FOR (n1) ON (n2.name)"
        |                            ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR (n1:Person|) ON (n2.name)") {
    // missing second label
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input ')': expected an identifier (line 1, column 36 (offset: 35))
            |"CREATE VECTOR INDEX FOR (n1:Person|) ON (n2.name)"
            |                                    ^""".stripMargin
        )
    }

  }

  test("CREATE VECTOR INDEX FOR (n1:Person) ON (n2.name) WITH") {
    // missing additional properties
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input '': expected '[' (line 1, column 54 (offset: 53))
            |"CREATE VECTOR INDEX FOR (n1:Person) ON (n2.name) WITH"
            |                                                      ^""".stripMargin
        )
    }
  }

  test("CREATE VECTOR INDEX FOR (n1:Person) ON (n2.name) WITH []") {
    // with empty additional properties list
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected a variable name (line 1, column 56 (offset: 55))
            |"CREATE VECTOR INDEX FOR (n1:Person) ON (n2.name) WITH []"
            |                                                        ^""".stripMargin
        )
    }
  }

  test("CREATE VECTOR INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 31 (offset: 30))
        |"CREATE VECTOR INDEX FOR ()-[n1]-() ON (n2.name)"
        |                               ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE VECTOR INDEX FOR -[r1:R]-() ON (r2.name)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR ()-[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected '(' or '>' (line 1, column 36 (offset: 35))
        |"CREATE VECTOR INDEX FOR ()-[r1:R]- ON (r2.name)"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR -[r1:R]- ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE VECTOR INDEX FOR -[r1:R]- ON (r2.name)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR [r1:R] ON (r2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '[': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE VECTOR INDEX FOR [r1:R] ON (r2.name)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 26 (offset: 25))
        |"CREATE VECTOR INDEX FOR (:A)-[n1:R]-() ON (n2.name)"
        |                          ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 36 (offset: 35))
        |"CREATE VECTOR INDEX FOR ()-[n1:R]-(:A) ON (n2.name)"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 28 (offset: 27))
        |"CREATE VECTOR INDEX FOR (n2)-[n1:R]-() ON (n2.name)"
        |                            ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 36 (offset: 35))
        |"CREATE VECTOR INDEX FOR ()-[n1:R]-(n2) ON (n2.name)"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected 'ON' (line 1, column 31 (offset: 30))
        |"CREATE VECTOR INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)"
        |                               ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 36 (offset: 35))
        |"CREATE VECTOR INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R|]-() ON (n2.name)") {
    // missing second relationship type
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected an identifier (line 1, column 34 (offset: 33))
            |"CREATE VECTOR INDEX FOR ()-[n1:R|]-() ON (n2.name)"
            |                                  ^""".stripMargin
        )
    }

  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-() ON (n2.name) WITH") {
    // missing additional properties
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input '': expected '[' (line 1, column 55 (offset: 54))
            |"CREATE VECTOR INDEX FOR ()-[n1:R]-() ON (n2.name) WITH"
            |                                                       ^""".stripMargin
        )
    }
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-() ON (n2.name) WITH []") {
    // with empty additional properties list
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected a variable name (line 1, column 57 (offset: 56))
            |"CREATE VECTOR INDEX FOR ()-[n1:R]-() ON (n2.name) WITH []"
            |                                                         ^""".stripMargin
        )
    }
  }

  test("CREATE LOOKUP INDEX FOR n1 ON EACH labels(n2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n1': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE LOOKUP INDEX FOR n1 ON EACH labels(n2)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR -[r1]-() ON EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE LOOKUP INDEX FOR -[r1]-() ON EACH type(r2)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]- ON EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected '>' or '(' (line 1, column 34 (offset: 33))
        |"CREATE LOOKUP INDEX FOR ()-[r1]- ON EACH type(r2)"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR -[r1]- ON EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE LOOKUP INDEX FOR -[r1]- ON EACH type(r2)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR [r1] ON EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '[': expected '(', 'IF NOT EXISTS' or 'FOR' (line 1, column 25 (offset: 24))
        |"CREATE LOOKUP INDEX FOR [r1] ON EACH type(r2)"
        |                         ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR (n1) EACH labels(n1)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'EACH': expected 'ON EACH' (line 1, column 30 (offset: 29))
        |"CREATE LOOKUP INDEX FOR (n1) EACH labels(n1)"
        |                              ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'EACH': expected 'ON' (line 1, column 36 (offset: 35))
        |"CREATE LOOKUP INDEX FOR ()-[r1]-() EACH type(r2)"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON labels(n2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'labels': expected 'EACH' (line 1, column 33 (offset: 32))
        |"CREATE LOOKUP INDEX FOR (n1) ON labels(n2)"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR (n1) ON EACH labels(n2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 21 (offset: 20))
        |"CREATE INDEX FOR (n1) ON EACH labels(n2)"
        |                     ^""".stripMargin
    )
  }

  test("CREATE INDEX FOR ()-[r1]-() ON EACH type(r2)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 24 (offset: 23))
        |"CREATE INDEX FOR ()-[r1]-() ON EACH type(r2)"
        |                        ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1) ON EACH [n2.x]") {
    // missing label
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 30 (offset: 29))
        |"CREATE FULLTEXT INDEX FOR (n1) ON EACH [n2.x]"
        |                              ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1]-() ON EACH [n2.x]") {
    // missing relationship type
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ']': expected ':' (line 1, column 33 (offset: 32))
        |"CREATE FULLTEXT INDEX FOR ()-[n1]-() ON EACH [n2.x]"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1|:A) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '|': expected ':' (line 1, column 30 (offset: 29))
        |"CREATE FULLTEXT INDEX FOR (n1|:A) ON EACH [n2.x]"
        |                              ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1|:R]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '|': expected ':' (line 1, column 33 (offset: 32))
        |"CREATE FULLTEXT INDEX FOR ()-[n1|:R]-() ON EACH [n2.x]"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A|:B) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected an identifier (line 1, column 33 (offset: 32))
        |"CREATE FULLTEXT INDEX FOR (n1:A|:B) ON EACH [n2.x]"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R|:S]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected an identifier (line 1, column 36 (offset: 35))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R|:S]-() ON EACH [n2.x]"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A||B) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '||': expected ')' or '|' (line 1, column 32 (offset: 31))
        |"CREATE FULLTEXT INDEX FOR (n1:A||B) ON EACH [n2.x]"
        |                                ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R||S]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '||': expected ']' or '|' (line 1, column 35 (offset: 34))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R||S]-() ON EACH [n2.x]"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A:B) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' or '|' (line 1, column 32 (offset: 31))
        |"CREATE FULLTEXT INDEX FOR (n1:A:B) ON EACH [n2.x]"
        |                                ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R:S]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ']' or '|' (line 1, column 35 (offset: 34))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R:S]-() ON EACH [n2.x]"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A&B) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '&': expected ')' or '|' (line 1, column 32 (offset: 31))
        |"CREATE FULLTEXT INDEX FOR (n1:A&B) ON EACH [n2.x]"
        |                                ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R&S]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '&': expected ']' or '|' (line 1, column 35 (offset: 34))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R&S]-() ON EACH [n2.x]"
        |                                   ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A B) ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'B': expected ')' or '|' (line 1, column 33 (offset: 32))
        |"CREATE FULLTEXT INDEX FOR (n1:A B) ON EACH [n2.x]"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R S]-() ON EACH [n2.x]") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'S': expected ']' or '|' (line 1, column 36 (offset: 35))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R S]-() ON EACH [n2.x]"
        |                                    ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (:A)-[n1:R]-() ON EACH [n2.name]") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 28 (offset: 27))
        |"CREATE FULLTEXT INDEX FOR (:A)-[n1:R]-() ON EACH [n2.name]"
        |                            ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(:A) ON EACH [n2.name]") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 38 (offset: 37))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R]-(:A) ON EACH [n2.name]"
        |                                      ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n2)-[n1:R]-() ON EACH [n2.name]") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 30 (offset: 29))
        |"CREATE FULLTEXT INDEX FOR (n2)-[n1:R]-() ON EACH [n2.name]"
        |                              ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2) ON EACH [n2.name]") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 38 (offset: 37))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2) ON EACH [n2.name]"
        |                                      ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR (n2:A)-[n1:R]-() ON EACH [n2.name]") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '-': expected 'ON EACH' (line 1, column 33 (offset: 32))
        |"CREATE FULLTEXT INDEX FOR (n2:A)-[n1:R]-() ON EACH [n2.name]"
        |                                 ^""".stripMargin
    )
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2:A) ON EACH [n2.name]") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 38 (offset: 37))
        |"CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2:A) ON EACH [n2.name]"
        |                                      ^""".stripMargin
    )
  }

  test("CREATE UNKNOWN INDEX FOR (n1:Person) ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'INDEX': expected a graph pattern (line 1, column 16 (offset: 15))
        |"CREATE UNKNOWN INDEX FOR (n1:Person) ON (n2.name)"
        |                ^""".stripMargin
    )
  }

  test("CREATE BUILT IN INDEX FOR (n1:Person) ON (n2.name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'IN': expected a graph pattern (line 1, column 14 (offset: 13))
        |"CREATE BUILT IN INDEX FOR (n1:Person) ON (n2.name)"
        |              ^""".stripMargin
    )
  }

  // Drop index

  test("DROP INDEX ON :Person(name)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(
          "Indexes cannot be dropped by schema, please drop by name instead: DROP INDEX index_name. The index name can be found using SHOW INDEXES.",
          testName,
          InputPosition(11, 1, 12)
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON :Person(name)"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON :Person(name, age)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(
          "Indexes cannot be dropped by schema, please drop by name instead: DROP INDEX index_name. The index name can be found using SHOW INDEXES.",
          testName,
          InputPosition(11, 1, 12)
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON :Person(name, age)"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX my_index") {
    assertAst(ast.DropIndexOnName("my_index", ifExists = false)(pos))
  }

  test("DROP INDEX `$my_index`") {
    assertAst(ast.DropIndexOnName("$my_index", ifExists = false)(pos))
  }

  test("DROP INDEX my_index IF EXISTS") {
    assertAst(ast.DropIndexOnName("my_index", ifExists = true)(pos))
  }

  test("DROP INDEX $my_index") {
    assertAst(
      ast.DropIndexOnName(stringParam("my_index"), ifExists = false)(pos),
      comparePosition = false
    )
  }

  test("DROP INDEX my_index ON :Person(name)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected 'IF EXISTS' or <EOF> (line 1, column 21 (offset: 20))
        |"DROP INDEX my_index ON :Person(name)"
        |                     ^""".stripMargin
    )
  }

  test("DROP INDEX ON (:Person(name))") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '(': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON (:Person(name))"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '(': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON (:Person(name))"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON (:Person {name})") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '(': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON (:Person {name})"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '(': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON (:Person {name})"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON [:Person(name)]") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '[': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON [:Person(name)]"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '[': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON [:Person(name)]"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON -[:Person(name)]-") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '-': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON -[:Person(name)]-"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON -[:Person(name)]-"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON ()-[:Person(name)]-()") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '(': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON ()-[:Person(name)]-()"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '(': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON ()-[:Person(name)]-()"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON [:Person {name}]") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '[': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON [:Person {name}]"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '[': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON [:Person {name}]"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON -[:Person {name}]-") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '-': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON -[:Person {name}]-"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON -[:Person {name}]-"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX ON ()-[:Person {name}]-()") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '(': expected ':', 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON ()-[:Person {name}]-()"
            |               ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '(': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON ()-[:Person {name}]-()"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP INDEX on IF EXISTS") {
    assertAst(ast.DropIndexOnName("on", ifExists = true)(pos))
  }

  test("DROP INDEX on") {
    assertAst(ast.DropIndexOnName("on", ifExists = false)(pos))
  }

  test("DROP INDEX ON :if(exists)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(
          "Indexes cannot be dropped by schema, please drop by name instead: DROP INDEX index_name. The index name can be found using SHOW INDEXES.",
          testName,
          InputPosition(11, 1, 12)
        )
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP INDEX ON :if(exists)"
            |               ^""".stripMargin
        )
    }
  }

  // help methods

  type CreateIndexFunction = (
    List[Property],
    Option[Expression],
    InputPosition,
    ast.IfExistsDo,
    ast.Options
  ) => InputPosition => ast.CreateIndex

  type CreateRangeIndexFunction = (
    List[Property],
    Option[Expression],
    InputPosition,
    ast.IfExistsDo,
    ast.Options,
    Boolean // fromDefault
  ) => InputPosition => ast.CreateIndex

  private def rangeNodeIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options,
    fromDefault: Boolean
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createRangeNodeIndex(
      varFor("n1", varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options,
      fromDefault
    )

  private def rangeRelIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options,
    fromDefault: Boolean
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createRangeRelationshipIndex(
      varFor("n1", varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options,
      fromDefault
    )

  type CreateLookupIndexFunction =
    (
      Option[Expression],
      InputPosition,
      ast.IfExistsDo,
      ast.Options
    ) => InputPosition => ast.CreateIndex

  private def lookupNodeIndex(
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createLookupIndex(
      varFor("n1", varPos),
      isNodeIndex = true,
      function(Labels.name, varFor("n2")),
      name,
      ifExistsDo,
      options
    )

  private def lookupRelIndex(
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createLookupIndex(
      varFor("r1", varPos),
      isNodeIndex = false,
      function(Type.name, varFor("r2")),
      name,
      ifExistsDo,
      options
    )

  private def fulltextIndex(
    isNodeIndex: Boolean,
    props: List[Property],
    labelOrTypes: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    if (isNodeIndex) {
      fulltextNodeIndex(props, labelOrTypes, name, varPos, ifExistsDo, options)
    } else {
      fulltextRelIndex(props, labelOrTypes, name, varPos, ifExistsDo, options)
    }

  private def fulltextNodeIndex(
    props: List[Property],
    labels: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createFulltextNodeIndex(
      varFor("n1", varPos),
      labels.map(labelName(_)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def fulltextRelIndex(
    props: List[Property],
    types: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createFulltextRelationshipIndex(
      varFor("n1", varPos),
      types.map(relTypeName(_)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def textNodeIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createTextNodeIndex(
      varFor("n1", varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def textRelIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createTextRelationshipIndex(
      varFor("n1", varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def pointNodeIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createPointNodeIndex(
      varFor("n1", varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def pointRelIndex(
    props: List[Property],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createPointRelationshipIndex(
      varFor("n1", varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def vectorIndex(
    isNodeIndex: Boolean,
    props: List[Property],
    additionalProps: List[Property],
    labelOrTypes: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    if (isNodeIndex) {
      vectorNodeIndex(props, additionalProps, labelOrTypes, name, varPos, ifExistsDo, options)
    } else {
      vectorRelIndex(props, additionalProps, labelOrTypes, name, varPos, ifExistsDo, options)
    }

  private def vectorNodeIndex(
    props: List[Property],
    additionalProps: List[Property],
    labels: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createVectorNodeIndex(
      varFor("n1", varPos),
      labels.map(labelName(_)),
      props,
      additionalProps,
      name,
      ifExistsDo,
      options
    )

  private def vectorRelIndex(
    props: List[Property],
    additionalProps: List[Property],
    types: List[String],
    name: Option[Expression],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateIndex.createVectorRelationshipIndex(
      varFor("n1", varPos),
      types.map(relTypeName(_)),
      props,
      additionalProps,
      name,
      ifExistsDo,
      options
    )

  private def pos(offset: Int): InputPosition = (1, offset + 1, offset)
  private def posN1(query: String): InputPosition = pos(query.indexOf("n1"))
  private def posN2(query: String): InputPosition = pos(query.indexOf("n2"))

  private def assertFailsOnBtree(failAsCreateCommand: Boolean = false) = {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax("Invalid index type b-tree, use range, point or text index instead.")
      case _ if failAsCreateCommand =>
        _.withSyntaxErrorContaining(
          "Invalid input 'BTREE': expected 'ALIAS', 'CONSTRAINT', 'DATABASE', 'COMPOSITE DATABASE', 'REPLICA DATABASE', 'IMMUTABLE', 'INDEX', " +
            "'FULLTEXT INDEX', 'LOOKUP INDEX', 'POINT INDEX', 'RANGE INDEX', 'TEXT INDEX', 'VECTOR INDEX', 'ROLE', 'AUTH RULE' or 'USER'"
        )
      case _ => // Since BTREE is no longer part of CREATE INDEX we fall back to regular Cypher CREATE (`CREATE btree=()-[]-()`)
        _.withSyntaxErrorContaining("Invalid input 'INDEX': expected a graph pattern")
    }
  }
}
