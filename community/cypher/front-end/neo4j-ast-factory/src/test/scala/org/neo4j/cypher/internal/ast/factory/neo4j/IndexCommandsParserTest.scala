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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap

/* Tests for creating and dropping indexes */
class IndexCommandsParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Create node index (old syntax)

  test("CREATE INDEX ON :Person(name)") {
    yields(ast.CreateIndexOldSyntax(labelName("Person"), List(propName("name"))))
  }

  test("CREATE INDEX ON :Person(name,age)") {
    yields(ast.CreateIndexOldSyntax(labelName("Person"), List(propName("name"), propName("age"))))
  }

  test("CREATE INDEX my_index ON :Person(name)") {
    failsToParse
  }

  test("CREATE INDEX my_index ON :Person(name,age)") {
    failsToParse
  }

  test("CREATE OR REPLACE INDEX ON :Person(name)") {
    assertFailsWithMessage(testName, "'REPLACE' is not allowed for this index syntax (line 1, column 1 (offset: 0))")
  }

  // Create index

  test("CrEATe INDEX FOR (n1:Person) ON (n2.name)") {
    yields(rangeNodeIndex(
      List(prop("n2", "name")),
      None,
      posN2(testName),
      ast.IfExistsThrowError,
      ast.NoOptions,
      fromDefault = true
    ))
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
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"USE neo4j CREATE INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            true
          ).withGraph(Some(use(varFor("neo4j"))))
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX ON FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("ON"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE OR REPLACE INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions, true))
      }

      test(s"CREATE OR REPLACE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
          true
        ))
      }

      test(
        s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        // will fail in options converter
        yields(createIndex(
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
          )),
          true
        ))
      }

      test(
        s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'native-btree-1.0'}"
      ) {
        // will fail in options converter
        yields(createIndex(
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
          )),
          true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))),
          true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap)),
          true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
          true
        ))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty),
          true
        ))
      }

      test(s"CREATE INDEX $$my_index FOR $pattern ON (n.name)") {
        yields(createIndex(
          List(prop("n", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        )(defaultPos))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          Some(Left("my_index")),
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          true
        )(defaultPos))
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          true
        )(defaultPos))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) {indexProvider : 'range-1.0'}") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
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
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"USE neo4j CREATE RANGE INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(
            List(prop("n2", "name")),
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          ).withGraph(Some(use(varFor("neo4j"))))
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE OR REPLACE RANGE INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions, false))
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE OR REPLACE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
          false
        ))
      }

      test(
        s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'range-1.0', indexConfig : {someConfig: 'toShowItCanBeParsed'}}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("range-1.0"),
            "indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))
          )),
          false
        ))
      }

      test(
        s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed'}, indexProvider : 'range-1.0'}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("range-1.0"),
            "indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))
          )),
          false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {}}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf())),
          false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap)),
          false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
          false
        ))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty),
          false
        ))
      }

      test(s"CREATE RANGE INDEX $$my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions,
          false
        )(defaultPos))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON n2.name") {
        assertAst(
          createIndex(
            List(prop("n2", "name")),
            Some(Left("my_index")),
            pos,
            ast.IfExistsThrowError,
            ast.NoOptions,
            false
          ),
          comparePosition = false
        )
      }

      test(s"CREATE OR REPLACE RANGE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(
          createIndex(List(prop("n2", "name")), None, pos, ast.IfExistsInvalidSyntax, ast.NoOptions, false),
          comparePosition = false
        )
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) {indexProvider : 'range-1.0'}") {
        failsToParse
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  // btree loop (will fail in semantic checking)
  Seq(
    ("(n1:Person)", btreeNodeIndex: CreateIndexFunction),
    ("()-[n1:R]-()", btreeRelIndex: CreateIndexFunction),
    ("()-[n1:R]->()", btreeRelIndex: CreateIndexFunction),
    ("()<-[n1:R]-()", btreeRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"USE neo4j CREATE BTREE INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions).withGraph(
            Some(use(varFor("neo4j")))
          )
        )
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE BTREE INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0")))
        ))
      }

      test(
        s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'native-btree-1.0'}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        ))
      }

      test(s"CREATE BTREE INDEX $$my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          Some(Left("my_index")),
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) {indexProvider : 'native-btree-1.0'}") {
        failsToParse
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
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
        yields(createIndex(None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"USE neo4j CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(_ =>
          createIndex(None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions)
            .withGraph(Some(use(varFor("neo4j"))))
        )
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some(Left("my_index")), posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"CREATE LOOKUP INDEX `$$my_index` FOR $pattern ON EACH $function") {
        yields(createIndex(Some("$my_index"), posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(createIndex(None, posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some(Left("my_index")), posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some(Left("my_index")), posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, posN2(testName), ast.IfExistsDoNothing, ast.NoOptions))
      }

      test(s"CREATE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some(Left("my_index")), posN2(testName), ast.IfExistsDoNothing, ast.NoOptions))
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS {anyOption : 42}") {
        yields(createIndex(
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("anyOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function OPTIONS {}") {
        yields(createIndex(Some(Left("my_index")), posN2(testName), ast.IfExistsThrowError, ast.OptionsMap(Map.empty)))
      }

      test(s"CREATE LOOKUP INDEX $$my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some(stringParam("my_index")), posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function {indexProvider : 'range-1.0'}") {
        failsToParse
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS") {
        failsToParse
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
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"USE neo4j CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name]") {
        yields(_ =>
          fulltextIndex(
            isNodeIndex,
            List(prop("n2", "name")),
            labelsOrTypes,
            None,
            posN2(testName),
            ast.IfExistsThrowError,
            ast.NoOptions
          ).withGraph(Some(use(varFor("neo4j"))))
        )
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name, n3.age]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name"), prop("n3", "age")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name, n3.age]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name"), prop("n3", "age")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX `$$my_index` FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name, n3.age]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name"), prop("n3", "age")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n2.name, n3.age]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name"), prop("n3", "age")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexProvider : 'fulltext-1.0'}") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("fulltext-1.0")))
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.analyzer`: 'some_analyzer'}}"
      ) {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig" -> mapOf("fulltext.analyzer" -> literalString("some_analyzer"))
          ))
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}, indexProvider : 'fulltext-1.0'}"
      ) {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig" -> mapOf("fulltext.eventually_consistent" -> falseLiteral)
          ))
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true}}"
      ) {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.analyzer" -> literalString("some_analyzer"),
            "fulltext.eventually_consistent" -> trueLiteral
          )))
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {nonValidOption : 42}") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name] OPTIONS {}") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        ))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n2.name] OPTIONS $$options") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE FULLTEXT INDEX $$my_index FOR $pattern ON EACH [n2.name]") {
        yields(fulltextIndex(
          isNodeIndex,
          List(prop("n2", "name")),
          labelsOrTypes,
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] {indexProvider : 'fulltext-1.0'}") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH (n2.name)") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH n2.name") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH []") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON [n2.name]") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON EACH [n2.name]") {
        assertFailsWithMessageStart(testName, "Invalid input") // different failures depending on pattern
      }

      // Missing escaping around `fulltext.analyzer`
      test(
        s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n2.name] OPTIONS {indexConfig : {fulltext.analyzer: 'some_analyzer'}}"
      ) {
        assertFailsWithMessageStart(testName, "Invalid input '{': expected \"+\" or \"-\"")
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
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"USE neo4j CREATE TEXT INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions).withGraph(
            Some(use(varFor("neo4j")))
          )
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE TEXT INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'text-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))
        ))
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'text-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'text-1.0'}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        ))
      }

      test(s"CREATE TEXT INDEX $$my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          Some(Left("my_index")),
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n.name, n.age") {
        assertFailsWithMessageStart(testName, """Invalid input ',': expected "OPTIONS" or <EOF>""")
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) {indexProvider : 'text-1.0'}") {
        failsToParse
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
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
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"USE neo4j CREATE POINT INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions).withGraph(
            Some(use(varFor("neo4j")))
          )
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE POINT INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE POINT INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE POINT INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE POINT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'point-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))
        ))
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'point-1.0'}"
      ) {
        yields(createIndex(
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
          ))
        ))
      }

      test(
        s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        ))
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        ))
      }

      test(s"CREATE POINT INDEX $$my_index FOR $pattern ON (n.name)") {
        yields(createIndex(
          List(prop("n", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE POINT INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE POINT INDEX my_index FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          Some(Left("my_index")),
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE OR REPLACE POINT INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(defaultPos))
      }
      test(s"CREATE POINT INDEX FOR $pattern ON n2.name, n3.age") {
        assertFailsWithMessageStart(testName, """Invalid input ',': expected "OPTIONS" or <EOF>""")
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n.name) {indexProvider : 'point-1.0'}") {
        failsToParse
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  // vector loop (relationship currently fails in semantic checking with a nicer error)
  Seq(
    ("(n1:Person)", vectorNodeIndex: CreateIndexFunction),
    ("()-[n1:R]-()", vectorRelIndex: CreateIndexFunction),
    ("()-[n1:R]->()", vectorRelIndex: CreateIndexFunction),
    ("()<-[n1:R]-()", vectorRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions))
      }

      test(s"USE neo4j CREATE VECTOR INDEX FOR $pattern ON (n2.name)") {
        yields(_ =>
          createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsThrowError, ast.NoOptions).withGraph(
            Some(use(varFor("neo4j")))
          )
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX `$$my_index` FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some("$my_index"),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE VECTOR INDEX FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsReplace, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE VECTOR INDEX my_index FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsReplace,
          ast.NoOptions
        ))
      }

      test(s"CREATE OR REPLACE VECTOR INDEX IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(List(prop("n2", "name")), None, posN2(testName), ast.IfExistsInvalidSyntax, ast.NoOptions))
      }

      test(s"CREATE OR REPLACE VECTOR INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX IF NOT EXISTS FOR $pattern ON (n2.name, n3.age)") {
        yields(createIndex(
          List(prop("n2", "name"), prop("n3", "age")),
          None,
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX my_index IF NOT EXISTS FOR $pattern ON (n2.name)") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'vector-1.0'}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("vector-1.0")))
        ))
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexProvider : 'vector-1.0', indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'euclidean' }}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(50),
              "vector.similarity_function" -> literalString("euclidean")
            )
          ))
        ))
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'cosine' }, indexProvider : 'vector-1.0'}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(50),
              "vector.similarity_function" -> literalString("cosine")
            )
          ))
        ))
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {indexConfig : {`vector.dimensions`: 50, `vector.similarity_function`: 'cosine' }}"
      ) {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(50),
            "vector.similarity_function" -> literalString("cosine")
          )))
        ))
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS $$options") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n2.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(
          List(prop("n2", "name")),
          None,
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))
        ))
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON (n2.name) OPTIONS {}") {
        yields(createIndex(
          List(prop("n2", "name")),
          Some(Left("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        ))
      }

      test(s"CREATE VECTOR INDEX $$my_index FOR $pattern ON (n.name)") {
        yields(createIndex(
          List(prop("n", "name")),
          Some(stringParam("my_index")),
          posN2(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        ))
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE VECTOR INDEX my_index FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          Some(Left("my_index")),
          posN1(testName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE OR REPLACE VECTOR INDEX IF NOT EXISTS FOR $pattern ON n2.name") {
        assertAst(createIndex(
          List(prop("n2", "name", posN2(testName))),
          None,
          posN1(testName),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(defaultPos))
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON n2.name, n3.age") {
        assertFailsWithMessageStart(testName, """Invalid input ',': expected "OPTIONS" or <EOF>""")
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n.name) {indexProvider : 'vector-1.0'}") {
        failsToParse
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  test("CREATE INDEX $ FOR (n1:Label) ON (n2.name)") {
    // Missing parameter name (or backticks)
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR (x1) ON EACH labels(x2)") {
    yields(ast.CreateLookupIndex(
      varFor("x1"),
      isNodeIndex = true,
      function(Labels.name, varFor("x2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[x1]-() ON EACH type(x2)") {
    yields(ast.CreateLookupIndex(
      varFor("x1"),
      isNodeIndex = false,
      function(Type.name, varFor("x2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON EACH count(n2)") {
    yields(ast.CreateLookupIndex(
      varFor("n1"),
      isNodeIndex = true,
      function(Count.name, varFor("n2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON EACH type(n2)") {
    yields(ast.CreateLookupIndex(
      varFor("n1"),
      isNodeIndex = true,
      function(Type.name, varFor("n2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH labels(x)") {
    yields(ast.CreateLookupIndex(
      varFor("n"),
      isNodeIndex = true,
      function(Labels.name, varFor("x")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON EACH count(r2)") {
    yields(ast.CreateLookupIndex(
      varFor("r1"),
      isNodeIndex = false,
      function(Count.name, varFor("r2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON EACH labels(r2)") {
    yields(ast.CreateLookupIndex(
      varFor("r1"),
      isNodeIndex = false,
      function(Labels.name, varFor("r2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(x)") {
    yields(ast.CreateLookupIndex(
      varFor("r"),
      isNodeIndex = false,
      function(Type.name, varFor("x")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() ON type(r2)") {
    yields(ast.CreateLookupIndex(
      varFor("r1"),
      isNodeIndex = false,
      function(Type.name, varFor("r2")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR (x) ON EACH EACH(x)") {
    yields(ast.CreateLookupIndex(
      varFor("x"),
      isNodeIndex = true,
      function("EACH", varFor("x")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH EACH(x)") {
    yields(ast.CreateLookupIndex(
      varFor("x"),
      isNodeIndex = false,
      function("EACH", varFor("x")),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions
    ))
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH(x)") {
    // Thinks it is missing the function name since `EACH` is parsed as keyword
    failsToParse
  }

  test("CREATE INDEX FOR n1:Person ON (n2.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsToParse
  }

  test("CREATE INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsToParse
  }

  test("CREATE INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR ()-[r1:R]- ON (r2.name)") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ON': expected \"(\", \">\" or <ARROW_RIGHT_HEAD> (line 1, column 29 (offset: 28))"
    )
  }

  test("CREATE INDEX FOR -[r1:R]- ON (r2.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR [r1:R] ON (r2.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input '-': expected "ON"""")
  }

  test("CREATE INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE TEXT INDEX FOR n1:Person ON (n2.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsToParse
  }

  test("CREATE TEXT INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsToParse
  }

  test("CREATE TEXT INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR ()-[r1:R]- ON (r2.name)") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ON': expected \"(\", \">\" or <ARROW_RIGHT_HEAD> (line 1, column 34 (offset: 33))"
    )
  }

  test("CREATE TEXT INDEX FOR -[r1:R]- ON (r2.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR [r1:R] ON (r2.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE TEXT INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE TEXT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input '-': expected "ON"""")
  }

  test("CREATE TEXT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE POINT INDEX FOR n1:Person ON (n2.name)") {
    failsToParse
  }

  test("CREATE POINT INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsToParse
  }

  test("CREATE POINT INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsToParse
  }

  test("CREATE POINT INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsToParse
  }

  test("CREATE POINT INDEX FOR ()-[r1:R]- ON (r2.name)") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ON': expected \"(\", \">\" or <ARROW_RIGHT_HEAD> (line 1, column 35 (offset: 34))"
    )
  }

  test("CREATE POINT INDEX FOR -[r1:R]- ON (r2.name)") {
    failsToParse
  }

  test("CREATE POINT INDEX FOR [r1:R] ON (r2.name)") {
    failsToParse
  }

  test("CREATE POINT INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE POINT INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE POINT INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input '-': expected "ON"""")
  }

  test("CREATE POINT INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE VECTOR INDEX FOR n1:Person ON (n2.name)") {
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR (n1) ON (n2.name)") {
    // missing label
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR ()-[n1]-() ON (n2.name)") {
    // missing relationship type
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR -[r1:R]-() ON (r2.name)") {
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR ()-[r1:R]- ON (r2.name)") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ON': expected \"(\", \">\" or <ARROW_RIGHT_HEAD> (line 1, column 36 (offset: 35))"
    )
  }

  test("CREATE VECTOR INDEX FOR -[r1:R]- ON (r2.name)") {
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR [r1:R] ON (r2.name)") {
    failsToParse
  }

  test("CREATE VECTOR INDEX FOR (:A)-[n1:R]-() ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(:A) ON (n2.name)") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE VECTOR INDEX FOR (n2)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(n2) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE VECTOR INDEX FOR (n2:A)-[n1:R]-() ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input '-': expected "ON"""")
  }

  test("CREATE VECTOR INDEX FOR ()-[n1:R]-(n2:A) ON (n2.name)") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE LOOKUP INDEX FOR n1 ON EACH labels(n2)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR -[r1]-() ON EACH type(r2)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]- ON EACH type(r2)") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ON': expected \"(\", \">\" or <ARROW_RIGHT_HEAD> (line 1, column 34 (offset: 33))"
    )
  }

  test("CREATE LOOKUP INDEX FOR -[r1]- ON EACH type(r2)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR [r1] ON EACH type(r2)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR (n1) EACH labels(n1)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR ()-[r1]-() EACH type(r2)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR (n1) ON labels(n2)") {
    failsToParse
  }

  test("CREATE INDEX FOR (n1) ON EACH labels(n2)") {
    failsToParse
  }

  test("CREATE INDEX FOR ()-[r1]-() ON EACH type(r2)") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1) ON EACH [n2.x]") {
    // missing label
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1]-() ON EACH [n2.x]") {
    // missing relationship type
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1|:A) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1|:R]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A|:B) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R|:S]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A||B) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R||S]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A:B) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R:S]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A&B) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R&S]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n1:A B) ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R S]-() ON EACH [n2.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (:A)-[n1:R]-() ON EACH [n2.name]") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(:A) ON EACH [n2.name]") {
    // label on node
    assertFailsWithMessageStart(testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE FULLTEXT INDEX FOR (n2)-[n1:R]-() ON EACH [n2.name]") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2) ON EACH [n2.name]") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE FULLTEXT INDEX FOR (n2:A)-[n1:R]-() ON EACH [n2.name]") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input '-': expected "ON"""")
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n1:R]-(n2:A) ON EACH [n2.name]") {
    // variable on node
    assertFailsWithMessageStart(testName, """Invalid input 'n2': expected ")"""")
  }

  // Drop index

  test("DROP INDEX ON :Person(name)") {
    yields(ast.DropIndex(labelName("Person"), List(propName("name"))))
  }

  test("DROP INDEX ON :Person(name, age)") {
    yields(ast.DropIndex(labelName("Person"), List(propName("name"), propName("age"))))
  }

  test("DROP INDEX my_index") {
    yields(ast.DropIndexOnName(Left("my_index"), ifExists = false))
  }

  test("DROP INDEX `$my_index`") {
    yields(ast.DropIndexOnName(Left("$my_index"), ifExists = false))
  }

  test("DROP INDEX my_index IF EXISTS") {
    yields(ast.DropIndexOnName(Left("my_index"), ifExists = true))
  }

  test("DROP INDEX $my_index") {
    yields(ast.DropIndexOnName(stringParam("my_index"), ifExists = false))
  }

  test("DROP INDEX my_index ON :Person(name)") {
    failsToParse
  }

  test("DROP INDEX ON (:Person(name))") {
    failsToParse
  }

  test("DROP INDEX ON (:Person {name})") {
    failsToParse
  }

  test("DROP INDEX ON [:Person(name)]") {
    failsToParse
  }

  test("DROP INDEX ON -[:Person(name)]-") {
    failsToParse
  }

  test("DROP INDEX ON ()-[:Person(name)]-()") {
    failsToParse
  }

  test("DROP INDEX ON [:Person {name}]") {
    failsToParse
  }

  test("DROP INDEX ON -[:Person {name}]-") {
    failsToParse
  }

  test("DROP INDEX ON ()-[:Person {name}]-()") {
    failsToParse
  }

  test("DROP INDEX on IF EXISTS") {
    yields(ast.DropIndexOnName(Left("on"), ifExists = true))
  }

  test("DROP INDEX on") {
    yields(ast.DropIndexOnName(Left("on"), ifExists = false))
  }

  test("DROP INDEX ON :if(exists)") {
    yields(ast.DropIndex(labelName("if"), List(propName("exists"))))
  }

  // help methods

  type CreateIndexFunction = (
    List[Property],
    Option[Either[String, Parameter]],
    InputPosition,
    ast.IfExistsDo,
    ast.Options
  ) => InputPosition => ast.CreateIndex

  private def btreeNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateBtreeNodeIndex(
      Variable("n1")(varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def btreeRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateBtreeRelationshipIndex(
      Variable("n1")(varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  type CreateRangeIndexFunction = (
    List[Property],
    Option[Either[String, Parameter]],
    InputPosition,
    ast.IfExistsDo,
    ast.Options,
    Boolean
  ) => InputPosition => ast.CreateIndex

  private def rangeNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options,
    fromDefault: Boolean
  ): InputPosition => ast.CreateIndex =
    ast.CreateRangeNodeIndex(
      Variable("n1")(varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options,
      fromDefault
    )

  private def rangeRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options,
    fromDefault: Boolean
  ): InputPosition => ast.CreateIndex =
    ast.CreateRangeRelationshipIndex(
      Variable("n1")(varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options,
      fromDefault
    )

  type CreateLookupIndexFunction =
    (Option[Either[String, Parameter]], InputPosition, ast.IfExistsDo, ast.Options) => InputPosition => ast.CreateIndex

  private def lookupNodeIndex(
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(
      Variable("n1")(varPos),
      isNodeIndex = true,
      function(Labels.name, varFor("n2")),
      name,
      ifExistsDo,
      options
    )

  private def lookupRelIndex(
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(
      Variable("r1")(varPos),
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
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex = {
    if (isNodeIndex) {
      fulltextNodeIndex(props, labelOrTypes, name, varPos, ifExistsDo, options)
    } else {
      fulltextRelIndex(props, labelOrTypes, name, varPos, ifExistsDo, options)
    }
  }

  private def fulltextNodeIndex(
    props: List[Property],
    labels: List[String],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateFulltextNodeIndex(Variable("n1")(varPos), labels.map(labelName(_)), props, name, ifExistsDo, options)

  private def fulltextRelIndex(
    props: List[Property],
    types: List[String],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateFulltextRelationshipIndex(
      Variable("n1")(varPos),
      types.map(relTypeName(_)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def textNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateTextNodeIndex(
      Variable("n1")(varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def textRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateTextRelationshipIndex(
      Variable("n1")(varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def pointNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreatePointNodeIndex(
      Variable("n1")(varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def pointRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex = {
    ast.CreatePointRelationshipIndex(
      Variable("n1")(varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )
  }

  private def vectorNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex =
    ast.CreateVectorNodeIndex(
      Variable("n1")(varPos),
      LabelName("Person")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )

  private def vectorRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    varPos: InputPosition,
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): InputPosition => ast.CreateIndex = {
    ast.CreateVectorRelationshipIndex(
      Variable("n1")(varPos),
      RelTypeName("R")(increasePos(varPos, 3)),
      props,
      name,
      ifExistsDo,
      options
    )
  }

  private def pos(offset: Int): InputPosition = (1, offset + 1, offset)
  private def posN1(query: String): InputPosition = pos(query.indexOf("n1"))
  private def posN2(query: String): InputPosition = pos(query.indexOf("n2"))
}
