/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap

/* Tests for creating and dropping indexes */
class IndexCommandsParserTest extends SchemaCommandsParserTestBase {

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

  // Create index

  // default type loop (parses as range, planned as btree)
  Seq(
    ("(n:Person)", rangeNodeIndex: CreateRangeIndexFunction),
    ("()-[n:R]-()", rangeRelIndex: CreateRangeIndexFunction),
    ("()-[n:R]->()", rangeRelIndex: CreateRangeIndexFunction),
    ("()<-[n:R]-()", rangeRelIndex: CreateRangeIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateRangeIndexFunction) =>
      test(s"CREATE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions, true))
      }

      test(s"USE neo4j CREATE INDEX FOR $pattern ON (n.name)") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions, true).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, NoOptions, true))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, NoOptions, true))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, NoOptions, true))
      }

      test(s"CREATE INDEX `$$my_index` FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, NoOptions, true))
      }

      test(s"CREATE OR REPLACE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, NoOptions, true))
      }

      test(s"CREATE OR REPLACE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, NoOptions, true))
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, NoOptions, true))
      }

      test(s"CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions, true))
      }

      test(s"CREATE INDEX IF NOT EXISTS FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, NoOptions, true))
      }

      test(s"CREATE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, NoOptions, true))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0"))), true))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          )), true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'lucene+native-3.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          )), true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          ))), true
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS $$options") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsParam(parameter("options", CTMap)), true))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42))), true))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n.name) OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty), true))
      }

      test(s"CREATE INDEX $$my_index FOR $pattern ON (n.name)") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON n.name") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) {indexProvider : 'native-btree-1.0'}") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  // range loop
  Seq(
    ("(n:Person)", rangeNodeIndex: CreateRangeIndexFunction),
    ("()-[n:R]-()", rangeRelIndex: CreateRangeIndexFunction),
    ("()-[n:R]->()", rangeRelIndex: CreateRangeIndexFunction),
    ("()<-[n:R]-()", rangeRelIndex: CreateRangeIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateRangeIndexFunction) =>
      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions, false))
      }

      test(s"USE neo4j CREATE RANGE INDEX FOR $pattern ON (n.name)") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions, false).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX `$$my_index` FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, NoOptions, false))
      }

      test(s"CREATE OR REPLACE RANGE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, NoOptions, false))
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, NoOptions, false))
      }

      test(s"CREATE OR REPLACE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, NoOptions, false))
      }

      test(s"CREATE OR REPLACE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX IF NOT EXISTS FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, NoOptions, false))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'range-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("range-1.0"))), false))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'range-1.0', indexConfig : {someConfig: 'toShowItCanBePrettified'}}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map(
            "indexProvider" -> literalString("range-1.0"),
            "indexConfig"   -> mapOf("someConfig" -> literalString("toShowItCanBePrettified"))
          )), false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {someConfig: 'toShowItCanBePrettified'}, indexProvider : 'range-1.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map(
            "indexProvider" -> literalString("range-1.0"),
            "indexConfig"   -> mapOf("someConfig" -> literalString("toShowItCanBePrettified"))
          )), false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {}}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexConfig" -> mapOf())), false
        ))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS $$options") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsParam(parameter("options", CTMap)), false))
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42))), false))
      }

      test(s"CREATE RANGE INDEX my_index FOR $pattern ON (n.name) OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty), false))
      }

      test(s"CREATE RANGE INDEX $$my_index FOR $pattern ON (n.name)") {
        failsToParse
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON n.name") {
        failsToParse
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) {indexProvider : 'range-1.0'}") {
        failsToParse
      }

      test(s"CREATE RANGE INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  // btree loop
  Seq(
    ("(n:Person)", btreeNodeIndex: CreateIndexFunction),
    ("()-[n:R]-()", btreeRelIndex: CreateIndexFunction),
    ("()-[n:R]->()", btreeRelIndex: CreateIndexFunction),
    ("()<-[n:R]-()", btreeRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"USE neo4j CREATE BTREE INDEX FOR $pattern ON (n.name)") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE BTREE INDEX `$$my_index` FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE OR REPLACE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE BTREE INDEX IF NOT EXISTS FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE BTREE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0")))))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'lucene+native-3.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS $$options") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42)))))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n.name) OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty)))
      }

      test(s"CREATE BTREE INDEX $$my_index FOR $pattern ON (n.name)") {
        failsToParse
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON n.name") {
        failsToParse
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
    ("(n)", "labels(n)", lookupNodeIndex: CreateLookupIndexFunction),
    ("()-[r]-()", "type(r)", lookupRelIndex: CreateLookupIndexFunction),
    ("()-[r]->()", "type(r)", lookupRelIndex: CreateLookupIndexFunction),
    ("()<-[r]-()", "type(r)", lookupRelIndex: CreateLookupIndexFunction)
  ).foreach {
    case (pattern, function, createIndex: CreateLookupIndexFunction) =>
      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"USE neo4j CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(_ => createIndex(None, ast.IfExistsThrowError, NoOptions).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE LOOKUP INDEX `$$my_index` FOR $pattern ON EACH $function") {
        yields(createIndex(Some("$my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS {anyOption : 42}") {
        yields(createIndex(None, ast.IfExistsThrowError, OptionsMap(Map("anyOption" -> literalInt(42)))))
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function OPTIONS {}") {
        yields(createIndex(Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty)))
      }

      test(s"CREATE LOOKUP INDEX $$my_index FOR $pattern ON EACH $function") {
        failsToParse
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function {indexProvider : 'native-btree-1.0'}") {
        failsToParse
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS") {
        failsToParse
      }
  }

  // fulltext loop
  Seq(
    ("(n:Person)", fulltextNodeIndex(_, List("Person"), _, _, _)),
    ("(n:Person|Colleague|Friend)", fulltextNodeIndex(_, List("Person", "Colleague", "Friend"), _, _, _)),
    ("()-[n:R]->()", fulltextRelIndex(_, List("R"), _, _, _)),
    ("()<-[n:R|S]-()", fulltextRelIndex(_, List("R", "S"), _, _, _))
  ).foreach {
    case (pattern, createIndex: CreateFulltextIndexFunction) =>
      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"USE neo4j CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name]") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name, n.age]") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n.name, n.age]") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX `$$my_index` FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index FOR $pattern ON EACH [n.name, n.age]") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE OR REPLACE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX IF NOT EXISTS FOR $pattern ON EACH [n.name, n.age]") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX my_index IF NOT EXISTS FOR $pattern ON EACH [n.name]") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {indexProvider : 'fulltext-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("fulltext-1.0")))))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.analyzer`: 'some_analyzer'}}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig"   -> mapOf("fulltext.analyzer" -> literalString("some_analyzer"))
          ))
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}, indexProvider : 'fulltext-1.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig"   -> mapOf("fulltext.eventually_consistent" -> falseLiteral)
          ))
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {indexConfig : {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true}}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.analyzer" -> literalString("some_analyzer"),
            "fulltext.eventually_consistent" -> trueLiteral
          )))
        ))
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42)))))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n.name] OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty)))
      }

      test(s"CREATE FULLTEXT INDEX my_index FOR $pattern ON EACH [n.name] OPTIONS $$options") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsParam(parameter("options", CTMap))))
      }

      test(s"CREATE FULLTEXT INDEX $$my_index FOR $pattern ON EACH [n.name]") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] {indexProvider : 'fulltext-1.0'}") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH (n.name)") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH n.name") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH []") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH") {
        failsToParse
      }

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON [n.name]") {
        failsToParse
      }

      test(s"CREATE INDEX FOR $pattern ON EACH [n.name]") {
        failsToParse
      }

      // Missing escaping around `fulltext.analyzer`
      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [n.name] OPTIONS {indexConfig : {fulltext.analyzer: 'some_analyzer'}}") {
        failsToParse
      }
  }

  // text loop
  Seq(
    ("(n:Person)", textNodeIndex: CreateIndexFunction),
    ("()-[n:R]-()", textRelIndex: CreateIndexFunction),
    ("()-[n:R]->()", textRelIndex: CreateIndexFunction),
    ("()<-[n:R]-()", textRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"USE neo4j CREATE TEXT INDEX FOR $pattern ON (n.name)") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, NoOptions).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE TEXT INDEX `$$my_index` FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE OR REPLACE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, NoOptions))
      }

      test(s"CREATE TEXT INDEX IF NOT EXISTS FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE TEXT INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, NoOptions))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'text-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("text-1.0")))))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'text-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("text-1.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'text-1.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexProvider" -> literalString("text-1.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS $$options") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          OptionsParam(parameter("options", CTMap))
        ))
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42)))))
      }

      test(s"CREATE TEXT INDEX my_index FOR $pattern ON (n.name) OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, OptionsMap(Map.empty)))
      }

      test(s"CREATE TEXT INDEX $$my_index FOR $pattern ON (n.name)") {
        failsToParse
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n.name") {
        failsToParse
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON n.name, n.age") {
        failsToParse
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) {indexProvider : 'text-1.0'}") {
        failsToParse
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (n.name) OPTIONS") {
        failsToParse
      }
  }

  test("CREATE LOOKUP INDEX FOR (x) ON EACH labels(x)") {
    yields(ast.CreateLookupIndex(varFor("x"), isNodeIndex = true, function(Labels.name, varFor("x")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH type(x)") {
    yields(ast.CreateLookupIndex(varFor("x"), isNodeIndex = false, function(Type.name, varFor("x")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH count(n)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Count.name, varFor("n")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH type(n)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Type.name, varFor("n")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH labels(x)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Labels.name, varFor("x")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH count(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Count.name, varFor("r")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH labels(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Labels.name, varFor("r")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(x)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("x")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON type(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("r")), None, ast.IfExistsThrowError, NoOptions))
  }

  test("CREATE INDEX FOR n:Person ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR -[r:R]-() ON (r.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR ()-[r:R]- ON (r.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR -[r:R]- ON (r.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR [r:R] ON (r.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR n:Person ON (n.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR -[r:R]-() ON (r.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR ()-[r:R]- ON (r.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR -[r:R]- ON (r.name)") {
    failsToParse
  }

  test("CREATE TEXT INDEX FOR [r:R] ON (r.name)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR n ON EACH labels(n)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR -[r]-() ON EACH type(r)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]- ON EACH type(r)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR -[r]- ON EACH type(r)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR [r] ON EACH type(r)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR (n) EACH labels(n)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() EACH type(r)") {
    failsToParse
  }

  test("CREATE LOOKUP INDEX FOR (n) ON labels(n)") {
    failsToParse
  }

  test("CREATE INDEX FOR (n) ON EACH labels(n)") {
    failsToParse
  }

  test("CREATE INDEX FOR ()-[r]-() ON EACH type(r)") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n|:A) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n|:R]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n:A|:B) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n:R|:S]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n:A||B) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n:R||S]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n:A:B) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n:R:S]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n:A&B) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n:R&S]-() ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR (n:A B) ON EACH [n.x]") {
    failsToParse
  }

  test("CREATE FULLTEXT INDEX FOR ()-[n:R S]-() ON EACH [n.x]") {
    failsToParse
  }

  // Drop index

  test("DROP INDEX ON :Person(name)") {
    yields(ast.DropIndex(labelName("Person"), List(propName("name"))))
  }

  test("DROP INDEX ON :Person(name, age)") {
    yields(ast.DropIndex(labelName("Person"), List(propName("name"), propName("age"))))
  }

  test("DROP INDEX my_index") {
    yields(ast.DropIndexOnName("my_index", ifExists = false))
  }

  test("DROP INDEX `$my_index`") {
    yields(ast.DropIndexOnName("$my_index", ifExists = false))
  }

  test("DROP INDEX my_index IF EXISTS") {
    yields(ast.DropIndexOnName("my_index", ifExists = true))
  }

  test("DROP INDEX $my_index") {
    failsToParse
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

  // help methods

  type CreateIndexFunction = (List[expressions.Property], Option[String], ast.IfExistsDo, Options) => InputPosition => ast.CreateIndex

  private def btreeNodeIndex(props: List[expressions.Property],
                             name: Option[String],
                             ifExistsDo: ast.IfExistsDo,
                             options: Options): InputPosition => ast.CreateIndex =
    ast.CreateBtreeNodeIndex(varFor("n"), labelName("Person"), props, name, ifExistsDo, options)

  private def btreeRelIndex(props: List[expressions.Property],
                            name: Option[String],
                            ifExistsDo: ast.IfExistsDo,
                            options: Options): InputPosition => ast.CreateIndex =
    ast.CreateBtreeRelationshipIndex(varFor("n"), relTypeName("R"), props, name, ifExistsDo, options)

  type CreateRangeIndexFunction = (List[expressions.Property], Option[String], ast.IfExistsDo, Options, Boolean) => InputPosition => ast.CreateIndex

  private def rangeNodeIndex(props: List[expressions.Property],
                             name: Option[String],
                             ifExistsDo: ast.IfExistsDo,
                             options: Options,
                             fromDefault: Boolean): InputPosition => ast.CreateIndex =
    ast.CreateRangeNodeIndex(varFor("n"), labelName("Person"), props, name, ifExistsDo, options, fromDefault)

  private def rangeRelIndex(props: List[expressions.Property],
                            name: Option[String],
                            ifExistsDo: ast.IfExistsDo,
                            options: Options,
                            fromDefault: Boolean): InputPosition => ast.CreateIndex =
    ast.CreateRangeRelationshipIndex(varFor("n"), relTypeName("R"), props, name, ifExistsDo, options, fromDefault)

  type CreateLookupIndexFunction = (Option[String], ast.IfExistsDo, Options) => InputPosition => ast.CreateIndex

  private def lookupNodeIndex(name: Option[String],
                              ifExistsDo: ast.IfExistsDo,
                              options: Options): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Labels.name, varFor("n")), name, ifExistsDo, options)

  private def lookupRelIndex(name: Option[String],
                             ifExistsDo: ast.IfExistsDo,
                             options: Options): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("r")), name, ifExistsDo, options)

  type CreateFulltextIndexFunction = (List[expressions.Property], Option[String], ast.IfExistsDo, Map[String, expressions.Expression]) => InputPosition => ast.CreateIndex

  private def fulltextNodeIndex(props: List[expressions.Property],
                                labels: List[String],
                                name: Option[String],
                                ifExistsDo: ast.IfExistsDo,
                                options: Options): InputPosition => ast.CreateIndex =
    ast.CreateFulltextNodeIndex(varFor("n"), labels.map(labelName), props, name, ifExistsDo, options)

  private def fulltextRelIndex(props: List[expressions.Property],
                               types: List[String],
                               name: Option[String],
                               ifExistsDo: ast.IfExistsDo,
                               options: Options): InputPosition => ast.CreateIndex =
    ast.CreateFulltextRelationshipIndex(varFor("n"), types.map(relTypeName), props, name, ifExistsDo, options)

  private def textNodeIndex(props: List[expressions.Property],
                            name: Option[String],
                            ifExistsDo: ast.IfExistsDo,
                            options: Options): InputPosition => ast.CreateIndex =
    ast.CreateTextNodeIndex(varFor("n"), labelName("Person"), props, name, ifExistsDo, options)

  private def textRelIndex(props: List[expressions.Property],
                           name: Option[String],
                           ifExistsDo: ast.IfExistsDo,
                           options: Options): InputPosition => ast.CreateIndex =
    ast.CreateTextRelationshipIndex(varFor("n"), relTypeName("R"), props, name, ifExistsDo, options)
}
