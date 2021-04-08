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
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition

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

  Seq(
    ("(n:Person)", btreeNodeIndex: CreateBtreeIndexFunction),
    ("()-[n:R]-()", btreeRelIndex: CreateBtreeIndexFunction),
    ("()-[n:R]->()", btreeRelIndex: CreateBtreeIndexFunction),
    ("()<-[n:R]-()", btreeRelIndex: CreateBtreeIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateBtreeIndexFunction) =>
      test(s"CREATE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, Map.empty))
      }

      test(s"USE neo4j CREATE INDEX FOR $pattern ON (n.name)") {
        yields(_ => createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, Map.empty).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE BTREE INDEX my_index FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE INDEX `$$my_index` FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("$my_index"), ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE OR REPLACE INDEX FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsReplace, Map.empty))
      }

      test(s"CREATE OR REPLACE INDEX my_index FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), Some("my_index"), ast.IfExistsReplace, Map.empty))
      }

      test(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsInvalidSyntax, Map.empty))
      }

      test(s"CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsInvalidSyntax, Map.empty))
      }

      test(s"CREATE INDEX IF NOT EXISTS FOR $pattern ON (n.name, n.age)") {
        yields(createIndex(List(prop("n", "name"), prop("n", "age")), None, ast.IfExistsDoNothing, Map.empty))
      }

      test(s"CREATE INDEX my_index IF NOT EXISTS FOR $pattern ON (n.name)") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsDoNothing, Map.empty))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
        yields(createIndex(List(prop("n", "name")),
          None, ast.IfExistsThrowError, Map("indexProvider" -> literalString("native-btree-1.0"))))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          )
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'lucene+native-3.0'}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          Map("indexProvider" -> literalString("lucene+native-3.0"),
            "indexConfig"   -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          )
        ))
      }

      test(s"CREATE BTREE INDEX FOR $pattern ON (n.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError,
          Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          ))
        ))
      }

      test(s"CREATE INDEX FOR $pattern ON (n.name) OPTIONS {nonValidOption : 42}") {
        yields(createIndex(List(prop("n", "name")), None, ast.IfExistsThrowError, Map("nonValidOption" -> literalInt(42))))
      }

      test(s"CREATE INDEX my_index FOR $pattern ON (n.name) OPTIONS {}") {
        yields(createIndex(List(prop("n", "name")), Some("my_index"), ast.IfExistsThrowError, Map.empty))
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

  Seq(
    ("(n)", "labels(n)", lookupNodeIndex: CreateLookupIndexFunction),
    ("()-[r]-()", "type(r)", lookupRelIndex: CreateLookupIndexFunction),
    ("()-[r]->()", "type(r)", lookupRelIndex: CreateLookupIndexFunction),
    ("()<-[r]-()", "type(r)", lookupRelIndex: CreateLookupIndexFunction)
  ).foreach {
    case (pattern, function, createIndex: CreateLookupIndexFunction) =>
      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsThrowError, Map.empty))
      }

      test(s"USE neo4j CREATE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(_ => createIndex(None, ast.IfExistsThrowError, Map.empty).withGraph(Some(use(varFor("neo4j")))))
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE LOOKUP INDEX `$$my_index` FOR $pattern ON EACH $function") {
        yields(createIndex(Some("$my_index"), ast.IfExistsThrowError, Map.empty))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsReplace, Map.empty))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsReplace, Map.empty))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsInvalidSyntax, Map.empty))
      }

      test(s"CREATE OR REPLACE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsInvalidSyntax, Map.empty))
      }

      test(s"CREATE LOOKUP INDEX IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(None, ast.IfExistsDoNothing, Map.empty))
      }

      test(s"CREATE LOOKUP INDEX my_index IF NOT EXISTS FOR $pattern ON EACH $function") {
        yields(createIndex(Some("my_index"), ast.IfExistsDoNothing, Map.empty))
      }

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $function OPTIONS {anyOption : 42}") {
        yields(createIndex(None, ast.IfExistsThrowError, Map("anyOption" -> literalInt(42))))
      }

      test(s"CREATE LOOKUP INDEX my_index FOR $pattern ON EACH $function OPTIONS {}") {
        yields(createIndex(Some("my_index"), ast.IfExistsThrowError, Map.empty))
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

  test("CREATE LOOKUP INDEX FOR (x) ON EACH labels(x)") {
    yields(ast.CreateLookupIndex(varFor("x"), isNodeIndex = true, function(Labels.name, varFor("x")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR ()-[x]-() ON EACH type(x)") {
    yields(ast.CreateLookupIndex(varFor("x"), isNodeIndex = false, function(Type.name, varFor("x")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH count(n)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Count.name, varFor("n")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH type(n)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Type.name, varFor("n")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR (n) ON EACH labels(x)") {
    yields(ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Labels.name, varFor("x")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH count(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Count.name, varFor("r")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH labels(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Labels.name, varFor("r")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(x)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("x")), None, ast.IfExistsThrowError, Map.empty))
  }

  test("CREATE LOOKUP INDEX FOR ()-[r]-() ON type(r)") {
    yields(ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("r")), None, ast.IfExistsThrowError, Map.empty))
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

  type CreateBtreeIndexFunction = (List[expressions.Property], Option[String], ast.IfExistsDo, Map[String, expressions.Expression]) => InputPosition => ast.CreateIndex

  private def btreeNodeIndex(props: List[expressions.Property],
                        name: Option[String],
                        ifExistsDo: ast.IfExistsDo,
                        options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateBtreeNodeIndex(varFor("n"), labelName("Person"), props, name, ifExistsDo, options)

  private def btreeRelIndex(props: List[expressions.Property],
                       name: Option[String],
                       ifExistsDo: ast.IfExistsDo,
                       options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateBtreeRelationshipIndex(varFor("n"), relTypeName("R"), props, name, ifExistsDo, options)

  type CreateLookupIndexFunction = (Option[String], ast.IfExistsDo, Map[String, expressions.Expression]) => InputPosition => ast.CreateIndex

  private def lookupNodeIndex(name: Option[String],
                        ifExistsDo: ast.IfExistsDo,
                        options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(varFor("n"), isNodeIndex = true, function(Labels.name, varFor("n")), name, ifExistsDo, options)

  private def lookupRelIndex(name: Option[String],
                       ifExistsDo: ast.IfExistsDo,
                       options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateLookupIndex(varFor("r"), isNodeIndex = false, function(Type.name, varFor("r")), name, ifExistsDo, options)
}
