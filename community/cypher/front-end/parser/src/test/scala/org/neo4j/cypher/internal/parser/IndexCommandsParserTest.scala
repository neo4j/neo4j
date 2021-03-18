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
    ("(n:Person)", nodeIndex: CreateIndexFunction),
    ("()-[n:R]-()", relIndex: CreateIndexFunction),
    ("()-[n:R]->()", relIndex: CreateIndexFunction),
    ("()<-[n:R]-()", relIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, createIndex: CreateIndexFunction) =>
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

  test("CREATE INDEX FOR n:Person ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR -[n:R]-() ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR ()-[n:R]- ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR -[n:R]- ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR [n:R] ON (n.name)") {
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

  type CreateIndexFunction = (List[expressions.Property], Option[String], ast.IfExistsDo, Map[String, expressions.Expression]) => InputPosition => ast.CreateIndex

  private def nodeIndex(props: List[expressions.Property],
                        name: Option[String],
                        ifExistsDo: ast.IfExistsDo,
                        options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateNodeIndex(varFor("n"), labelName("Person"), props, name, ifExistsDo, options)

  private def relIndex(props: List[expressions.Property],
                       name: Option[String],
                       ifExistsDo: ast.IfExistsDo,
                       options: Map[String, expressions.Expression]): InputPosition => ast.CreateIndex =
    ast.CreateRelationshipIndex(varFor("n"), relTypeName("R"), props, name, ifExistsDo, options)
}
