/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast

class MultiDatabaseAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  // SHOW DATABASE

  test("SHOW DATABASE foo") {
    yields(ast.ShowDatabase("foo"))
  }

  test("SHOW DATABASE `foo.bar`") {
    yields(ast.ShowDatabase("foo.bar"))
  }

  test("SHOW DATABASES") {
    yields(ast.ShowDatabases())
  }

  test("SHOW DEFAULT DATABASE") {
    yields(ast.ShowDefaultDatabase())
  }

  test("SHOW DATABASE foo.bar") {
    failsToParse
  }

  test("SHOW DATABASE") {
    failsToParse
  }

  // CREATE DATABASE

  test("CREATE DATABASE foo") {
    yields(ast.CreateDatabase("foo", ast.IfExistsThrowError()))
  }

  test("CREATE DATABASE `foo.bar`") {
    yields(ast.CreateDatabase("foo.bar", ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE DATABASE `foo.bar`") {
    yields(ast.CreateDatabase("foo.bar", ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE DATABASE `foo-bar42`") {
    yields(ast.CreateDatabase("foo-bar42", ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE DATABASE `_foo-bar42`") {
    yields(ast.CreateDatabase("_foo-bar42", ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE DATABASE ``") {
    yields(ast.CreateDatabase("", ast.IfExistsThrowError()))
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase("foo", ast.IfExistsDoNothing()))
  }

  test("CATALOG CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    yields(ast.CreateDatabase("_foo-bar42", ast.IfExistsDoNothing()))
  }

  test("CREATE OR REPLACE DATABASE foo") {
    yields(ast.CreateDatabase("foo", ast.IfExistsReplace()))
  }

  test("CATALOG CREATE OR REPLACE DATABASE `_foo-bar42`") {
    yields(ast.CreateDatabase("_foo-bar42", ast.IfExistsReplace()))
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase("foo", ast.IfExistsInvalidSyntax()))
  }

  test("CREATE DATABASE foo.bar") {
    failsToParse
  }

  test("CREATE DATABASE \"foo.bar\"") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE foo.bar") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE foo-bar42") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE 42foo-bar") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    failsToParse
  }

  test("CATALOG CREATE OR REPLACE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CREATE OR REPLACE DATABASE") {
    failsToParse
  }

  // DROP DATABASE

  test("DROP DATABASE foo") {
    yields(ast.DropDatabase("foo", ifExists = false))
  }

  test("CATALOG DROP DATABASE `foo.bar`") {
    yields(ast.DropDatabase("foo.bar", ifExists = false))
  }

  test("DROP DATABASE foo IF EXISTS") {
    yields(ast.DropDatabase("foo", ifExists = true))
  }

  test("CATALOG DROP DATABASE foo.bar") {
    failsToParse
  }

  test("DROP DATABASE") {
    failsToParse
  }

  test("DROP DATABASE  IF EXISTS") {
    failsToParse
  }

  // START DATABASE

  test("START DATABASE foo") {
    yields(ast.StartDatabase("foo"))
  }

  test("CATALOG START DATABASE `foo.bar`") {
    yields(ast.StartDatabase("foo.bar"))
  }

  test("CATALOG START DATABASE foo.bar") {
    failsToParse
  }

  test("START DATABASE") {
    failsToParse
  }

  // STOP DATABASE

  test("STOP DATABASE foo") {
    yields(ast.StopDatabase("foo"))
  }

  test("CATALOG STOP DATABASE `foo.bar`") {
    yields(ast.StopDatabase("foo.bar"))
  }

  test("CATALOG STOP DATABASE foo.bar") {
    failsToParse
  }

  test("STOP DATABASE") {
    failsToParse
  }
}
