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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

abstract class WritePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  def privilegeTests(command: String, preposition: String, func: noResourcePrivilegeFunc): Unit = {
    Seq("GRAPH", "GRAPHS").foreach {
      graphKeyword =>

        Seq("ELEMENT", "ELEMENTS").foreach {
          elementKeyword =>

            Seq(
              ("*", ast.AllGraphsScope()(pos)),
              ("foo", ast.NamedGraphScope(literal("foo"))(pos)),
              ("$foo", ast.NamedGraphScope(param("foo"))(pos))
            ).foreach {
              case (dbName: String, graphScope: ast.GraphScope) =>
                val graphScopes = List(graphScope)

                test(s"$command WRITE ON $graphKeyword $dbName $elementKeyword * $preposition $$role") {
                  yields(func(ast.WritePrivilege()(pos), graphScopes, ast.ElementsAllQualifier() _, Seq(param("role"))))
                }

                test(s"$command WRITE ON $graphKeyword $dbName $elementKeyword * (*) $preposition role") {
                  yields(func(ast.WritePrivilege()(pos), graphScopes, ast.ElementsAllQualifier() _, Seq(literal("role"))))
                }

                test(s"$command WRITE ON $graphKeyword $dbName $elementKeyword * $preposition `r:ole`") {
                  yields(func(ast.WritePrivilege()(pos), graphScopes, ast.ElementsAllQualifier() _, Seq(literal("r:ole"))))
                }

                test(s"$command WRITE ON $graphKeyword $dbName $elementKeyword A $preposition role") {
                  yields(func(ast.WritePrivilege()(pos), graphScopes, ast.ElementsQualifier(Seq("A")) _, Seq(literal("role"))))
                }

                test(s"$command WRITE ON $graphKeyword $dbName $elementKeyword A (*) $preposition role") {
                  yields(func(ast.WritePrivilege()(pos), graphScopes, ast.ElementsQualifier(Seq("A")) _, Seq(literal("role"))))
                }

                test(s"failToParseStatements $graphKeyword $dbName $elementKeyword $preposition") {
                  // Missing `ON`
                  assertFails( s"$command WRITE $graphKeyword $dbName $elementKeyword * (*) $preposition role")
                  // Missing role
                  assertFails( s"$command WRITE ON $graphKeyword $dbName $elementKeyword * (*)")
                  // Invalid role name
                  assertFails( s"$command WRITE ON $graphKeyword $dbName $elementKeyword * $preposition r:ole")
                  // Does not support write on specific label yet
                  assertFails( s"$command WRITE ON $graphKeyword $dbName $elementKeyword * (foo) $preposition role")
                  assertFails( s"$command WRITE ON $graphKeyword $dbName $elementKeyword A (foo) $preposition role")
                  // Does not support write on specific property
                  assertFails( s"$command WRITE {*} ON $graphKeyword $dbName $elementKeyword * $preposition role")
                  assertFails( s"$command WRITE {prop} ON $graphKeyword $dbName $elementKeyword * $preposition role")
                }
            }

            test(s"$command WRITE ON $graphKeyword `f:oo` $elementKeyword * $preposition role") {
              yields(func(ast.WritePrivilege()(pos), List(ast.NamedGraphScope(literal("f:oo")) _), ast.ElementsAllQualifier() _, Seq(literal("role"))))
            }

            // Multiple graphs allowed
            test(s"$command WRITE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") {
              yields(func(ast.WritePrivilege()(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.ElementsQualifier(Seq("A")) _, List(literal("role"))))
            }

            test(s"parseErrors $command $graphKeyword $elementKeyword $preposition") {
              // Invalid graph name
              assertFails(s"$command WRITE ON $graphKeyword f:oo $elementKeyword * $preposition role")
              assertFails(s"$command WRITE ON $graphKeyword $elementKeyword * (*) $preposition role")
            }
        }

        // Needs to be separate loop to avoid duplicate tests since the test does not have any $elementKeyword
        Seq(
          ("*", ast.AllGraphsScope()(pos)),
          ("foo", ast.NamedGraphScope(literal("foo"))(pos)),
          ("$foo", ast.NamedGraphScope(param("foo"))(pos))
        ).foreach {
          case (dbName: String, graphScope: ast.GraphScope) =>

            test(s"$command WRITE ON $graphKeyword $dbName $preposition role") {
              yields(func(ast.WritePrivilege()(pos), List(graphScope), ast.ElementsAllQualifier() _, Seq(literal("role"))))
            }
        }
    }

    // Mix of specific graph and *

    test(s"$command WRITE ON GRAPH foo, * $preposition role") {
      failsToParse
    }

    test(s"$command WRITE ON GRAPH *, foo $preposition role") {
      failsToParse
    }

    // Database instead of graph keyword

    test(s"$command WRITE ON DATABASES * $preposition role") {
      failsToParse
    }

    test(s"$command WRITE ON DATABASE foo $preposition role") {
      failsToParse
    }

    test(s"$command WRITE ON DEFAULT DATABASE $preposition role") {
      failsToParse
    }
  }
}
