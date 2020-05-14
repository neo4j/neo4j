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
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class WritePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grant: noResourcePrivilegeFunc),
    ("DENY", "TO", deny: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrant: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDeny: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeBoth: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      test(s"$verb WRITE ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPHS foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      // Multiple graphs should be allowed (with and without plural GRAPHS)

      test(s"$verb WRITE ON GRAPH * $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.AllGraphsScope()(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.AllGraphsScope()(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPH foo, baz $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.ElementsAllQualifier() _, List(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPHS foo, baz $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.ElementsAllQualifier() _, List(literal("role"))))
      }

      // Multiple roles should be allowed
      test(s"$verb WRITE ON GRAPH foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(WriteAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier()(_), Seq(literal("role1"), literal("role2"))))
      }

      // Parameters and escaped strings should be allowed

      test(s"$verb WRITE ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(param("foo"))(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPH `f:oo` $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("f:oo"))(_)), ast.ElementsAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$verb WRITE ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier() _, Seq(param("role"))))
      }

      test(s"$verb WRITE ON GRAPH foo $preposition `r:ole`") {
        yields(func(ast.GraphPrivilege(WriteAction)(pos), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier() _, Seq(literal("r:ole"))))
      }

      // Resource or qualifier should not be supported
      test(s"$verb WRITE {*} ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE {prop} ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo NODE A $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo NODES * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIP R $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIPS * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENT A $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENTS * $preposition role") {
        failsToParse
      }

      // Invalid/missing part of the command

      test(s"$verb WRITE ON GRAPH f:oo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo $preposition ro:le") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo $preposition") {
        failsToParse
      }

      test(s"$verb WRITE GRAPH foo $preposition role") {
        failsToParse
      }

      // Mix of specific graph and *

      test(s"$verb WRITE ON GRAPH foo, * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH *, foo $preposition role") {
        failsToParse
      }

      // Database instead of graph keyword

      test(s"$verb WRITE ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
