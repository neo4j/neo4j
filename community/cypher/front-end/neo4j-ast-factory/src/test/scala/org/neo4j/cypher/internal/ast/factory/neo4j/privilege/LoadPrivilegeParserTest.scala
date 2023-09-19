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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.LoadAction
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition

class LoadPrivilegeParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantLoadPrivilege: loadPrivilegeFunc),
    ("DENY", "TO", denyLoadPrivilege: loadPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantLoadPrivilege: loadPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyLoadPrivilege: loadPrivilegeFunc),
    ("REVOKE", "FROM", revokeLoadPrivilege: loadPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: loadPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)

          test(s"""$verb$immutableString LOAD ON URL "https://my.server.com/some/file.csv" $preposition role""") {
            yields(func(
              ast.LoadUrlQualifier("https://my.server.com/some/file.csv")(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"""$verb$immutableString LOAD ON CIDR "192.168.1.0/24" $preposition role""") {
            yields(func(ast.LoadCidrQualifier("192.168.1.0/24")(InputPosition.NONE), Seq(literalRole), immutable))
          }

          test(s"""$verb$immutableString LOAD ON URL $$foo $preposition role""") {
            yields(func(ast.LoadUrlQualifier(paramFoo)(InputPosition.NONE), Seq(literalRole), immutable))
          }

          test(s"""$verb$immutableString LOAD ON CIDR $$foo $preposition role""") {
            yields(func(ast.LoadCidrQualifier(paramFoo)(InputPosition.NONE), Seq(literalRole), immutable))
          }

          test(s"""$verb$immutableString LOAD ON ALL DATA $preposition role""") {
            yields(func(ast.LoadAllQualifier()(InputPosition.NONE), Seq(literalRole), immutable))
          }

      }
  }

  test("""DENY LOAD ON URL "not really a url" TO $role""") {
    yields(denyLoadPrivilege(ast.LoadUrlQualifier("not really a url")(InputPosition.NONE), Seq(paramRole), i = false))
  }

  test("""REVOKE GRANT LOAD ON CIDR 'not a cidr' FROM $role""") {
    yields(revokeGrantLoadPrivilege(ast.LoadCidrQualifier("not a cidr")(InputPosition.NONE), Seq(paramRole), i = false))
  }

  // Error Cases

  test("""GRANT LOAD ON DATABASE foo TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'DATABASE': expected""")
  }

  test("""DENY LOAD ON HOME GRAPH TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'HOME': expected""")
  }

  test("""REVOKE GRANT LOAD ON DBMS ON ALL DATA FROM role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'DBMS': expected""")
  }

  test("""GRANT LOAD ON CIDR TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'TO': expected""")
  }

  test("""GRANT LOAD ON URL TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'TO': expected""")
  }

  test("""DENY LOAD TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'TO': expected""")
  }

  test("""GRANT LOAD ON CIDR "1.2.3.4/22" URL "http://example.com" TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'URL': expected""")
  }

  test("""DENY LOAD ON CIDR "1.2.3.4/22" ON URL "http://example.com" TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input 'ON': expected""")
  }

  test("""GRANT LOAD ON URL "http://www.badger.com","file:///test.csv" TO role""") {
    assertFailsWithMessageStart(testName, """Invalid input ',': expected""")
  }

  test("""REVOKE DENY LOAD ON CIDR "1.2.3.4/22","1.2.3.4/22" FROM role""") {
    assertFailsWithMessageStart(testName, """Invalid input ',': expected""")
  }

  test("""REVOKE DENY LOAD ON ALL DATA "1.2.3.4/22" FROM role""") {
    assertFailsWithMessageStart(testName, """Invalid input '1.2.3.4/22': expected""")
  }

  // help methods

  def grantLoadPrivilege(
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(
      ast.LoadPrivilege(LoadAction)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r
    )

  def denyLoadPrivilege(
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(
      ast.LoadPrivilege(LoadAction)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r
    )

  def revokeGrantLoadPrivilege(
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(LoadAction)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeGrantType()(pos)
    )

  def revokeDenyLoadPrivilege(
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(LoadAction)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeDenyType()(pos)
    )

  def revokeLoadPrivilege(
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(LoadAction)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeBothType()(pos)
    )
}
