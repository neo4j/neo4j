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
import org.neo4j.cypher.internal.ast.Statements
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
            yields[Statements](func(
              ast.LoadUrlAction,
              ast.LoadUrlQualifier("https://my.server.com/some/file.csv")(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"""$verb$immutableString LOAD ON CIDR "192.168.1.0/24" $preposition role""") {
            yields[Statements](func(
              ast.LoadCidrAction,
              ast.LoadCidrQualifier("192.168.1.0/24")(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"""$verb$immutableString LOAD ON URL $$foo $preposition role""") {
            yields[Statements](func(
              ast.LoadUrlAction,
              ast.LoadUrlQualifier(paramFoo)(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"""$verb$immutableString LOAD ON CIDR $$foo $preposition role""") {
            yields[Statements](func(
              ast.LoadCidrAction,
              ast.LoadCidrQualifier(paramFoo)(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"""$verb$immutableString LOAD ON ALL DATA $preposition role""") {
            yields[Statements](func(
              ast.LoadAllDataAction,
              ast.LoadAllQualifier()(InputPosition.NONE),
              Seq(literalRole),
              immutable
            ))
          }

      }
  }

  test("""DENY LOAD ON URL "not really a url" TO $role""") {
    yields[Statements](denyLoadPrivilege(
      ast.LoadUrlAction,
      ast.LoadUrlQualifier("not really a url")(InputPosition.NONE),
      Seq(paramRole),
      i = false
    ))
  }

  test("""REVOKE GRANT LOAD ON CIDR 'not a cidr' FROM $role""") {
    yields[Statements](revokeGrantLoadPrivilege(
      ast.LoadCidrAction,
      ast.LoadCidrQualifier("not a cidr")(InputPosition.NONE),
      Seq(paramRole),
      i = false
    ))
  }

  test("GRANT LOAD ON CIDR $x TO `\u0885`, `x\u0885y`") {
    yields[Statements](grantLoadPrivilege(
      ast.LoadCidrAction,
      ast.LoadCidrQualifier(stringParam("x"))(InputPosition.NONE),
      Seq(Left("\u0885"), Left("x\u0885y")),
      i = false
    ))
  }

  // Error Cases

  test("GRANT LOAD ON CIDR $x TO \u0885") {
    // the `\u0885` needs to be escaped to be able to be parsed
    assertFailsWithMessageStart[Statements](testName, "Invalid input '\u0885': expected a parameter or an identifier")
  }

  test("GRANT LOAD ON CIDR $x TO x\u0885y") {
    // the `\u0885` needs to be escaped to be able to be parsed
    assertFailsWithMessageStart[Statements](testName, "Invalid input '\u0885': expected \",\" or <EOF>")
  }

  test("""GRANT LOAD ON DATABASE foo TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'DATABASE': expected""")
  }

  test("""DENY LOAD ON HOME GRAPH TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'HOME': expected""")
  }

  test("""REVOKE GRANT LOAD ON DBMS ON ALL DATA FROM role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'DBMS': expected""")
  }

  test("""GRANT LOAD ON CIDR TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'TO': expected""")
  }

  test("""GRANT LOAD ON URL TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'TO': expected""")
  }

  test("""DENY LOAD TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'TO': expected""")
  }

  test("""GRANT LOAD ON CIDR "1.2.3.4/22" URL "https://example.com" TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'URL': expected""")
  }

  test("""DENY LOAD ON CIDR "1.2.3.4/22" ON URL "https://example.com" TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'ON': expected""")
  }

  test("""GRANT LOAD ON URL "https://www.badger.com","file:///test.csv" TO role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input ',': expected""")
  }

  test("""REVOKE DENY LOAD ON CIDR "1.2.3.4/22","1.2.3.4/22" FROM role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input ',': expected""")
  }

  test("""REVOKE DENY LOAD ON ALL DATA "1.2.3.4/22" FROM role""") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input '1.2.3.4/22': expected""")
  }

  test("GRANT LOAD ON CIDR 'x'+'y' TO role") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input '+': expected "TO""")
  }

  test("GRANT LOAD ON URL ['x'] TO role") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input '[': expected "\"", "\'" or a parameter""")
  }

  // help methods

  def grantLoadPrivilege(
    a: ast.LoadActions,
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(
      ast.LoadPrivilege(a)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r
    )

  def denyLoadPrivilege(
    a: ast.LoadActions,
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(
      ast.LoadPrivilege(a)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r
    )

  def revokeGrantLoadPrivilege(
    a: ast.LoadActions,
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(a)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeGrantType()(pos)
    )

  def revokeDenyLoadPrivilege(
    a: ast.LoadActions,
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(a)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeDenyType()(pos)
    )

  def revokeLoadPrivilege(
    a: ast.LoadActions,
    q: ast.LoadPrivilegeQualifier,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.LoadPrivilege(a)(InputPosition.NONE),
      i,
      Some(ast.FileResource()(InputPosition.NONE)),
      List(q),
      r,
      ast.RevokeBothType()(pos)
    )
}
