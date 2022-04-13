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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class JavaCCPrettifierIT extends CypherFunSuite {
  private implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())
  val parboiledPrettifier = new ParboiledPrettifierIT()

  val javaCcOnlyTests: Seq[(String, String)] = Seq[(String, String)](
    "CALL nsp.proc() yield *" ->
      """CALL nsp.proc()
        |  YIELD *""".stripMargin,

    "MATCH (n WHERE n:N)" -> "MATCH (n WHERE n:N)",

    "MATCH (n:N WHERE n.prop > 0)" -> "MATCH (n:N WHERE n.prop > 0)",

    "MATCH (n:N {foo: 5} WHERE n.prop > 0)" -> "MATCH (n:N {foo: 5} WHERE n.prop > 0)",

    "call { create ( n ) } in transactions" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS""".stripMargin,

    "call { create ( n ) } in transactions of 1 row" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 1 ROWS""".stripMargin,

    "call { create ( n ) } in transactions of 10 rows" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 10 ROWS""".stripMargin,

    "call { create ( n ) } in transactions of $p rows" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF $p ROWS""".stripMargin,

    "alter database foo set ACCESS read only" ->
      "ALTER DATABASE foo SET ACCESS READ ONLY".stripMargin,

    "alteR databaSe foo if EXISTS SEt access read WRITE" ->
      "ALTER DATABASE foo IF EXISTS SET ACCESS READ WRITE".stripMargin,

    "create alias alias FOR database database" -> "CREATE ALIAS alias FOR DATABASE database",
    "create alias alias if not exists for database database" -> "CREATE ALIAS alias IF NOT EXISTS FOR DATABASE database",
    "create or replace alias alias FOR database database" -> "CREATE OR REPLACE ALIAS alias FOR DATABASE database",
    "create alias alias FOR database database at 'url' user user password 'password'" -> """CREATE ALIAS alias FOR DATABASE database AT "url" USER user PASSWORD '******'""",
    "create or replace alias alias FOR database database at 'url' user user password 'password' driver { ssl_enforced: $val }" -> """CREATE OR REPLACE ALIAS alias FOR DATABASE database AT "url" USER user PASSWORD '******' DRIVER {ssl_enforced: $val}""",
    "create alias $alias if not exists FOR database $database at $url user $user password $password driver { }" -> """CREATE ALIAS $alias IF NOT EXISTS FOR DATABASE $database AT $url USER $user PASSWORD $password DRIVER {}""",

    "alter alias alias if exists set database target database" -> "ALTER ALIAS alias IF EXISTS SET DATABASE TARGET database",
    "alter alias alias set database target database" -> "ALTER ALIAS alias SET DATABASE TARGET database",
    "alter alias $alias set database target $database at $url user $user password $password driver $driver" -> "ALTER ALIAS $alias SET DATABASE TARGET $database AT $url USER $user PASSWORD $password DRIVER $driver",
    "alter alias alias if exists set database target database at 'url'" -> """ALTER ALIAS alias IF EXISTS SET DATABASE TARGET database AT "url"""",
    "alter alias alias set database user user" -> "ALTER ALIAS alias SET DATABASE USER user",
    "alter alias alias set database password 'password'" -> "ALTER ALIAS alias SET DATABASE PASSWORD '******'",
    "alter alias alias set database driver { ssl_enforced: true }" -> "ALTER ALIAS alias SET DATABASE DRIVER {ssl_enforced: true}",

    "drop alias alias for database" -> "DROP ALIAS alias FOR DATABASE",
    "drop alias alias if exists for database" -> "DROP ALIAS alias IF EXISTS FOR DATABASE",

    "show alias for database" -> "SHOW ALIASES FOR DATABASE",
    "show aliases for database" -> "SHOW ALIASES FOR DATABASE",
    "show aliases for database YIELD * where name = 'neo4j' Return *" ->
      """SHOW ALIASES FOR DATABASE
        |  YIELD *
        |    WHERE name = "neo4j"
        |  RETURN *""".stripMargin,
    "show aliases for database YIELD * Return DISTINCT default, name" ->
      """SHOW ALIASES FOR DATABASE
        |  YIELD *
        |  RETURN DISTINCT default, name""".stripMargin,
    "show aliases for database yield name order by name skip 1 limit 1 where name='neo4j'" ->
      """SHOW ALIASES FOR DATABASE
        |  YIELD name
        |    ORDER BY name ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE name = "neo4j"""".stripMargin,

    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name = "Sweden"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name = "Sweden"""".stripMargin,

    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name EnDs WItH "eden"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name ENDS WITH "eden"""".stripMargin,

    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name COnTaIns "ede"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name CONTAINS "ede"""".stripMargin,

    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name =~ ".+eden?"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name =~ ".+eden?"""".stripMargin,

    "MATCH (c:Country) UsIng BtRee iNdEx c:Country(name)" ->
      """MATCH (c:Country)
        |  USING BTREE INDEX c:Country(name)""".stripMargin,

    "MATCH (c:Country)-[v:VISITED { year: 1972 } ]->() UsIng BtRee iNdEx v:VISITED(year)" ->
      """MATCH (c:Country)-[v:VISITED {year: 1972}]->()
        |  USING BTREE INDEX v:VISITED(year)""".stripMargin
  ) ++
  (
    Seq(
      ("GRANT", "TO"),
      ("DENY", "TO"),
      ("REVOKE GRANT", "FROM"),
      ("REVOKE DENY", "FROM"),
      ("REVOKE", "FROM")
    ) flatMap {
      case (action, preposition) =>

        Seq(s"$action alter database on dbms $preposition role" ->
          s"$action ALTER DATABASE ON DBMS $preposition role",

          s"$action set database access on dbms $preposition role" ->
            s"$action SET DATABASE ACCESS ON DBMS $preposition role",

          s"$action aliaS Management on dbms $preposition role" ->
            s"$action ALIAS MANAGEMENT ON DBMS $preposition role",

          s"$action  create alias on DBMS $preposition role" ->
            s"$action CREATE ALIAS ON DBMS $preposition role",

          s"$action dRoP aLiAs On DbMs $preposition role" ->
            s"$action DROP ALIAS ON DBMS $preposition role",

          s"$action Alter AliAs on dbms $preposition role" ->
            s"$action ALTER ALIAS ON DBMS $preposition role",

          s"$action show    alias on dbms $preposition role" ->
            s"$action SHOW ALIAS ON DBMS $preposition role")
    }
  )

  (parboiledPrettifier.tests ++ javaCcOnlyTests) foreach {
    case (inputString, expected) =>
      test(inputString) {
        try {
          val parsingResults: Statement = JavaCCParser.parse(inputString, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator())
          val str = prettifier.asString(parsingResults)
          str should equal(expected)
        } catch {
          case _: SyntaxException if JavaCCParser.shouldFallback(inputString) =>
          // Should not succeed in new parser so this is correct
        }
      }
  }

  test("Ensure tests don't include fallback triggers") {
    // Sanity check
    (parboiledPrettifier.queryTests() ++ javaCcOnlyTests) foreach {
      case (inputString, _) if JavaCCParser.shouldFallback(inputString) =>
        fail(s"should not use fallback strings in tests: $inputString")
      case _ =>
    }
  }
}
