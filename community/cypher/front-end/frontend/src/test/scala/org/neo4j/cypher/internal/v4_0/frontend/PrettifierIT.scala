/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.parser.CypherParser
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, WindowsStringSafe}

class PrettifierIT extends CypherFunSuite {
  private implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  val parser = new CypherParser
  val tests: Seq[(String, String)] =
    Seq[(String, String)](
      "return 42" -> "RETURN 42",
      "return 42 as x" -> "RETURN 42 AS x",
      "return 42 as `43`" -> "RETURN 42 AS `43`",
      "return distinct 42" -> "RETURN DISTINCT 42",

      "return distinct a, b as X, 3+3 as six order by b.prop, b.foo descending skip 1 limit 2" ->
        """RETURN DISTINCT a, b AS X, 3 + 3 AS six
          |  ORDER BY b.prop ASCENDING, b.foo DESCENDING
          |  SKIP 1
          |  LIMIT 2""".stripMargin,

      "match (a) return a" ->
        """MATCH (a)
          |RETURN a""".stripMargin,

      "match (a) where a.prop = 42 return a" ->
        """MATCH (a)
          |  WHERE a.prop = 42
          |RETURN a""".stripMargin,

      "match (a) with distinct a, b as X, 3+3 as six order by b.prop, b.foo descending skip 1 limit 2 where true" ->
        """MATCH (a)
          |WITH DISTINCT a, b AS X, 3 + 3 AS six
          |  ORDER BY b.prop ASCENDING, b.foo DESCENDING
          |  SKIP 1
          |  LIMIT 2
          |  WHERE true""".stripMargin,

      "create (a)--(b) RETURN a" ->
        """CREATE (a)--(b)
          |RETURN a""".stripMargin,

      "match (a:Label {prop: 1}) RETURN a" ->
        """MATCH (a:Label {prop: 1})
          |RETURN a""".stripMargin,

      "unwind [1,2,3] AS x RETURN x" ->
        """UNWIND [1, 2, 3] AS x
          |RETURN x""".stripMargin,

      "CALL nsp.proc()" ->
        """CALL nsp.proc()""".stripMargin,

      "CALL proc()" ->
        """CALL proc()""".stripMargin,

      "CALL nsp1.nsp2.proc()" ->
        """CALL nsp1.nsp2.proc()""".stripMargin,

      "CALL nsp.proc(a)" ->
        """CALL nsp.proc(a)""".stripMargin,

      "CALL nsp.proc(a,b)" ->
        """CALL nsp.proc(a, b)""".stripMargin,

      "CALL nsp.proc() yield x" ->
        """CALL nsp.proc()
          |  YIELD x""".stripMargin,

      "CALL nsp.proc() yield x, y" ->
        """CALL nsp.proc()
          |  YIELD x, y""".stripMargin,

      "match (n) SET n.prop = 1" ->
        """MATCH (n)
          |SET n.prop = 1""".stripMargin,

      "match (n) SET n.prop = 1, n.prop2 = 2" ->
        """MATCH (n)
          |SET n.prop = 1, n.prop2 = 2""".stripMargin,

      "match (n) SET n:Label" ->
        """MATCH (n)
          |SET n:Label""".stripMargin,

      "match (n) SET n:`La bel`" ->
        """MATCH (n)
          |SET n:`La bel`""".stripMargin,

      "match (n) SET n:Label:Bla" ->
        """MATCH (n)
          |SET n:Label:Bla""".stripMargin,

      "match (n) SET n += {prop: 1}" ->
        """MATCH (n)
          |SET n += {prop: 1}""".stripMargin,

      "match (n) SET n = {prop: 1}" ->
        """MATCH (n)
          |SET n = {prop: 1}""".stripMargin,

      "match (n) SET n:Label, n.prop = 1" ->
        """MATCH (n)
          |SET n:Label, n.prop = 1""".stripMargin,

      "match (n) DELETE n" ->
        """MATCH (n)
          |DELETE n""".stripMargin,

      "match (n), (m) DELETE n, m" ->
        """MATCH (n), (m)
          |DELETE n, m""".stripMargin,

      "merge (n)" ->
        "MERGE (n)",

      "merge (n)--(m)" ->
        "MERGE (n)--(m)",

      "merge (n:Label {prop:1})--(m)" ->
        "MERGE (n:Label {prop: 1})--(m)",

      "create INDEX ON :A(p)" ->
        "CREATE INDEX ON :A(p)",

      "create INDEX ON :A(p1, p2, p3)" ->
        "CREATE INDEX ON :A(p1, p2, p3)",

      "drop INDEX ON :A(p)" ->
        "DROP INDEX ON :A(p)",

      "drop INDEX ON :A(p1, p2, p3)" ->
        "DROP INDEX ON :A(p1, p2, p3)",

      "create CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY" ->
        "CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY",

      "create CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
        "CREATE CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

      "drop CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY" ->
        "DROP CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY",

      "drop CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
        "DROP CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

      "create CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE" ->
        "CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE",

      "create CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
        "CREATE CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

      "drop CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE" ->
        "DROP CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE",

      "drop CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
        "DROP CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

      "create CONSTRAINT ON (a:A) ASSERT exists(a.p)" ->
        "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.p)",

      "drop CONSTRAINT ON (a:A) ASSERT exists(a.p)" ->
        "DROP CONSTRAINT ON (a:A) ASSERT exists(a.p)",

      "create CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)" ->
        "CREATE CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)",

      "drop CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)" ->
        "DROP CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)",

      "match (n) UNION match (n)" ->
        """MATCH (n)
          |UNION
          |MATCH (n)""".stripMargin,

      "match (n) UNION ALL match (n)" ->
        """MATCH (n)
          |UNION ALL
          |MATCH (n)""".stripMargin,

      "match (n) UNION match (n) UNION ALL RETURN $node AS n" ->
        """MATCH (n)
          |UNION
          |MATCH (n)
          |UNION ALL
          |RETURN $node AS n""".stripMargin,

      "create user abc set password 'foo'" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED",

      "create user abc set password $password" ->
        "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED",

      "create user `ab%$c` set password 'foo'" ->
        "CREATE USER `ab%$c` SET PASSWORD '******' CHANGE REQUIRED",

      "create user abc set password 'foo' change required" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED",

      "create user abc set password 'foo' change not required" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED",

      "create user abc set password 'foo' set status active" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS ACTIVE",

      "create user abc set password 'foo' change required set status active" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS ACTIVE",

      "create user abc set password 'foo' change not required set status active" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED SET STATUS ACTIVE",

      "create user abc set password $password change not required set status active" ->
        "CREATE USER abc SET PASSWORD $password CHANGE NOT REQUIRED SET STATUS ACTIVE",

      "create user abc set password 'foo' set status suspended" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS SUSPENDED",

      "create user abc set password 'foo' change required set status suspended" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS SUSPENDED",

      "create user abc set password 'foo' change not required set status suspended" ->
        "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED SET STATUS SUSPENDED",

      "alter user abc set password 'foo'" ->
        "ALTER USER abc SET PASSWORD '******'",

      "alter user abc set password $password" ->
        "ALTER USER abc SET PASSWORD $password",

      "alter user `ab%$c` set password 'foo'" ->
        "ALTER USER `ab%$c` SET PASSWORD '******'",

      "alter user abc set status active" ->
        "ALTER USER abc SET STATUS ACTIVE",

      "alter user abc set password 'foo' change required set status active" ->
        "ALTER USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS ACTIVE",

      "alter user abc set password 'foo' change required set status suspended" ->
        "ALTER USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS SUSPENDED",

      "alter user abc set password $password change not required set status suspended" ->
        "ALTER USER abc SET PASSWORD $password CHANGE NOT REQUIRED SET STATUS SUSPENDED",

      "alter user abc set password change not required set status suspended" ->
        "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED",

      "alter user abc set password change not required set status active" ->
        "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE",

      "drop user abc" ->
        "DROP USER abc",

      "drop user `ab%$c`" ->
        "DROP USER `ab%$c`",

      "alter current user set password from 'foo' to 'bar'" ->
        "ALTER CURRENT USER SET PASSWORD FROM '******' TO '******'",

      "alter current user set password from $currentPassword to 'bar'" ->
        "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO '******'",

      "alter current user set password from 'foo' to $newPassword" ->
        "ALTER CURRENT USER SET PASSWORD FROM '******' TO $newPassword",

      "alter current user set password from $currentPassword to $newPassword" ->
        "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword",

      "create role abc" ->
        "CREATE ROLE abc",

      "create role `ab%$c`" ->
        "CREATE ROLE `ab%$c`",

      "create role abc as copy of def" ->
        "CREATE ROLE abc AS COPY OF def",

      "create role `ab%$c` as copy of `$d3f`" ->
        "CREATE ROLE `ab%$c` AS COPY OF `$d3f`",

      "drop role abc" ->
        "DROP ROLE abc",

      "drop role `ab%$c`" ->
        "DROP ROLE `ab%$c`",

      "grant role abc to xyz" ->
        "GRANT ROLE abc TO xyz",

      "grant roles abc to xyz" ->
        "GRANT ROLE abc TO xyz",

      "grant roles abc to xyz, qwe" ->
        "GRANT ROLE abc TO xyz, qwe",

      "grant role abc to xyz, qwe" ->
        "GRANT ROLE abc TO xyz, qwe",

      "grant role `ab%$c` to `x%^yz`" ->
        "GRANT ROLE `ab%$c` TO `x%^yz`",

      "grant roles abc, def to xyz" ->
        "GRANT ROLES abc, def TO xyz",

      "grant roles abc, def to xyz, qwr" ->
        "GRANT ROLES abc, def TO xyz, qwr",

      "revoke role abc from xyz" ->
        "REVOKE ROLE abc FROM xyz",

      "revoke roles abc from xyz" ->
        "REVOKE ROLE abc FROM xyz",

      "revoke role abc, def from xyz" ->
        "REVOKE ROLES abc, def FROM xyz",

      "revoke roles abc, def from xyz" ->
        "REVOKE ROLES abc, def FROM xyz",

      "revoke role abc from xyz, qwr" ->
        "REVOKE ROLE abc FROM xyz, qwr",

      "revoke roles abc, def from xyz, qwr" ->
        "REVOKE ROLES abc, def FROM xyz, qwr",

      "revoke role `ab%$c` from `x%^yz`" ->
        "REVOKE ROLE `ab%$c` FROM `x%^yz`",

      "show privileges" ->
        "SHOW ALL PRIVILEGES",

      "show all privileges" ->
        "SHOW ALL PRIVILEGES",

      "show user abc privileges" ->
        "SHOW USER abc PRIVILEGES",

      "show  user `$aB%x`  privileges" ->
        "SHOW USER `$aB%x` PRIVILEGES",

      "show user `$user` privileges" ->
        "SHOW USER `$user` PRIVILEGES",

      "show role abc privileges" ->
        "SHOW ROLE abc PRIVILEGES",

      "show  role `$aB%x`  privileges" ->
        "SHOW ROLE `$aB%x` PRIVILEGES",

      "show role `$role` privileges" ->
        "SHOW ROLE `$role` PRIVILEGES",

      "catalog show databases" ->
        "SHOW DATABASES",

      "catalog show default database" ->
        "SHOW DEFAULT DATABASE",

      "catalog show database foO_Bar_42" ->
        "SHOW DATABASE foO_Bar_42",

      "catalog create database foO_Bar_42" ->
        "CREATE DATABASE foO_Bar_42",

      "catalog create database `foO_Bar_42`" ->
        "CREATE DATABASE foO_Bar_42",

      "catalog create database `graph.db`" ->
        "CREATE DATABASE `graph.db`",

      "catalog DROP database foO_Bar_42" ->
        "DROP DATABASE foO_Bar_42",

      "catalog start database foO_Bar_42" ->
        "START DATABASE foO_Bar_42",

      "start database foO_Bar_42" ->
        "START DATABASE foO_Bar_42",

      "catalog stop database foO_Bar_42" ->
        "STOP DATABASE foO_Bar_42",

      "stop database foO_Bar_42" ->
        "STOP DATABASE foO_Bar_42",

      "catalog create graph com.neo4j.Users { MATCH (n) RETURN n }" ->
        """CATALOG CREATE GRAPH com.neo4j.Users {
          |MATCH (n)
          |RETURN n
          |}""".stripMargin,

      "catalog DROP graph com.neo4j.Users" ->
        "CATALOG DROP GRAPH com.neo4j.Users",

      "catalog create VIEW com.neo4j.Users($p, $k) { MATCH (n) WHERE n.p=$p RETURN n LIMIT $k }" ->
        """CATALOG CREATE VIEW com.neo4j.Users($p, $k) {
          |MATCH (n)
          |  WHERE n.p = $p
          |RETURN n
          |  LIMIT $k
          |}""".stripMargin,

      "catalog DROP VIEW com.neo4j.Users" ->
        "CATALOG DROP VIEW com.neo4j.Users",

      "load csv from '/import/data.csv' AS row create ({key: row[0]})" ->
        """LOAD CSV FROM "/import/data.csv" AS row
          |CREATE ({key: row[0]})""".stripMargin,

      "load csv WITH headers from '/import/data.csv' AS row create ({key: row[0]})" ->
        """LOAD CSV WITH HEADERS FROM "/import/data.csv" AS row
          |CREATE ({key: row[0]})""".stripMargin,

      "load csv from '/import/data.csv' AS row FIELDTERMINATOR '-' create ({key: row[0]})" ->
        """LOAD CSV FROM "/import/data.csv" AS row FIELDTERMINATOR "-"
          |CREATE ({key: row[0]})""".stripMargin,

      "USING periodic commit 30 load csv from '/import/data.csv' AS row create ({key: row[0]})" ->
        """USING PERIODIC COMMIT 30
          |LOAD CSV FROM "/import/data.csv" AS row
          |CREATE ({key: row[0]})""".stripMargin,

      "FOREACH ( n IN [1,2,3] | create ({key: n}) CREATE ({foreignKey: n}) )" ->
        """FOREACH ( n IN [1, 2, 3] |
          |  CREATE ({key: n})
          |  CREATE ({foreignKey: n})
          |)""".stripMargin,

      "create unique (a)--(b) RETURN a" ->
        """CREATE UNIQUE (a)--(b)
          |RETURN a""".stripMargin
    ) ++ startTests("node") ++ startTests("relationship") ++ privilegeTests()

  def privilegeTests(): Seq[(String, String)] = {
    Seq(("GRANT", "TO"), ("REVOKE", "FROM")) flatMap {
      case (action, preposition) =>
        Seq(
          s"$action traverse on graph * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * ELEMENTS * (*) $preposition role",

          s"$action traverse on graph * nodes * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * NODES * (*) $preposition role",

          s"$action traverse on graph * nodes * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * NODES * (*) $preposition role",

          s"$action traverse on graph foo nodes * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo NODES * (*) $preposition role",

          s"$action traverse on graph foo nodes A (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo NODES A (*) $preposition role",

          s"$action traverse on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH `#%¤` NODES `()/&` (*) $preposition role",

          s"$action traverse on graph foo nodes A,B,C (*) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo NODES A, B, C (*) $preposition x, y, z",

          s"$action traverse on graph * relationships * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * RELATIONSHIPS * (*) $preposition role",

          s"$action traverse on graph * relationships * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * RELATIONSHIPS * (*) $preposition role",

          s"$action traverse on graph foo relationships * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) $preposition role",

          s"$action traverse on graph foo relationships A (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) $preposition role",

          s"$action traverse on graph `#%¤` relationships `()/&` (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH `#%¤` RELATIONSHIPS `()/&` (*) $preposition role",

          s"$action traverse on graph foo relationships A,B,C (*) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo RELATIONSHIPS A, B, C (*) $preposition x, y, z",

          s"$action read (*) on graph * $preposition role" ->
            s"$action READ (*) ON GRAPH * ELEMENTS * (*) $preposition role",

          s"$action read (*) on graph * nodes * $preposition role" ->
            s"$action READ (*) ON GRAPH * NODES * (*) $preposition role",

          s"$action read (*) on graph * nodes * (*) $preposition role" ->
            s"$action READ (*) ON GRAPH * NODES * (*) $preposition role",

          s"$action read (*) on graph foo nodes * (*) $preposition role" ->
            s"$action READ (*) ON GRAPH foo NODES * (*) $preposition role",

          s"$action read (*) on graph foo nodes A (*) $preposition role" ->
            s"$action READ (*) ON GRAPH foo NODES A (*) $preposition role",

          s"$action read (bar) on graph foo nodes A (*) $preposition role" ->
            s"$action READ (bar) ON GRAPH foo NODES A (*) $preposition role",

          s"$action read ( `&bar` ) on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action READ (`&bar`) ON GRAPH `#%¤` NODES `()/&` (*) $preposition role",

          s"$action read (foo,bar) on graph foo nodes A,B,C (*) $preposition x,y,z" ->
            s"$action READ (foo, bar) ON GRAPH foo NODES A, B, C (*) $preposition x, y, z",

          s"$action write (*) on graph * $preposition role" ->
            s"$action WRITE (*) ON GRAPH * ELEMENTS * (*) $preposition role",

          s"$action write (*) on graph * elements * $preposition role" ->
            s"$action WRITE (*) ON GRAPH * ELEMENTS * (*) $preposition role",

          s"$action write (*) on graph * elements * (*) $preposition role" ->
            s"$action WRITE (*) ON GRAPH * ELEMENTS * (*) $preposition role",

          s"$action write (*) on graph foo $preposition role" ->
            s"$action WRITE (*) ON GRAPH foo ELEMENTS * (*) $preposition role",

          s"$action write (*) on graph foo elements * (*) $preposition role" ->
            s"$action WRITE (*) ON GRAPH foo ELEMENTS * (*) $preposition role",

          s"$action write (*) on graphs foo elements * (*) $preposition role" ->
            s"$action WRITE (*) ON GRAPH foo ELEMENTS * (*) $preposition role"
        )
    }
  }

  def startTests(entityType: String): Seq[(String, String)] = {
    val ENTITYTYPE = entityType.toUpperCase
    Seq(
      s"START x=$entityType(*) RETURN x" ->
        s"""START x = $ENTITYTYPE( * )
           |RETURN x""".stripMargin,

      s"START x=$entityType(42) RETURN x" ->
        s"""START x = $ENTITYTYPE( 42 )
           |RETURN x""".stripMargin,

      s"START x=$entityType(42,101) RETURN x" ->
        s"""START x = $ENTITYTYPE( 42, 101 )
           |RETURN x""".stripMargin,

      s"START x=$entityType($$param) RETURN x" ->
        s"""START x = $ENTITYTYPE( $$param )
           |RETURN x""".stripMargin,

      s"START x=$entityType($$param), y=$entityType(42,101) RETURN x, y" ->
        s"""START x = $ENTITYTYPE( $$param ),
           |      y = $ENTITYTYPE( 42, 101 )
           |RETURN x, y""".stripMargin
    )
  }

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults: Statement = parser.parse(inputString)
        val str = prettifier.asString(parsingResults)
        str should equal(expected)
      }
  }

}
