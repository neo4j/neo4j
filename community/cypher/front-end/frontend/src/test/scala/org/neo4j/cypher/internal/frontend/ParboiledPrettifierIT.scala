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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class ParboiledPrettifierIT extends CypherFunSuite {
  private implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  val parser = new CypherParser
  val tests: Seq[(String, String)] = queryTests() ++ schemaTests() ++ administrationTests()

  def queryTests(): Seq[(String, String)] = Seq[(String, String)](
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

    "CALL nsp.proc() yield x where x > 2" ->
      """CALL nsp.proc()
        |  YIELD x
        |    WHERE x > 2""".stripMargin,

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
        |)""".stripMargin
  )

  def schemaTests(): Seq[(String, String)] = Seq[(String, String)](
    "create INDEX ON :A(p)" ->
      "CREATE INDEX ON :A(p)",

    "create INDEX ON :A(p1, p2, p3)" ->
      "CREATE INDEX ON :A(p1, p2, p3)",

    "create INDEX FOR (n:A) ON (n.p)" ->
      "CREATE INDEX FOR (n:A) ON (n.p)",

    "create btree INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)",

    "create INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE INDEX foo FOR (n:A) ON (n.p)",

    "create INDEX `foo` FOR (n:A) ON (n.p)" ->
      "CREATE INDEX foo FOR (n:A) ON (n.p)",

    "create INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",

    "CREATE index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'native-btree-1.0'}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "native-btree-1.0"}""",

    "create BTREE INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'lucene+native-3.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "lucene+native-3.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",

    "create BTREE INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",

    "CREATE index FOR (n:Person) on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",

    "CREATE index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name)""",

    "create or REPLACE INDEX FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE INDEX FOR (n:A) ON (n.p)",

    "create or REPLACE INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE INDEX foo FOR (n:A) ON (n.p)",

    "create INDEX IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE INDEX IF NOT EXISTS FOR (n:A) ON (n.p)",

    "create INDEX foo IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE INDEX foo IF NOT EXISTS FOR (n:A) ON (n.p)",

    "create INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE INDEX FOR ()-[n:R]-() ON (n.p)",

    "create btree INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",

    "create INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo FOR ()-[n:R]-() ON (n.p)",

    "create INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo FOR ()-[n:R]-() ON (n.p)",

    "create INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",

    "CREATE index FOR ()-[n:R]->() on (n.name) OPtiONS {indexProvider: 'native-btree-1.0'}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "native-btree-1.0"}""",

    "create BTREE INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'lucene+native-3.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "lucene+native-3.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",

    "create BTREE INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",

    "CREATE index FOR ()-[n:R]-() on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",

    "CREATE index FOR ()<-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name)""",

    "create or REPLACE INDEX FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE INDEX FOR ()-[n:R]-() ON (n.p)",

    "create or REPLACE INDEX foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE INDEX foo FOR ()-[n:R]-() ON (n.p)",

    "create INDEX IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    "create INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    "drop INDEX ON :A(p)" ->
      "DROP INDEX ON :A(p)",

    "drop INDEX ON :A(p1, p2, p3)" ->
      "DROP INDEX ON :A(p1, p2, p3)",

    "drop INDEX foo" ->
      "DROP INDEX foo",

    "drop INDEX `foo`" ->
      "DROP INDEX foo",

    "drop INDEX `$foo`" ->
      "DROP INDEX `$foo`",

    "drop INDEX foo if EXISTS" ->
      "DROP INDEX foo IF EXISTS",

    "show index" ->
      "SHOW ALL INDEXES",

    "show all inDEXES" ->
      "SHOW ALL INDEXES",

    "show indexes brief" ->
      "SHOW ALL INDEXES BRIEF",

    "show index verbose" ->
      "SHOW ALL INDEXES VERBOSE",

    "show BTREE index" ->
      "SHOW BTREE INDEXES",

    "show BTREE index BRIEF" ->
      "SHOW BTREE INDEXES BRIEF",

    "show BTREE index VERBOSE output" ->
      "SHOW BTREE INDEXES VERBOSE",

    "show \nindex\n verbose" ->
      "SHOW ALL INDEXES VERBOSE",

    "show index WHERE uniqueness = 'UNIQUE'" ->
      """SHOW ALL INDEXES
        |  WHERE uniqueness = "UNIQUE"""".stripMargin,

    "show btree inDEXES WHERE uniqueness = 'UNIQUE'" ->
      """SHOW BTREE INDEXES
        |  WHERE uniqueness = "UNIQUE"""".stripMargin,

    "show index  YIELD *" ->
      """SHOW ALL INDEXES
        |YIELD *""".stripMargin,

    "show index  YIELD * Return DISTINCT type" ->
      """SHOW ALL INDEXES
        |YIELD *
        |RETURN DISTINCT type""".stripMargin,

    "show index YIELD * where name = 'neo4j' Return *" ->
      """SHOW ALL INDEXES
        |YIELD *
        |  WHERE name = "neo4j"
        |RETURN *""".stripMargin,

    "show index yield name order by name skip 1 limit 1" ->
      """SHOW ALL INDEXES
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,

    "create CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create CONSTRAINT foo ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create CONSTRAINT `foo` ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create CONSTRAINT `$foo` ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT `$foo` ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create OR replace CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE OR REPLACE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create CONSTRAINT foo IF NOT EXISTS ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS ON (n:A) ASSERT (n.p) IS NODE KEY",

    "create CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

    "create CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

    "create CONSTRAINT `foo` ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

    "create CONSTRAINT `$foo` ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT `$foo` ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS NODE KEY OPtiONS {indexProvider: 'native-btree-1.0'}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY OPTIONS {indexProvider: "native-btree-1.0"}""",

    "create CONSTRAINT myConstraint ON (n:A) assert (n.p) IS NODE KEY OPTIONS {`indexProvider`: 'lucene+native-3.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE CONSTRAINT myConstraint ON (n:A) ASSERT (n.p) IS NODE KEY OPTIONS {indexProvider: "lucene+native-3.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",

    "create CONSTRAINT ON (n:A) assert (n.p) IS NODE KEY OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS NODE KEY OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS NODE KEY OPtiONS {}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY""",

    "drop CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY" ->
      "DROP CONSTRAINT ON (n:A) ASSERT (n.p) IS NODE KEY",

    "drop CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY" ->
      "DROP CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS NODE KEY",

    "create CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE",

    "create CONSTRAINT foo ON (n:A) ASSERT n.p IS UNIQUE" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p) IS UNIQUE",

    "create CONSTRAINT `foo` ON (n:A) ASSERT (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p) IS UNIQUE",

    "create CONSTRAINT `$foo` ON (n:A) ASSERT (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT `$foo` ON (n:A) ASSERT (n.p) IS UNIQUE",

    "create CONSTRAINT IF NoT ExistS ON (n:A) ASSERT (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT IF NOT EXISTS ON (n:A) ASSERT (n.p) IS UNIQUE",

    "create or REPLACE CONSTRAINT foo ON (n:A) ASSERT n.p IS UNIQUE" ->
      "CREATE OR REPLACE CONSTRAINT foo ON (n:A) ASSERT (n.p) IS UNIQUE",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS UNIQUE OPtiONS {indexProvider: 'native-btree-1.0'}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE OPTIONS {indexProvider: "native-btree-1.0"}""",

    "create CONSTRAINT myConstraint ON (n:A) assert (n.p) IS UNIQUE OPTIONS {`indexProvider`: 'lucene+native-3.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE CONSTRAINT myConstraint ON (n:A) ASSERT (n.p) IS UNIQUE OPTIONS {indexProvider: "lucene+native-3.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",

    "create CONSTRAINT ON (n:A) assert (n.p) IS UNIQUE OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS UNIQUE OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",

    "CREATE constraint ON (n:A) ASSERT (n.p) IS UNIQUE OPtiONS {}" ->
      """CREATE CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE""",

    "create CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

    "create CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

    "create CONSTRAINT `foo` ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT foo ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

    "create CONSTRAINT `$foo` ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT `$foo` ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

    "drop CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE" ->
      "DROP CONSTRAINT ON (n:A) ASSERT (n.p) IS UNIQUE",

    "drop CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE" ->
      "DROP CONSTRAINT ON (n:A) ASSERT (n.p1, n.p2) IS UNIQUE",

    "create CONSTRAINT ON (a:A) ASSERT EXISTS (a.p)" ->
      "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.p)",

    "create CONSTRAINT ON (a:A) ASSERT (a.p) is not null" ->
      "CREATE CONSTRAINT ON (a:A) ASSERT (a.p) IS NOT NULL",

    "create CONSTRAINT foo ON (a:A) ASSERT (a.p) IS NoT NulL" ->
      "CREATE CONSTRAINT foo ON (a:A) ASSERT (a.p) IS NOT NULL",

    "create CONSTRAINT `foo` ON (a:A) ASSERT (a.p) IS NOT NULL OPTIONS {}" ->
      "CREATE CONSTRAINT foo ON (a:A) ASSERT (a.p) IS NOT NULL OPTIONS {}",

    "create CONSTRAINT `foo` ON (a:A) ASSERT a.p IS NOT NULL OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT foo ON (a:A) ASSERT (a.p) IS NOT NULL OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",

    "create CONSTRAINT `$foo` ON (a:A) ASSERT a.p IS NOT NULL" ->
      "CREATE CONSTRAINT `$foo` ON (a:A) ASSERT (a.p) IS NOT NULL",

    "create OR replace CONSTRAINT ON (a:A) ASSERT a.p IS NOT NULL" ->
      "CREATE OR REPLACE CONSTRAINT ON (a:A) ASSERT (a.p) IS NOT NULL",

    "create CONSTRAINT foo if not EXISTS ON (a:A) ASSERT a.p IS NOT NULL" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS ON (a:A) ASSERT (a.p) IS NOT NULL",

    "drop CONSTRAINT ON (a:A) ASSERT exists(a.p)" ->
      "DROP CONSTRAINT ON (a:A) ASSERT exists(a.p)",

    "create CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.p)" ->
      "CREATE CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)",

    "create CONSTRAINT ON ()-[r:R]-() ASSERT r.p is not nULl" ->
      "CREATE CONSTRAINT ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL",

    "create CONSTRAINT foo ON ()-[r:R]->() ASSERT (r.p) IS NOT NULL" ->
      "CREATE CONSTRAINT foo ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL",

    "create CONSTRAINT `foo` ON ()<-[r:R]-() ASSERT r.p is NOT null" ->
      "CREATE CONSTRAINT foo ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL",

    "create CONSTRAINT `$foo` ON ()-[r:R]-() ASSERT r.p IS NOT NULL OPTIONS {}" ->
      "CREATE CONSTRAINT `$foo` ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL OPTIONS {}",

    "create CONSTRAINT `$foo` ON ()-[r:R]-() ASSERT r.p IS NOT NULL OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT `$foo` ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",

    "create CONSTRAINT IF not exists ON ()-[r:R]-() ASSERT r.p IS NOT NULL" ->
      "CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL",

    "create or Replace CONSTRAINT foo ON ()-[r:R]-() ASSERT r.p IS NOT NULL" ->
      "CREATE OR REPLACE CONSTRAINT foo ON ()-[r:R]-() ASSERT (r.p) IS NOT NULL",

    "drop CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)" ->
      "DROP CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)",

    "drop CONSTRAINT foo" ->
      "DROP CONSTRAINT foo",

    "drop CONSTRAINT `foo`" ->
      "DROP CONSTRAINT foo",

    "drop CONSTRAINT `$foo`" ->
      "DROP CONSTRAINT `$foo`",

    "drop CONSTRAINT foo IF exists" ->
      "DROP CONSTRAINT foo IF EXISTS",

    "show constraints" ->
      "SHOW ALL CONSTRAINTS",

    "show exists constraint brief" ->
      "SHOW EXISTS CONSTRAINTS BRIEF",

    "show exist constraint" ->
      "SHOW EXIST CONSTRAINTS",

    "show property existence constraint" ->
      "SHOW PROPERTY EXISTENCE CONSTRAINTS",

    "SHOW NODE EXISTS constraint BRIEF output" ->
      "SHOW NODE EXISTS CONSTRAINTS BRIEF",

    "SHOW NODE EXIST constraint" ->
      "SHOW NODE EXIST CONSTRAINTS",

    "SHOW NODE property EXIST constraint" ->
      "SHOW NODE PROPERTY EXISTENCE CONSTRAINTS",

    "show relationship EXISTS cOnStRaInTs VERBOSE" ->
      "SHOW RELATIONSHIP EXISTS CONSTRAINTS VERBOSE",

    "show relationship EXIST cOnStRaInTs" ->
      "SHOW RELATIONSHIP EXIST CONSTRAINTS",

    "show relationship EXISTENCE cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",

    "show rel EXIST cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",

    "show rel property EXISTence cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",

    "show unique constraint VERBOSE output" ->
      "SHOW UNIQUE CONSTRAINTS VERBOSE",

    "show node key CONSTRAINTS" ->
      "SHOW NODE KEY CONSTRAINTS",

    "show constraints WHERE entityType = 'NODE'" ->
      """SHOW ALL CONSTRAINTS
        |  WHERE entityType = "NODE"""".stripMargin,

    "show node KEY CONstraints WHERE entityType = 'NODE'" ->
      """SHOW NODE KEY CONSTRAINTS
        |  WHERE entityType = "NODE"""".stripMargin,

    "show constraint  YIELD *" ->
      """SHOW ALL CONSTRAINTS
        |YIELD *""".stripMargin,

    "show UNIQUE constraint  YIELD * Return DISTINCT type" ->
      """SHOW UNIQUE CONSTRAINTS
        |YIELD *
        |RETURN DISTINCT type""".stripMargin,

    "show existence constraint YIELD * where name = 'neo4j' Return *" ->
      """SHOW PROPERTY EXISTENCE CONSTRAINTS
        |YIELD *
        |  WHERE name = "neo4j"
        |RETURN *""".stripMargin,

    "show constraint yield name order by name skip 1 limit 1" ->
      """SHOW ALL CONSTRAINTS
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,
  )

  def administrationTests(): Seq[(String, String)] = Seq[(String, String)](
    "Show Users" ->
      "SHOW USERS",

    "Show Users where user = 'neo4j'" ->
      """SHOW USERS
        |  WHERE user = "neo4j"""".stripMargin,

    "Show Users YIELD * where user = 'neo4j' Return *" ->
      """SHOW USERS
        |  YIELD *
        |    WHERE user = "neo4j"
        |  RETURN *""".stripMargin,

    "Show Users YIELD * Return DISTINCT roles, user" ->
      """SHOW USERS
        |  YIELD *
        |  RETURN DISTINCT roles, user""".stripMargin,

    "show users yield user order by user skip 1 limit 1 where user='neo4j'" ->
      """SHOW USERS
        |  YIELD user
        |    ORDER BY user ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE user = "neo4j"""".stripMargin,

    "Show Current User" ->
      "SHOW CURRENT USER",

    "Show Current User where user = 'neo4j'" ->
      """SHOW CURRENT USER
        |  WHERE user = "neo4j"""".stripMargin,

    "Show Current User YIELD * where user = 'neo4j' Return *" ->
      """SHOW CURRENT USER
        |  YIELD *
        |    WHERE user = "neo4j"
        |  RETURN *""".stripMargin,

    "Show Current User YIELD * Return DISTINCT roles, user" ->
      """SHOW CURRENT USER
        |  YIELD *
        |  RETURN DISTINCT roles, user""".stripMargin,

    "show current user yield user order by user skip 1 limit 1 where user='neo4j'" ->
      """SHOW CURRENT USER
        |  YIELD user
        |    ORDER BY user ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE user = "neo4j"""".stripMargin,

    "create user abc set password 'foo'" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED",

    "create user abc set password $password" ->
      "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED",

    "create user $abc set password $password" ->
      "CREATE USER $abc SET PASSWORD $password CHANGE REQUIRED",

    "create user `ab%$c` if not exists set password 'foo'" ->
      "CREATE USER `ab%$c` IF NOT EXISTS SET PASSWORD '******' CHANGE REQUIRED",

    "create or replace user `ab%$c` set password 'foo'" ->
      "CREATE OR REPLACE USER `ab%$c` SET PASSWORD '******' CHANGE REQUIRED",

    "create user abc set password 'foo' change required" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED",

    "create user abc set password 'foo' change not required" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED",

    "create user abc set password 'foo' set status active" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS ACTIVE",

    "create user abc if not exists set password 'foo' change required set status active" ->
      "CREATE USER abc IF NOT EXISTS SET PASSWORD '******' CHANGE REQUIRED SET STATUS ACTIVE",

    "create user abc set password 'foo' change not required set status active" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED SET STATUS ACTIVE",

    "create or replace user abc set password 'foo' change not required set status active" ->
      "CREATE OR REPLACE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED SET STATUS ACTIVE",

    "create user abc set password $password change not required set status active" ->
      "CREATE USER abc SET PASSWORD $password CHANGE NOT REQUIRED SET STATUS ACTIVE",

    "create user abc set password 'foo' set status suspended" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS SUSPENDED",

    "create user abc set password 'foo' change required set status suspended" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED SET STATUS SUSPENDED",

    "create user abc set password 'foo' change not required set status suspended" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE NOT REQUIRED SET STATUS SUSPENDED",

    "create user abc set encrypted password 'foo'" ->
      "CREATE USER abc SET ENCRYPTED PASSWORD '******' CHANGE REQUIRED",

    "create user abc set encrypted password $password" ->
      "CREATE USER abc SET ENCRYPTED PASSWORD $password CHANGE REQUIRED",

    "create user abc set plaintext password 'foo'" ->
      "CREATE USER abc SET PASSWORD '******' CHANGE REQUIRED",

    "create user abc set plaintext password $password" ->
      "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED",

    "rename user alice to bob" ->
      "RENAME USER alice TO bob",

    "rename user `a%i$e` if exists to `b!b`" ->
      "RENAME USER `a%i$e` IF EXISTS TO `b!b`",

    "rename user $param to bob" ->
      "RENAME USER $param TO bob",

    "rename user alice to $other" ->
      "RENAME USER alice TO $other",

    "rename user $param IF EXISTS to $other" ->
      "RENAME USER $param IF EXISTS TO $other",

    "alter user abc set password 'foo'" ->
      "ALTER USER abc SET PASSWORD '******'",

    "alter user $abc set password 'foo'" ->
      "ALTER USER $abc SET PASSWORD '******'",

    "alter user abc set password $password" ->
      "ALTER USER abc SET PASSWORD $password",

    "alter user `ab%$c` if exists set password 'foo'" ->
      "ALTER USER `ab%$c` IF EXISTS SET PASSWORD '******'",

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

    "alter user abc if EXiSTS set password change not required set status active" ->
      "ALTER USER abc IF EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE",

    "alter user abc set encrypted password 'foo'" ->
      "ALTER USER abc SET ENCRYPTED PASSWORD '******'",

    "alter user $abc set encrypted password 'foo'" ->
      "ALTER USER $abc SET ENCRYPTED PASSWORD '******'",

    "alter user abc set encrypted password $password" ->
      "ALTER USER abc SET ENCRYPTED PASSWORD $password",

    "alter user abc set plaintext password 'foo'" ->
      "ALTER USER abc SET PASSWORD '******'",

    "alter user abc set plaintext password $password" ->
      "ALTER USER abc SET PASSWORD $password",

    "alter user abc set status active set home database db1 set password change not required" ->
      "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE SET HOME DATABASE db1",

    "alter user abc set status active set password change not required" ->
      "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE",

    "alter user abc set home database null" ->
      "ALTER USER abc SET HOME DATABASE null", // this is the string "null"

    "alter user abc remove home database" ->
      "ALTER USER abc REMOVE HOME DATABASE",

    "alter user abc if exists remove home database" ->
      "ALTER USER abc IF EXISTS REMOVE HOME DATABASE",

    "drop user abc" ->
      "DROP USER abc",

    "drop user $abc" ->
      "DROP USER $abc",

    "drop user abc if exists" ->
      "DROP USER abc IF EXISTS",

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

    "Show Roles" ->
      "SHOW ALL ROLES",

    "Show roles where role = 'admin'" ->
      """SHOW ALL ROLES
        |  WHERE role = "admin"""".stripMargin,

    "Show Roles YIELD * where role = 'admin' Return *" ->
      """SHOW ALL ROLES
        |  YIELD *
        |    WHERE role = "admin"
        |  RETURN *""".stripMargin,

    "Show Roles YIELD * Return DISTINCT role" ->
      """SHOW ALL ROLES
        |  YIELD *
        |  RETURN DISTINCT role""".stripMargin,

    "show roles yield role order by role skip 1 limit 1 where role='admin'" ->
      """SHOW ALL ROLES
        |  YIELD role
        |    ORDER BY role ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE role = "admin"""".stripMargin,

    "Show Roles with users" ->
      "SHOW ALL ROLES WITH USERS",

    "Show roles with users where role = 'admin'" ->
      """SHOW ALL ROLES WITH USERS
        |  WHERE role = "admin"""".stripMargin,

    "Show roles with users YIELD * where member = 'neo4j' Return *" ->
      """SHOW ALL ROLES WITH USERS
        |  YIELD *
        |    WHERE member = "neo4j"
        |  RETURN *""".stripMargin,

    "Show roles with users YIELD * Return DISTINCT member, role" ->
      """SHOW ALL ROLES WITH USERS
        |  YIELD *
        |  RETURN DISTINCT member, role""".stripMargin,

    "show roles with users yield member order by member skip 1 limit 1 where member='neo4j'" ->
      """SHOW ALL ROLES WITH USERS
        |  YIELD member
        |    ORDER BY member ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE member = "neo4j"""".stripMargin,

    "Show Populated Roles" ->
      "SHOW POPULATED ROLES",

    "Show Populated roles where role = 'admin'" ->
      """SHOW POPULATED ROLES
        |  WHERE role = "admin"""".stripMargin,

    "Show populated Roles YIELD * where role = 'admin' Return *" ->
      """SHOW POPULATED ROLES
        |  YIELD *
        |    WHERE role = "admin"
        |  RETURN *""".stripMargin,

    "Show populated Roles YIELD * Return DISTINCT role" ->
      """SHOW POPULATED ROLES
        |  YIELD *
        |  RETURN DISTINCT role""".stripMargin,

    "show Populated roles yield role order by role skip 1 limit 1 where role='admin'" ->
      """SHOW POPULATED ROLES
        |  YIELD role
        |    ORDER BY role ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE role = "admin"""".stripMargin,

    "Show Populated Roles with users" ->
      "SHOW POPULATED ROLES WITH USERS",

    "Show Populated roles with users where member = 'neo4j'" ->
      """SHOW POPULATED ROLES WITH USERS
        |  WHERE member = "neo4j"""".stripMargin,

    "Show populated roles with users YIELD * where role = 'admin' Return *" ->
      """SHOW POPULATED ROLES WITH USERS
        |  YIELD *
        |    WHERE role = "admin"
        |  RETURN *""".stripMargin,

    "Show populated roles with users YIELD * Return DISTINCT member, role" ->
      """SHOW POPULATED ROLES WITH USERS
        |  YIELD *
        |  RETURN DISTINCT member, role""".stripMargin,

    "show Populated roles with users yield member order by member skip 1 limit 1 where member='neo4j'" ->
      """SHOW POPULATED ROLES WITH USERS
        |  YIELD member
        |    ORDER BY member ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE member = "neo4j"""".stripMargin,

    "create role abc" ->
      "CREATE ROLE abc",

    "create role $abc" ->
      "CREATE ROLE $abc",

    "create role abc if not exists" ->
      "CREATE ROLE abc IF NOT EXISTS",

    "create or replace role abc" ->
      "CREATE OR REPLACE ROLE abc",

    "create role `ab%$c`" ->
      "CREATE ROLE `ab%$c`",

    "create role abc as copy of def" ->
      "CREATE ROLE abc AS COPY OF def",

    "create role abc as copy of $def" ->
      "CREATE ROLE abc AS COPY OF $def",

    "create role `ab%$c` if not exists as copy of `$d3f`" ->
      "CREATE ROLE `ab%$c` IF NOT EXISTS AS COPY OF `$d3f`",

    "create or replace role `ab%$c` as copy of `$d3f`" ->
      "CREATE OR REPLACE ROLE `ab%$c` AS COPY OF `$d3f`",

    "rename role abc to cba" ->
      "RENAME ROLE abc TO cba",

    "rename role `a%b$c` if exists to `c!b?a`" ->
      "RENAME ROLE `a%b$c` IF EXISTS TO `c!b?a`",

    "rename role $param to cba" ->
      "RENAME ROLE $param TO cba",

    "rename role abc to $other" ->
      "RENAME ROLE abc TO $other",

    "rename role $param IF EXISTS to $other" ->
      "RENAME ROLE $param IF EXISTS TO $other",

    "drop role abc" ->
      "DROP ROLE abc",

    "drop role $abc" ->
      "DROP ROLE $abc",

    "drop role abc if exists" ->
      "DROP ROLE abc IF EXISTS",

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

    "show privilege where action = 'match'" ->
      """SHOW ALL PRIVILEGES
        |  WHERE action = "match"""".stripMargin,

    "Show privileges YIELD * where action = 'match' Return *" ->
      """SHOW ALL PRIVILEGES
        |  YIELD *
        |    WHERE action = "match"
        |  RETURN *""".stripMargin,

    "Show privileges YIELD * Return DISTINCT action, role" ->
      """SHOW ALL PRIVILEGES
        |  YIELD *
        |  RETURN DISTINCT action, role""".stripMargin,

    "show user privileges yield user order by user skip 1 limit 1 where user='neo4j'" ->
      """SHOW USER PRIVILEGES
        |  YIELD user
        |    ORDER BY user ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE user = "neo4j"""".stripMargin,

    "show user abc privileges" ->
      "SHOW USER abc PRIVILEGES",

    "show  users `$aB%x`  privileges" ->
      "SHOW USER `$aB%x` PRIVILEGES",

    "show user `$user` privilege" ->
      "SHOW USER `$user` PRIVILEGES",

    "show user $user privileges" ->
      "SHOW USER $user PRIVILEGES",

    "show user abc,$user,edf privileges" ->
      "SHOW USERS abc, $user, edf PRIVILEGES",

    "show users $user1,abc,$user2,edf privileges" ->
      "SHOW USERS $user1, abc, $user2, edf PRIVILEGES",

    "show user privileges" ->
      "SHOW USER PRIVILEGES",

    "show users privilege" ->
      "SHOW USER PRIVILEGES",

    "show role abc privileges" ->
      "SHOW ROLE abc PRIVILEGES",

    "show  role `$aB%x`  privileges" ->
      "SHOW ROLE `$aB%x` PRIVILEGES",

    "show roles `$role` privileges" ->
      "SHOW ROLE `$role` PRIVILEGES",

    "show role $role privileges" ->
      "SHOW ROLE $role PRIVILEGES",

    "show role $role1,abc, $role2 privileges" ->
      "SHOW ROLES $role1, abc, $role2 PRIVILEGES",

    "show roles $role1,abc, $role2,$role3 privilege" ->
      "SHOW ROLES $role1, abc, $role2, $role3 PRIVILEGES",

    "show privileges as command" ->
      "SHOW ALL PRIVILEGES AS COMMANDS",

    "show privilege as commands" ->
      "SHOW ALL PRIVILEGES AS COMMANDS",

    "show privileges as revoke command" ->
      "SHOW ALL PRIVILEGES AS REVOKE COMMANDS",

    "show user privilege as revoke command" ->
      "SHOW USER PRIVILEGES AS REVOKE COMMANDS",

    "show user user privileges as command" ->
      "SHOW USER user PRIVILEGES AS COMMANDS",

    "show user $bar privilege as command" ->
      "SHOW USER $bar PRIVILEGES AS COMMANDS",

    "show user foo, $bar privileges as command" ->
      "SHOW USERS foo, $bar PRIVILEGES AS COMMANDS",

    "show role role privileges as revoke command" ->
      "SHOW ROLE role PRIVILEGES AS REVOKE COMMANDS",

    "show role $bar privilege as command" ->
      "SHOW ROLE $bar PRIVILEGES AS COMMANDS",

    "show role foo, $bar privileges as command" ->
      "SHOW ROLES foo, $bar PRIVILEGES AS COMMANDS",

    "show privileges as revoke command yield command order by command" ->
      """SHOW ALL PRIVILEGES AS REVOKE COMMANDS
        |  YIELD command
        |    ORDER BY command ASCENDING""".stripMargin,

    "show user privileges as commands where command CONTAINS 'MATCH' and command CONTAINS 'NODE'" ->
      """SHOW USER PRIVILEGES AS COMMANDS
        |  WHERE command CONTAINS "MATCH" AND command CONTAINS "NODE"""".stripMargin,

    "catalog show databases" ->
      "SHOW DATABASES",

    "Show Databases YIELD * where name = 'neo4j' Return *" ->
      """SHOW DATABASES
        |  YIELD *
        |    WHERE name = "neo4j"
        |  RETURN *""".stripMargin,

    "Show Databases YIELD * Return DISTINCT default, name" ->
      """SHOW DATABASES
        |  YIELD *
        |  RETURN DISTINCT default, name""".stripMargin,

    "catalog show default database" ->
      "SHOW DEFAULT DATABASE",

    "catalog show database foO_Bar_42" ->
      "SHOW DATABASE foO_Bar_42",

    "show database $foo" ->
      "SHOW DATABASE $foo",

    "show database $foo yield name order by name skip 1 limit 1 where name='neo4j'" ->
      """SHOW DATABASE $foo
        |  YIELD name
        |    ORDER BY name ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE name = "neo4j"""".stripMargin,

    "show home database" ->
      "SHOW HOME DATABASE",

    "catalog create database foO_Bar_42" ->
      "CREATE DATABASE foO_Bar_42",

    "create database $foo" ->
      "CREATE DATABASE $foo",

    "catalog create database `foO_Bar_42`" ->
      "CREATE DATABASE foO_Bar_42",

    "catalog create database `foO_Bar_42` if not exists" ->
      "CREATE DATABASE foO_Bar_42 IF NOT EXISTS",

    "catalog create or replace database `foO_Bar_42`" ->
      "CREATE OR REPLACE DATABASE foO_Bar_42",

    "catalog create database `graph.db`" ->
      "CREATE DATABASE `graph.db`",

    "catalog create database graph.db" ->
      "CREATE DATABASE `graph.db`",

    "catalog create database graph.db wait" ->
      "CREATE DATABASE `graph.db` WAIT",

    "catalog create database graph.db nowait" ->
      "CREATE DATABASE `graph.db`",

    "catalog create database graph.db if not exists wait" ->
      "CREATE DATABASE `graph.db` IF NOT EXISTS WAIT",

    "catalog DROP database foO_Bar_42" ->
      "DROP DATABASE foO_Bar_42 DESTROY DATA",

    "DROP database $foo" ->
      "DROP DATABASE $foo DESTROY DATA",

    "catalog DROP database foO_Bar_42 if EXISTS" ->
      "DROP DATABASE foO_Bar_42 IF EXISTS DESTROY DATA",

    "catalog DROP database blah if EXISTS WAIT" ->
      "DROP DATABASE blah IF EXISTS DESTROY DATA WAIT",

    "DROP database foO_Bar_42 dump Data" ->
      "DROP DATABASE foO_Bar_42 DUMP DATA",

    "DROP database foO_Bar_42 Destroy DATA" ->
      "DROP DATABASE foO_Bar_42 DESTROY DATA",

    "catalog start database foO_Bar_42" ->
      "START DATABASE foO_Bar_42",

    "start database $foo" ->
      "START DATABASE $foo",

    "start database foO_Bar_42" ->
      "START DATABASE foO_Bar_42",

    "catalog start database graph.db" ->
      "START DATABASE `graph.db`",

    "catalog stop database foO_Bar_42" ->
      "STOP DATABASE foO_Bar_42",

    "stop database $foo" ->
      "STOP DATABASE $foo",

    "stop database foO_Bar_42" ->
      "STOP DATABASE foO_Bar_42",
  ) ++ privilegeTests()

  def privilegeTests(): Seq[(String, String)] = {
    Seq(
      ("GRANT", "TO"),
      ("DENY", "TO"),
      ("REVOKE GRANT", "FROM"),
      ("REVOKE DENY", "FROM"),
      ("REVOKE", "FROM")
    ) flatMap {
      case (action, preposition) =>
        Seq(
          s"$action traverse on graph * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * ELEMENTS * $preposition role",

          s"$action traverse on graph * nodes * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * NODES * $preposition role",

          s"$action traverse on graph * nodes * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * NODES * $preposition role",

          s"$action traverse on graph foo nodes * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo NODES * $preposition role",

          s"$action traverse on graph $$foo nodes * (*) $preposition $$role" ->
            s"$action TRAVERSE ON GRAPH $$foo NODES * $preposition $$role",

          s"$action traverse on graph FoO nodes A (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO NODE A $preposition role",

          s"$action traverse on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH `#%¤` NODE `()/&` $preposition role",

          s"$action traverse on graph foo nodes A,B,C (*) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo NODES A, B, C $preposition x, y, z",

          s"$action traverse on graph * relationships * $preposition role" ->
            s"$action TRAVERSE ON GRAPH * RELATIONSHIPS * $preposition role",

          s"$action traverse on graph * relationships * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * RELATIONSHIPS * $preposition role",

          s"$action traverse on graph foo relationships * (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo RELATIONSHIPS * $preposition role",

          s"$action traverse on graph FoO relationships A (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO RELATIONSHIP A $preposition role",

          s"$action traverse on graph `#%¤` relationships `()/&` (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH `#%¤` RELATIONSHIP `()/&` $preposition role",

          s"$action traverse on graph foo relationships A,B,C (*) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo RELATIONSHIPS A, B, C $preposition x, y, z",

          s"$action traverse on graphs $$foo, $$bar nodes * (*) $preposition $$role" ->
            s"$action TRAVERSE ON GRAPHS $$foo, $$bar NODES * $preposition $$role",

          s"$action traverse on graph * elements A (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * ELEMENTS A $preposition role",

          s"$action traverse on home graph elements A (*) $preposition role" ->
            s"$action TRAVERSE ON HOME GRAPH ELEMENTS A $preposition role",

          s"$action traverse on default graph elements A (*) $preposition role" ->
            s"$action TRAVERSE ON DEFAULT GRAPH ELEMENTS A $preposition role",

          s"$action read {*} on graph * $preposition role" ->
            s"$action READ {*} ON GRAPH * ELEMENTS * $preposition role",

          s"$action read {*} on graph * nodes * $preposition role" ->
            s"$action READ {*} ON GRAPH * NODES * $preposition role",

          s"$action read {*} on graph * nodes * (*) $preposition role" ->
            s"$action READ {*} ON GRAPH * NODES * $preposition role",

          s"$action read {*} on graph foo node * (*) $preposition role" ->
            s"$action READ {*} ON GRAPH foo NODES * $preposition role",

          s"$action read {*} on graph foo nodes A (*) $preposition role" ->
            s"$action READ {*} ON GRAPH foo NODE A $preposition role",

          s"$action read {bar} on graph FoO nodes A (*) $preposition role" ->
            s"$action READ {bar} ON GRAPH FoO NODE A $preposition role",

          s"$action read { `&bar` } on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action READ {`&bar`} ON GRAPH `#%¤` NODE `()/&` $preposition role",

          s"$action read {foo,bar} on graph foo nodes A,B,C (*) $preposition x,y,$$z" ->
            s"$action READ {foo, bar} ON GRAPH foo NODES A, B, C $preposition x, y, $$z",

          s"$action read {*} on graph $$foo relationships * (*) $preposition role" ->
            s"$action READ {*} ON GRAPH $$foo RELATIONSHIPS * $preposition role",

          s"$action read {*} on graph foo, bar relationships * (*) $preposition role" ->
            s"$action READ {*} ON GRAPHS foo, bar RELATIONSHIPS * $preposition role",

          s"$action read {*} on graph * elements A (*) $preposition role" ->
            s"$action READ {*} ON GRAPH * ELEMENTS A $preposition role",

          s"$action read {*} on home graph elements A (*) $preposition role" ->
            s"$action READ {*} ON HOME GRAPH ELEMENTS A $preposition role",

          s"$action read {*} on default graph elements A (*) $preposition role" ->
            s"$action READ {*} ON DEFAULT GRAPH ELEMENTS A $preposition role",

          s"$action match {*} on graph * $preposition role" ->
            s"$action MATCH {*} ON GRAPH * ELEMENTS * $preposition role",

          s"$action match {*} on graph * node * $preposition role" ->
            s"$action MATCH {*} ON GRAPH * NODES * $preposition role",

          s"$action match {*} on graph * nodes * (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPH * NODES * $preposition role",

          s"$action match {*} on graph foo nodes * (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPH foo NODES * $preposition role",

          s"$action match {*} on graph foo nodes A (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPH foo NODE A $preposition role",

          s"$action match {bar} on graph foo nodes A (*) $preposition role" ->
            s"$action MATCH {bar} ON GRAPH foo NODE A $preposition role",

          s"$action match { `&bar` } on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action MATCH {`&bar`} ON GRAPH `#%¤` NODE `()/&` $preposition role",

          s"$action match {foo,bar} on graph foo nodes A,B,C (*) $preposition x,$$y,z" ->
            s"$action MATCH {foo, bar} ON GRAPH foo NODES A, B, C $preposition x, $$y, z",

          s"$action match {foo,bar} on graph $$foo relationship A,B,C (*) $preposition x,y,z" ->
            s"$action MATCH {foo, bar} ON GRAPH $$foo RELATIONSHIPS A, B, C $preposition x, y, z",

          s"$action match {*} on graph $$foo, bar nodes * (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPHS $$foo, bar NODES * $preposition role",

          s"$action match {*} on graph * elements A (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPH * ELEMENTS A $preposition role",

          s"$action match {*} on home graph nodes * (*) $preposition role" ->
            s"$action MATCH {*} ON HOME GRAPH NODES * $preposition role",

          s"$action match {*} on default graph nodes * (*) $preposition role" ->
            s"$action MATCH {*} ON DEFAULT GRAPH NODES * $preposition role",

          s"$action write on graph * $preposition role" ->
            s"$action WRITE ON GRAPH * $preposition role",

          s"$action write on graph foo $preposition role" ->
            s"$action WRITE ON GRAPH foo $preposition role",

          s"$action write on graph foo, $$bar $preposition role" ->
            s"$action WRITE ON GRAPHS foo, $$bar $preposition role",

          s"$action create on graph * $preposition role" ->
            s"$action CREATE ON GRAPH * ELEMENTS * $preposition role",

          s"$action create on graph * elements * $preposition role" ->
            s"$action CREATE ON GRAPH * ELEMENTS * $preposition role",

          s"$action create on graph * elements foo $preposition role" ->
            s"$action CREATE ON GRAPH * ELEMENTS foo $preposition role",

          s"$action create on graph foo $preposition role" ->
            s"$action CREATE ON GRAPH foo ELEMENTS * $preposition role",

          s"$action create on graph $$foo $preposition role" ->
            s"$action CREATE ON GRAPH $$foo ELEMENTS * $preposition role",

          s"$action create on graph foo nodes * $preposition role" ->
            s"$action CREATE ON GRAPH foo NODES * $preposition role",

          s"$action create on graphs FoO relationships * $preposition $$role" ->
            s"$action CREATE ON GRAPH FoO RELATIONSHIPS * $preposition $$role",

          s"$action create on graph foo, $$bar relationship * $preposition role" ->
            s"$action CREATE ON GRAPHS foo, $$bar RELATIONSHIPS * $preposition role",

          s"$action create on home graph relationship * $preposition role" ->
            s"$action CREATE ON HOME GRAPH RELATIONSHIPS * $preposition role",

          s"$action create on default graph relationship * $preposition role" ->
            s"$action CREATE ON DEFAULT GRAPH RELATIONSHIPS * $preposition role",

          s"$action delete on graph * $preposition role" ->
            s"$action DELETE ON GRAPH * ELEMENTS * $preposition role",

          s"$action delete on graph * elements * $preposition role" ->
            s"$action DELETE ON GRAPH * ELEMENTS * $preposition role",

          s"$action delete on graph * elements foo $preposition role" ->
            s"$action DELETE ON GRAPH * ELEMENTS foo $preposition role",

          s"$action delete on graph foo $preposition role" ->
            s"$action DELETE ON GRAPH foo ELEMENTS * $preposition role",

          s"$action delete on graph $$foo $preposition role" ->
            s"$action DELETE ON GRAPH $$foo ELEMENTS * $preposition role",

          s"$action delete on graph foo nodes * $preposition role" ->
            s"$action DELETE ON GRAPH foo NODES * $preposition role",

          s"$action delete on graphs FoO relationships * $preposition $$role" ->
            s"$action DELETE ON GRAPH FoO RELATIONSHIPS * $preposition $$role",

          s"$action delete on graph foo, $$bar relationship * $preposition role" ->
            s"$action DELETE ON GRAPHS foo, $$bar RELATIONSHIPS * $preposition role",

          s"$action delete on home graph relationship * $preposition role" ->
            s"$action DELETE ON HOME GRAPH RELATIONSHIPS * $preposition role",

          s"$action delete on default graph relationship * $preposition role" ->
            s"$action DELETE ON DEFAULT GRAPH RELATIONSHIPS * $preposition role",

          s"$action set label label on graph * $preposition role" ->
            s"$action SET LABEL label ON GRAPH * $preposition role",

          s"$action set label label1, label2 on graph * $preposition role" ->
            s"$action SET LABEL label1, label2 ON GRAPH * $preposition role",

          s"$action set label * on graph * $preposition role" ->
            s"$action SET LABEL * ON GRAPH * $preposition role",

          s"$action set label label on graph foo $preposition role1, role2, role3" ->
            s"$action SET LABEL label ON GRAPH foo $preposition role1, role2, role3",

          s"$action set label label on graph foo, $$bar $preposition role" ->
            s"$action SET LABEL label ON GRAPHS foo, $$bar $preposition role",

          s"$action set label label on home graph $preposition role" ->
            s"$action SET LABEL label ON HOME GRAPH $preposition role",

          s"$action set label label on default graph $preposition role" ->
            s"$action SET LABEL label ON DEFAULT GRAPH $preposition role",

          s"$action remove label label on graph * $preposition role" ->
            s"$action REMOVE LABEL label ON GRAPH * $preposition role",

          s"$action remove label label1, label2 on graph * $preposition role" ->
            s"$action REMOVE LABEL label1, label2 ON GRAPH * $preposition role",

          s"$action remove label * on graph * $preposition role" ->
            s"$action REMOVE LABEL * ON GRAPH * $preposition role",

          s"$action remove label label on graph foo $preposition role1, role2, role3" ->
            s"$action REMOVE LABEL label ON GRAPH foo $preposition role1, role2, role3",

          s"$action remove label label on graph foo, $$bar $preposition role" ->
            s"$action REMOVE LABEL label ON GRAPHS foo, $$bar $preposition role",

          s"$action remove label label on home graph $preposition role1, role2, role3" ->
            s"$action REMOVE LABEL label ON HOME GRAPH $preposition role1, role2, role3",

          s"$action remove label label on default graph $preposition role1, role2, role3" ->
            s"$action REMOVE LABEL label ON DEFAULT GRAPH $preposition role1, role2, role3",

          s"$action set property {*} on graph * $preposition role" ->
            s"$action SET PROPERTY {*} ON GRAPH * ELEMENTS * $preposition role",

          s"$action set property {foo} on graph * NODES bar $preposition role" ->
            s"$action SET PROPERTY {foo} ON GRAPH * NODE bar $preposition role",

          s"$action set property {*} on graph * RELATIONSHIPS bar, baz $preposition role" ->
            s"$action SET PROPERTY {*} ON GRAPH * RELATIONSHIPS bar, baz $preposition role",

          s"$action set property {Foo, BAR} on graph * $preposition role" ->
            s"$action SET PROPERTY {Foo, BAR} ON GRAPH * ELEMENTS * $preposition role",

          s"$action set property {*} on graph foo, $$bar $preposition role1, role2, role3" ->
            s"$action SET PROPERTY {*} ON GRAPHS foo, $$bar ELEMENTS * $preposition role1, role2, role3",

          s"$action set property {Foo, BAR} on home graph $preposition role" ->
            s"$action SET PROPERTY {Foo, BAR} ON HOME GRAPH ELEMENTS * $preposition role",

          s"$action set property {Foo, BAR} on default graph $preposition role" ->
            s"$action SET PROPERTY {Foo, BAR} ON DEFAULT GRAPH ELEMENTS * $preposition role",

          s"$action all on graph * $preposition role" ->
            s"$action ALL GRAPH PRIVILEGES ON GRAPH * $preposition role",

          s"$action all privileges on graph foo $preposition role" ->
            s"$action ALL GRAPH PRIVILEGES ON GRAPH foo $preposition role",

          s"$action all graph privileges on graph foo, $$bar $preposition role1, role2, role3" ->
            s"$action ALL GRAPH PRIVILEGES ON GRAPHS foo, $$bar $preposition role1, role2, role3",

          s"$action all privileges on home graph $preposition role" ->
            s"$action ALL GRAPH PRIVILEGES ON HOME GRAPH $preposition role",

          s"$action all privileges on default graph $preposition role" ->
            s"$action ALL GRAPH PRIVILEGES ON DEFAULT GRAPH $preposition role",

          s"$action merge {*} on graph * $preposition role" ->
            s"$action MERGE {*} ON GRAPH * ELEMENTS * $preposition role",

          s"$action merge {foo} on graph * NODES bar $preposition role" ->
            s"$action MERGE {foo} ON GRAPH * NODE bar $preposition role",

          s"$action merge {*} on graph * RELATIONSHIPS bar, baz $preposition role" ->
            s"$action MERGE {*} ON GRAPH * RELATIONSHIPS bar, baz $preposition role",

          s"$action merge {Foo, BAR} on graph * $preposition role" ->
            s"$action MERGE {Foo, BAR} ON GRAPH * ELEMENTS * $preposition role",

          s"$action merge {*} on graph foo, $$bar $preposition role1, role2, role3" ->
            s"$action MERGE {*} ON GRAPHS foo, $$bar ELEMENTS * $preposition role1, role2, role3",

          s"$action merge {Foo, BAR} on home graph $preposition role" ->
            s"$action MERGE {Foo, BAR} ON HOME GRAPH ELEMENTS * $preposition role",

          s"$action merge {Foo, BAR} on default graph $preposition role" ->
            s"$action MERGE {Foo, BAR} ON DEFAULT GRAPH ELEMENTS * $preposition role",

        ) ++ Seq(
          ("access", "ACCESS"),
          ("start", "START"),
          ("stop", "STOP"),
          ("create index", "CREATE INDEX"),
          ("create indexes", "CREATE INDEX"),
          ("drop index", "DROP INDEX"),
          ("drop indexes", "DROP INDEX"),
          ("show index", "SHOW INDEX"),
          ("show indexes", "SHOW INDEX"),
          ("index", "INDEX MANAGEMENT"),
          ("indexes", "INDEX MANAGEMENT"),
          ("index management", "INDEX MANAGEMENT"),
          ("indexes management", "INDEX MANAGEMENT"),
          ("create constraint", "CREATE CONSTRAINT"),
          ("create constraints", "CREATE CONSTRAINT"),
          ("drop constraint", "DROP CONSTRAINT"),
          ("drop constraints", "DROP CONSTRAINT"),
          ("show constraint", "SHOW CONSTRAINT"),
          ("show constraints", "SHOW CONSTRAINT"),
          ("constraint", "CONSTRAINT MANAGEMENT"),
          ("constraints", "CONSTRAINT MANAGEMENT"),
          ("constraint management", "CONSTRAINT MANAGEMENT"),
          ("constraints management", "CONSTRAINT MANAGEMENT"),
          ("create new label", "CREATE NEW NODE LABEL"),
          ("create new node label", "CREATE NEW NODE LABEL"),
          ("create new type", "CREATE NEW RELATIONSHIP TYPE"),
          ("create new relationship type", "CREATE NEW RELATIONSHIP TYPE"),
          ("create new name", "CREATE NEW PROPERTY NAME"),
          ("create new property name", "CREATE NEW PROPERTY NAME"),
          ("name", "NAME MANAGEMENT"),
          ("name management", "NAME MANAGEMENT"),
          ("all", "ALL DATABASE PRIVILEGES"),
          ("all privileges", "ALL DATABASE PRIVILEGES"),
          ("all database privileges", "ALL DATABASE PRIVILEGES")
        ).flatMap {
          case (databaseAction, prettifiedDatabaseAction) =>
            Seq(
              s"$action $databaseAction on database * $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE * $preposition role",

              s"$action $databaseAction on databases * $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE * $preposition role",

              s"$action $databaseAction on database foo $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE foo $preposition role",

              s"$action $databaseAction on database foo, bar $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASES foo, bar $preposition role",

              s"$action $databaseAction on database $$foo $preposition $$role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE $$foo $preposition $$role",

              s"$action $databaseAction on databases $$foo, bar $preposition $$role" ->
                s"$action $prettifiedDatabaseAction ON DATABASES $$foo, bar $preposition $$role",

              s"$action $databaseAction on databases FoO $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE FoO $preposition role",

              s"$action $databaseAction on databases F.o.O $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE `F.o.O` $preposition role",

              s"$action $databaseAction on home database $preposition role" ->
                s"$action $prettifiedDatabaseAction ON HOME DATABASE $preposition role",

              s"$action $databaseAction on default database $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DEFAULT DATABASE $preposition role",
            )
        } ++ Seq(
          s"$action show transaction (*) on database * $preposition role" ->
            s"$action SHOW TRANSACTION (*) ON DATABASE * $preposition role",

          s"$action show transactions (*) on database foo $preposition role" ->
            s"$action SHOW TRANSACTION (*) ON DATABASE foo $preposition role",

          s"$action show transactions (*) on database $$foo $preposition role" ->
            s"$action SHOW TRANSACTION (*) ON DATABASE $$foo $preposition role",

          s"$action show transaction (foo,$$bar) on home database $preposition $$role" ->
            s"$action SHOW TRANSACTION (foo, $$bar) ON HOME DATABASE $preposition $$role",

          s"$action show transaction (foo,$$bar) on default database $preposition $$role" ->
            s"$action SHOW TRANSACTION (foo, $$bar) ON DEFAULT DATABASE $preposition $$role",

          s"$action terminate transaction (*) on database * $preposition role" ->
            s"$action TERMINATE TRANSACTION (*) ON DATABASE * $preposition role",

          s"$action terminate transactions (*) on database foo $preposition role" ->
            s"$action TERMINATE TRANSACTION (*) ON DATABASE foo $preposition role",

          s"$action terminate transactions (*) on database $$foo $preposition $$role" ->
            s"$action TERMINATE TRANSACTION (*) ON DATABASE $$foo $preposition $$role",

          s"$action terminate transaction (foo,$$bar) on home database $preposition role" ->
            s"$action TERMINATE TRANSACTION (foo, $$bar) ON HOME DATABASE $preposition role",

          s"$action terminate transaction (foo,$$bar) on default database $preposition role" ->
            s"$action TERMINATE TRANSACTION (foo, $$bar) ON DEFAULT DATABASE $preposition role",

          s"$action transaction on database * $preposition role" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role",

          s"$action transaction (*) on database * $preposition role" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role",

          s"$action transaction management on database foo $preposition $$role" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DATABASE foo $preposition $$role",

          s"$action transaction management on database $$foo $preposition role" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DATABASE $$foo $preposition role",

          s"$action transaction management (*) on database foo $preposition role" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DATABASE foo $preposition role",

          s"$action transaction management (user1,$$user2) on database foo $preposition role" ->
            s"$action TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASE foo $preposition role",

          s"$action transaction management on home database $preposition role1,$$role2" ->
            s"$action TRANSACTION MANAGEMENT (*) ON HOME DATABASE $preposition role1, $$role2",

          s"$action transaction management on default database $preposition role1,$$role2" ->
            s"$action TRANSACTION MANAGEMENT (*) ON DEFAULT DATABASE $preposition role1, $$role2",

          s"$action role management on dbms $preposition $$role" ->
            s"$action ROLE MANAGEMENT ON DBMS $preposition $$role",

          s"$action create role on dbms $preposition role" ->
            s"$action CREATE ROLE ON DBMS $preposition role",

          s"$action rename role on dbms $preposition role" ->
            s"$action RENAME ROLE ON DBMS $preposition role",

          s"$action drop role on dbms $preposition role" ->
            s"$action DROP ROLE ON DBMS $preposition role",

          s"$action assign role on dbms $preposition role" ->
            s"$action ASSIGN ROLE ON DBMS $preposition role",

          s"$action remove role on dbms $preposition role" ->
            s"$action REMOVE ROLE ON DBMS $preposition role",

          s"$action show role on dbms $preposition role" ->
            s"$action SHOW ROLE ON DBMS $preposition role",

          s"$action user management on dbms $preposition role" ->
            s"$action USER MANAGEMENT ON DBMS $preposition role",

          s"$action create user on dbms $preposition $$role" ->
            s"$action CREATE USER ON DBMS $preposition $$role",

          s"$action rename user on dbms $preposition role" ->
            s"$action RENAME USER ON DBMS $preposition role",

          s"$action drop user on dbms $preposition role" ->
            s"$action DROP USER ON DBMS $preposition role",

          s"$action show user on dbms $preposition role" ->
            s"$action SHOW USER ON DBMS $preposition role",

          s"$action set password on dbms $preposition role" ->
            s"$action SET PASSWORDS ON DBMS $preposition role",

          s"$action set passwords on dbms $preposition role" ->
            s"$action SET PASSWORDS ON DBMS $preposition role",

          s"$action set user status on dbms $preposition role" ->
            s"$action SET USER STATUS ON DBMS $preposition role",

          s"$action set user home database on dbms $preposition role" ->
            s"$action SET USER HOME DATABASE ON DBMS $preposition role",

          s"$action alter user on dbms $preposition role" ->
            s"$action ALTER USER ON DBMS $preposition role",

          s"$action database management on dbms $preposition role" ->
            s"$action DATABASE MANAGEMENT ON DBMS $preposition role",

          s"$action create database on dbms $preposition role" ->
            s"$action CREATE DATABASE ON DBMS $preposition role",

          s"$action drop database on dbms $preposition $$role" ->
            s"$action DROP DATABASE ON DBMS $preposition $$role",

          s"$action privilege management on dbms $preposition role" ->
            s"$action PRIVILEGE MANAGEMENT ON DBMS $preposition role",

          s"$action show privilege on dbms $preposition role" ->
            s"$action SHOW PRIVILEGE ON DBMS $preposition role",

          s"$action assign privilege on dbms $preposition role" ->
            s"$action ASSIGN PRIVILEGE ON DBMS $preposition role",

          s"$action remove privilege on dbms $preposition $$role" ->
            s"$action REMOVE PRIVILEGE ON DBMS $preposition $$role",

          s"$action all on dbms $preposition role" ->
            s"$action ALL DBMS PRIVILEGES ON DBMS $preposition role",

          s"$action execute procedure * on dbms $preposition role" ->
            s"$action EXECUTE PROCEDURE * ON DBMS $preposition role",

          s"$action execute procedures * on dbms $preposition role" ->
            s"$action EXECUTE PROCEDURE * ON DBMS $preposition role",

          s"$action execute procedure math.sin, ma*.`*/a?`,math.`c%s` on dbms $preposition role" ->
            s"$action EXECUTE PROCEDURE math.sin, ma*.`*/a?`, math.`c%s` ON DBMS $preposition role",

          s"$action execute boosted procedure * on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE * ON DBMS $preposition role",

          s"$action execute boosted procedures * on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE * ON DBMS $preposition role",

          s"$action execute boosted procedure math.`s/n`, `ma/*`.`*a?`,math.cos on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE math.`s/n`, `ma/*`.*a?, math.cos ON DBMS $preposition role",

          s"$action execute admin procedures on dbms $preposition role" ->
            s"$action EXECUTE ADMIN PROCEDURES ON DBMS $preposition role",

          s"$action execute administrator procedures on dbms $preposition role" ->
            s"$action EXECUTE ADMIN PROCEDURES ON DBMS $preposition role",

          s"$action execute function * on dbms $preposition role" ->
            s"$action EXECUTE USER DEFINED FUNCTION * ON DBMS $preposition role",

          s"$action execute user function * on dbms $preposition role" ->
            s"$action EXECUTE USER DEFINED FUNCTION * ON DBMS $preposition role",

          s"$action execute user defined functions * on dbms $preposition role" ->
            s"$action EXECUTE USER DEFINED FUNCTION * ON DBMS $preposition role",

          s"$action execute boosted function math.sin, ma*.`*/a?`,math.`c%s` on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION math.sin, ma*.`*/a?`, math.`c%s` ON DBMS $preposition role",

          s"$action execute boosted user function apoc.math on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION apoc.math ON DBMS $preposition role",

          s"$action execute boosted user defined functions ??? on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION ??? ON DBMS $preposition role",
        )
    }
  }

  def startTests(entityType: String): Seq[(String, String)] = {
    // Not valid in JavaCC parser
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

  (tests ++ startTests("node") ++ startTests("relationship")) foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults: Statement = parser.parse(inputString, OpenCypherExceptionFactory(None))
        val str = prettifier.asString(parsingResults)
        str should equal(expected)
      }
  }

}
