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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class PrettifierIT extends CypherFunSuite {
  implicit private val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  val tests: Seq[(String, String)] =
    queryTests() ++ indexCommandTests() ++ constraintCommandTests() ++ showCommandTests() ++ administrationTests()

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
    "fiNIsh" -> "FINISH",
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
    "MATCH (n WHERE n:N)" -> "MATCH (n WHERE n:N)",
    "MATCH (n:(A| (B)))" -> "MATCH (n:A|B)",
    "MATCH (n:(A| ( B & %)))" -> "MATCH (n:A|(B&%))",
    "MATCH (n:((A|  B ) & %))" -> "MATCH (n:(A|B)&%)",
    "MATCH (n:(A&B)&C)" -> "MATCH (n:A&B&C)",
    "MATCH (n:A&(B&C))" -> "MATCH (n:A&B&C)",
    "MATCH (n) WHERE n:(A| (B))" ->
      """MATCH (n)
        |  WHERE n:A|B""".stripMargin,
    "MATCH (n:N WHERE n.prop > 0)" -> "MATCH (n:N WHERE n.prop > 0)",
    "MATCH (n:N {foo: 5} WHERE n.prop > 0)" -> "MATCH (n:N {foo: 5} WHERE n.prop > 0)",
    "create (a)--(b) RETURN a" ->
      """CREATE (a)--(b)
        |RETURN a""".stripMargin,
    "insert (n :label{prop:1})" -> "INSERT (n:label {prop: 1})",
    "insert (a)-[:R]->(b) RETURN a" ->
      """INSERT (a)-[:R]->(b)
        |RETURN a""".stripMargin,
    "match (a:Label {prop: 1}) RETURN a" ->
      """MATCH (a:Label {prop: 1})
        |RETURN a""".stripMargin,
    "unwind [1,2,3] AS x RETURN x" ->
      """UNWIND [1, 2, 3] AS x
        |RETURN x""".stripMargin,
    "mAtch all (a)-->(b)     rETuRN a" ->
      """MATCH (a)-->(b)
        |RETURN a""".stripMargin,
    "mAtch aNy 2 path (a)-->(b)     rETuRN a" ->
      """MATCH ANY 2 PATHS (a)-->(b)
        |RETURN a""".stripMargin,
    "mAtch aNy SHORTeST pathS (a)-->(b)     rETuRN a" ->
      """MATCH SHORTEST 1 PATHS (a)-->(b)
        |RETURN a""".stripMargin,
    "mAtch all SHORTeST path (a)-->(b)     rETuRN a" ->
      """MATCH ALL SHORTEST PATHS (a)-->(b)
        |RETURN a""".stripMargin,
    "mAtch SHORTeST path group (a)-->(b)     rETuRN a" ->
      """MATCH SHORTEST 1 PATH GROUPS (a)-->(b)
        |RETURN a""".stripMargin,
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
    "CALL nsp.proc() yield *" ->
      """CALL nsp.proc()
        |  YIELD *""".stripMargin,
    "call { Match (n)  where n:(A| (B)) finish }" ->
      """CALL {
        |  MATCH (n)
        |    WHERE n:A|B
        |  FINISH
        |}""".stripMargin,
    "call { create ( n ) } in transactions" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS""".stripMargin,
    "call { create ( n ) } in transactions of 1 row on error break" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 1 ROWS ON ERROR BREAK""".stripMargin,
    "call { create ( n ) } in transactions of 1 row on error continue" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 1 ROWS ON ERROR CONTINUE""".stripMargin,
    "call { create ( n ) } in transactions report status as s" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS REPORT STATUS AS s""".stripMargin,
    "call { create ( n ) } in transactions of 1 row on error fail report status as s" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 1 ROWS ON ERROR FAIL REPORT STATUS AS s""".stripMargin,
    "call { create ( n ) } in transactions of 10 rows" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 10 ROWS""".stripMargin,
    "call { create ( n ) } in transactions of $p rows" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF $p ROWS""".stripMargin,
    "call { create ( n ) } in transactions of 10 rows on error break" ->
      """CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS OF 10 ROWS ON ERROR BREAK""".stripMargin,
    "call { return 1 as i } in transactions of 10 rows report status as s on error break return s, i" ->
      """CALL {
        |  RETURN 1 AS i
        |} IN TRANSACTIONS OF 10 ROWS ON ERROR BREAK REPORT STATUS AS s
        |RETURN s, i""".stripMargin,
    "call { return 1 as i } in transactions on error break" ->
      """CALL {
        |  RETURN 1 AS i
        |} IN TRANSACTIONS ON ERROR BREAK""".stripMargin,
    "match (n) SET n:A   :$(B):   $(  1 + 1  + \"2\")  :D" ->
      """MATCH (n)
        |SET n:A:$(B):$((1 + 1) + "2"):D""".stripMargin,
    "match (n) SET n IS $([a,  b,   c])" ->
      """MATCH (n)
        |SET n IS $([a, b, c])""".stripMargin,
    "match (n) SET n.prop = 1" ->
      """MATCH (n)
        |SET n.prop = 1""".stripMargin,
    "match (n) SET n.prop = 1, n.prop2 = 2" ->
      """MATCH (n)
        |SET n.prop = 1, n.prop2 = 2""".stripMargin,
    "match (n) SET n:Label" ->
      """MATCH (n)
        |SET n:Label""".stripMargin,
    "match (n) SET n IS Label" ->
      """MATCH (n)
        |SET n IS Label""".stripMargin,
    "match (n) SET n:Label, m is Label2:Label3" ->
      """MATCH (n)
        |SET n:Label, m IS Label2:Label3""".stripMargin,
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
    "match (n) REMOVE n:A   :$(B):   $(  1 + 1  + \"2\")  :D" ->
      """MATCH (n)
        |REMOVE n:A:$(B):$((1 + 1) + "2"):D""".stripMargin,
    "match (n) REMOVE n IS $([a,  b,   c])" ->
      """MATCH (n)
        |REMOVE n IS $([a, b, c])""".stripMargin,
    "match (n) REMOVE n:Label" ->
      """MATCH (n)
        |REMOVE n:Label""".stripMargin,
    "match (n) REMOVE n[\"prop\"]" ->
      """MATCH (n)
        |REMOVE n["prop"]""".stripMargin,
    "match (n) REMOVE n IS Label" ->
      """MATCH (n)
        |REMOVE n IS Label""".stripMargin,
    "match (n) REMOVE n iS Label:Label1, m : Label2 : LaBeL3,    n[\"prop\" +     \"2\"]" ->
      """MATCH (n)
        |REMOVE n IS Label:Label1, m:Label2:LaBeL3, n[("prop" + "2")]""".stripMargin,
    "match (n) SET n[\"prop\"]    =    2" ->
      """MATCH (n)
        |SET n["prop"] = 2""".stripMargin,
    "match (n) SET n[\"prop\" +     \"2\"]    =    2" ->
      """MATCH (n)
        |SET n[("prop" + "2")] = 2""".stripMargin,
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
    "FOREACH ( n IN [1,2,3] | create ({key: n}) CREATE ({foreignKey: n}) )" ->
      """FOREACH ( n IN [1, 2, 3] |
        |  CREATE ({key: n})
        |  CREATE ({foreignKey: n})
        |)""".stripMargin,
    "MATCH (a)-[r:R|:Q]->(b)" ->
      "MATCH (a)-[r:R|:Q]->(b)",
    """MATCH (c:Country { name: "Sweden" }) UsInG ScAn c:Country""" ->
      """MATCH (c:Country {name: "Sweden"})
        |  USING SCAN c:Country""".stripMargin,
    """MATCH (c:Country)-[v:VISITED { year: 1950 }]->() UsInG ScAn v:VISITED""" ->
      """MATCH (c:Country)-[v:VISITED {year: 1950}]->()
        |  USING SCAN v:VISITED""".stripMargin,
    """MATCH (n) UsInG ScAn n:Country UsInG Scan n:City WHERE n:Country oR n:City""" ->
      """MATCH (n)
        |  USING SCAN n:Country
        |  USING SCAN n:City
        |  WHERE n:Country OR n:City""".stripMargin,
    """MATCH (p:Person)-[v:VISITED]->(c:Country) UsInG JoIn On v""" ->
      """MATCH (p:Person)-[v:VISITED]->(c:Country)
        |  USING JOIN ON v""".stripMargin,
    """MATCH (p:Person { born: 1950 })-[v:VISITED]->(c:Country { name: "Sweden"}) UsInG InDeX p:Person(born) UsInG InDex c:Country(name) USING JoIn On v""" ->
      """MATCH (p:Person {born: 1950})-[v:VISITED]->(c:Country {name: "Sweden"})
        |  USING INDEX p:Person(born)
        |  USING INDEX c:Country(name)
        |  USING JOIN ON v""".stripMargin,
    "MATCH (p:Person { born: 1950 }) OPTIONAL MATCH (p)-[:VISITED]->(c:Country) UsInG JoIn oN p RETURN *" ->
      """MATCH (p:Person {born: 1950})
        |OPTIONAL MATCH (p)-[:VISITED]->(c:Country)
        |  USING JOIN ON p
        |RETURN *""".stripMargin,
    "MATCH (c:Country) UsIng iNdEx c:Country(name)" ->
      """MATCH (c:Country)
        |  USING INDEX c:Country(name)""".stripMargin,
    "MATCH (c:Country)-[v:VISITED { year: 1972 } ]->() UsIng iNdEx v:VISITED(year)" ->
      """MATCH (c:Country)-[v:VISITED {year: 1972}]->()
        |  USING INDEX v:VISITED(year)""".stripMargin,
    """MATCH (c:Country { name: "Sweden"})-[v:VISITED { year: 1972 } ]->() UsIng iNdEx c:Country(name) UsInG INDeX v:VISITED(year)""" ->
      """MATCH (c:Country {name: "Sweden"})-[v:VISITED {year: 1972}]->()
        |  USING INDEX c:Country(name)
        |  USING INDEX v:VISITED(year)""".stripMargin,
    """MATCH (c:Country) UsIng iNdEx c:Country(name) UsInG INDeX c:Country(year_formed) WHERE c.formed = 500 OR c.name StARtS WiTh "A"""" ->
      """MATCH (c:Country)
        |  USING INDEX c:Country(name)
        |  USING INDEX c:Country(year_formed)
        |  WHERE c.formed = 500 OR c.name STARTS WITH "A"""".stripMargin,
    """MATCH (c:Country) UsInG RanGe INdEX c:Country(founded_year) WHERE c.founded_year > 1500""" ->
      """MATCH (c:Country)
        |  USING RANGE INDEX c:Country(founded_year)
        |  WHERE c.founded_year > 1500""".stripMargin,
    """MATCH (c:Country) UsInG RanGe INdEX c:Country(founded_year) WHERE 1500 < c.founded_year AND c.founded_year < 1700""" ->
      """MATCH (c:Country)
        |  USING RANGE INDEX c:Country(founded_year)
        |  WHERE 1500 < c.founded_year AND c.founded_year < 1700""".stripMargin,
    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name = "Sweden"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name = "Sweden"""".stripMargin,
    """MATCH (c:Country) UsIng TexT iNdEx c:Country(name) WHERE c.name STaRTS wITH "Swe"""" ->
      """MATCH (c:Country)
        |  USING TEXT INDEX c:Country(name)
        |  WHERE c.name STARTS WITH "Swe"""".stripMargin,
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
    "MATCH (c:City) UsInG POiNt InDEX c:City(coordinates) WHERE point.distance(c.coordinates, $p1, $p2) < 1000" ->
      """MATCH (c:City)
        |  USING POINT INDEX c:City(coordinates)
        |  WHERE point.distance(c.coordinates, $p1, $p2) < 1000""".stripMargin
  )

  def indexCommandTests(): Seq[(String, String)] = Seq[(String, String)](
    // index commands

    // default type

    "create INDEX FOR (n:A) ON (n.p)" ->
      "CREATE INDEX FOR (n:A) ON (n.p)",
    "create INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE INDEX foo FOR (n:A) ON (n.p)",
    "create INDEX `foo` FOR (n:A) ON (n.p)" ->
      "CREATE INDEX foo FOR (n:A) ON (n.p)",
    "create INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create INDEX $foo FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX $foo FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "CREATE index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "range-1.0"}""",
    "create INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE index FOR (n:Person) on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {}""",
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
    "create INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "CREATE index FOR ()-[n:R]->() on (n.name) OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "range-1.0"}""",
    "create INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE index FOR ()-[n:R]-() on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE index FOR ()<-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {}""",
    "create or REPLACE INDEX FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE INDEX FOR ()-[n:R]-() ON (n.p)",
    "create or REPLACE INDEX foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create INDEX IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",
    "create INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    // range

    "create RANGE INDEX FOR (n:A) ON (n.p)" ->
      "CREATE RANGE INDEX FOR (n:A) ON (n.p)",
    "create RANGE INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE RANGE INDEX foo FOR (n:A) ON (n.p)",
    "create RANGE INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE RANGE INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create RANGE INDEX $foo FOR (n:A) ON n.p" ->
      "CREATE RANGE INDEX $foo FOR (n:A) ON (n.p)",
    "CREATE RANGE index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE RANGE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "range-1.0"}""",
    "create RANGE INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE RANGE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create RANGE INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE RANGE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE RANGE index FOR (n:Person) on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE RANGE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE RANGE index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE RANGE INDEX FOR (n:Person) ON (n.name) OPTIONS {}""",
    "create or REPLACE RANGE INDEX FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE RANGE INDEX FOR (n:A) ON (n.p)",
    "create or REPLACE RANGE INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE RANGE INDEX foo FOR (n:A) ON (n.p)",
    "create RANGE INDEX IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE RANGE INDEX IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create RANGE INDEX foo IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE RANGE INDEX foo IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create RANGE INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.p)",
    "create RANGE INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create RANGE INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE RANGE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create RANGE INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE RANGE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "CREATE RANGE index FOR ()-[n:R]->() on (n.name) OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "range-1.0"}""",
    "create RANGE INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create RANGE INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE RANGE INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE RANGE index FOR ()-[n:R]-() on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE RANGE index FOR ()<-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE RANGE INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {}""",
    "create or REPLACE RANGE INDEX FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE RANGE INDEX FOR ()-[n:R]-() ON (n.p)",
    "create or REPLACE RANGE INDEX foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE RANGE INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create RANGE INDEX IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",
    "create RANGE INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE RANGE INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    // lookup

    "CREATE lookup INDEX FOR (n) ON each labels(n)" ->
      "CREATE LOOKUP INDEX FOR (n) ON EACH labels(n)",
    "CREATE lookup INDEX foo FOR (n) ON each labels(n)" ->
      "CREATE LOOKUP INDEX foo FOR (n) ON EACH labels(n)",
    "CREATE lookup INDEX IF NOT EXISTS FOR (n) ON each labels(n)" ->
      "CREATE LOOKUP INDEX IF NOT EXISTS FOR (n) ON EACH labels(n)",
    "CREATE OR REPLACE lookup INDEX foo FOR (n) ON each labels(n) OPTIONS {}" ->
      "CREATE OR REPLACE LOOKUP INDEX foo FOR (n) ON EACH labels(n) OPTIONS {}",
    "CREATE LOOKUP index FOR (n) ON each labels(n) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE LOOKUP INDEX FOR (n) ON EACH labels(n) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE lookup INDEX FOR ()-[r]-() ON each type(r)" ->
      "CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(r)",
    "CREATE lookup INDEX foo FOR ()-[r]-() ON type(r)" ->
      "CREATE LOOKUP INDEX foo FOR ()-[r]-() ON EACH type(r)",
    "CREATE lookup INDEX $foo FOR ()-[r]-() ON type(r)" ->
      "CREATE LOOKUP INDEX $foo FOR ()-[r]-() ON EACH type(r)",
    "CREATE lookup INDEX IF NOT EXISTS FOR ()-[r]-() ON each type(r)" ->
      "CREATE LOOKUP INDEX IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)",
    "CREATE OR REPLACE lookup INDEX foo FOR ()-[r]-() ON type(r) OPTIONS {}" ->
      "CREATE OR REPLACE LOOKUP INDEX foo FOR ()-[r]-() ON EACH type(r) OPTIONS {}",
    "CREATE LOOKUP index FOR ()-[r]-() ON each type(r) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE LOOKUP INDEX FOR ()-[r]-() ON EACH type(r) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",

    // fulltext

    "create FULLTEXT INDEX FOR (n:A) ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX FOR (n:A) ON EACH [n.p]",
    "create FULLTEXT INDEX FOR (n:A) ON EACH [n.p1, n.p2,n.p3]" ->
      "CREATE FULLTEXT INDEX FOR (n:A) ON EACH [n.p1, n.p2, n.p3]",
    "create FULLTEXT INDEX foo FOR (n:A | B) ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX foo FOR (n:A|B) ON EACH [n.p]",
    "create FULLTEXT INDEX `$foo` FOR (n:A|B|C|D) ON EACH [n.p1,n.p2, n.p3]" ->
      "CREATE FULLTEXT INDEX `$foo` FOR (n:A|B|C|D) ON EACH [n.p1, n.p2, n.p3]",
    "CREATE fulltext index FOR (n:Person) on EACH [n.name] OPtiONS {indexProvider: 'fulltext-1.0'}" ->
      """CREATE FULLTEXT INDEX FOR (n:Person) ON EACH [n.name] OPTIONS {indexProvider: "fulltext-1.0"}""",
    "create FULLTEXT INDEX for (n:Person) ON EACH [n.name] OPTIONS {`indexProvider`: 'fulltext-1.0', indexConfig: {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true}}" ->
      """CREATE FULLTEXT INDEX FOR (n:Person) ON EACH [n.name] OPTIONS {indexProvider: "fulltext-1.0", indexConfig: {`fulltext.analyzer`: "some_analyzer", `fulltext.eventually_consistent`: true}}""",
    "create FULLTEXT INDEX myIndex for (n:Person) ON EACH [n.name] OPTIONS {indexConfig: {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true }}" ->
      """CREATE FULLTEXT INDEX myIndex FOR (n:Person) ON EACH [n.name] OPTIONS {indexConfig: {`fulltext.analyzer`: "some_analyzer", `fulltext.eventually_consistent`: true}}""",
    "CREATE FULLTEXT index FOR (n:Person) on EACH [n.name] OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE FULLTEXT INDEX FOR (n:Person) ON EACH [n.name] OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE fulltext index FOR (n:Person) on EACH [n.name] OPtiONS {}" ->
      """CREATE FULLTEXT INDEX FOR (n:Person) ON EACH [n.name] OPTIONS {}""",
    "create or REPLACE FULLTEXT INDEX FOR (n:A) ON EACH [n.p]" ->
      "CREATE OR REPLACE FULLTEXT INDEX FOR (n:A) ON EACH [n.p]",
    "create or REPLACE FULLTEXT INDEX foo FOR (n:A) ON EACH [n.p]" ->
      "CREATE OR REPLACE FULLTEXT INDEX foo FOR (n:A) ON EACH [n.p]",
    "create FULLTEXT INDEX IF not EXISTS FOR (n:A) ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:A) ON EACH [n.p]",
    "create FULLTEXT INDEX foo IF not EXISTS FOR (n:A) ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX foo IF NOT EXISTS FOR (n:A) ON EACH [n.p]",
    "create FULLTEXT INDEX FOR ()-[n:R]->() ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.p]",
    "create FULLTEXT INDEX FOR ()-[n:R|S]-() ON EACH [n.p1,n.p2, n.p3]" ->
      "CREATE FULLTEXT INDEX FOR ()-[n:R|S]-() ON EACH [n.p1, n.p2, n.p3]",
    "create FULLTEXT INDEX `foo` FOR ()-[n:R|S|T|U]-() ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX foo FOR ()-[n:R|S|T|U]-() ON EACH [n.p]",
    "create FULLTEXT INDEX `$foo` FOR ()-[n:R]-() ON EACH [n.p1, n.p2,n.p3]" ->
      "CREATE FULLTEXT INDEX `$foo` FOR ()-[n:R]-() ON EACH [n.p1, n.p2, n.p3]",
    "create FULLTEXT INDEX $foo FOR ()-[n:R]-() ON EACH [n.p1, n.p2,n.p3]" ->
      "CREATE FULLTEXT INDEX $foo FOR ()-[n:R]-() ON EACH [n.p1, n.p2, n.p3]",
    "CREATE FULLtext index FOR ()-[n:R]->() on EACH [n.name] OPtiONS {indexProvider: 'fulltext-1.0'}" ->
      """CREATE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.name] OPTIONS {indexProvider: "fulltext-1.0"}""",
    "create FULLTEXT INDEX for ()-[n:R]-() ON EACH [n.name] OPTIONS {`indexProvider`: 'fulltext-1.0', indexConfig: {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true }}" ->
      """CREATE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.name] OPTIONS {indexProvider: "fulltext-1.0", indexConfig: {`fulltext.analyzer`: "some_analyzer", `fulltext.eventually_consistent`: true}}""",
    "create FULLTEXT INDEX myIndex for ()-[n:R]-() ON EACH [n.name] OPTIONS {indexConfig: {`fulltext.analyzer`: 'some_analyzer', `fulltext.eventually_consistent`: true }}" ->
      """CREATE FULLTEXT INDEX myIndex FOR ()-[n:R]-() ON EACH [n.name] OPTIONS {indexConfig: {`fulltext.analyzer`: "some_analyzer", `fulltext.eventually_consistent`: true}}""",
    "CREATE fullTEXT index FOR ()-[n:R]-() on EACH [n.name] OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.name] OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE fulltext index FOR ()<-[n:R]-() on EACH [n.name] OPtiONS {}" ->
      """CREATE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.name] OPTIONS {}""",
    "create or REPLACE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.p]" ->
      "CREATE OR REPLACE FULLTEXT INDEX FOR ()-[n:R]-() ON EACH [n.p]",
    "create or REPLACE FULLTEXT INDEX foo FOR ()-[n:R]-() ON EACH [n.p]" ->
      "CREATE OR REPLACE FULLTEXT INDEX foo FOR ()-[n:R]-() ON EACH [n.p]",
    "create FULLTEXT INDEX IF not EXISTS FOR ()-[n:R]-() ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX IF NOT EXISTS FOR ()-[n:R]-() ON EACH [n.p]",
    "create FULLTEXT INDEX foo IF not EXISTS FOR ()-[n:R]-() ON EACH [n.p]" ->
      "CREATE FULLTEXT INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON EACH [n.p]",

    // text

    "create TEXT INDEX FOR (n:A) ON (n.p)" ->
      "CREATE TEXT INDEX FOR (n:A) ON (n.p)",
    "create TEXT INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE TEXT INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create TEXT INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE TEXT INDEX foo FOR (n:A) ON (n.p)",
    "create TEXT INDEX `foo` FOR (n:A) ON (n.p)" ->
      "CREATE TEXT INDEX foo FOR (n:A) ON (n.p)",
    "create TEXT INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE TEXT INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create TEXT INDEX $foo FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE TEXT INDEX $foo FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "CREATE TEXT index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'text-1.0'}" ->
      """CREATE TEXT INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "text-1.0"}""",
    "create text INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'text-1.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE TEXT INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "text-1.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",
    "create TEXT INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE TEXT INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",
    "CREATE TEXT index FOR (n:Person) on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE TEXT INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE TEXT index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE TEXT INDEX FOR (n:Person) ON (n.name) OPTIONS {}""",
    "create or REPLACE TEXT INDEX FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE TEXT INDEX FOR (n:A) ON (n.p)",
    "create or REPLACE TEXT INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE TEXT INDEX foo FOR (n:A) ON (n.p)",
    "create TEXT INDEX IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE TEXT INDEX IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create TEXT INDEX foo IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE TEXT INDEX foo IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create TEXT INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.p)",
    "create TEXT INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create TEXT INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE TEXT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create TEXT INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE TEXT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create TEXT INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE TEXT INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "CREATE TEXT index FOR ()-[n:R]->() on (n.name) OPtiONS {indexProvider: 'text-1.0'}" ->
      """CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "text-1.0"}""",
    "create TEXT INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'text-1.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "text-1.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",
    "create TEXT INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE TEXT INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",
    "CREATE TEXT index FOR ()-[n:R]-() on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE TEXT index FOR ()<-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE TEXT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {}""",
    "create or REPLACE TEXT INDEX FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE TEXT INDEX FOR ()-[n:R]-() ON (n.p)",
    "create or REPLACE TEXT INDEX foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE TEXT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create TEXT INDEX IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE TEXT INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",
    "create TEXT INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE TEXT INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    // point

    "create POINT INDEX FOR (n:A) ON (n.p)" ->
      "CREATE POINT INDEX FOR (n:A) ON (n.p)",
    "create POINT INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE POINT INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create POINT INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE POINT INDEX foo FOR (n:A) ON (n.p)",
    "create POINT INDEX `foo` FOR (n:A) ON (n.p)" ->
      "CREATE POINT INDEX foo FOR (n:A) ON (n.p)",
    "create POINT INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE POINT INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "CREATE POINT index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'point-1.0'}" ->
      """CREATE POINT INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "point-1.0"}""",
    "create point INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'point-1.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE POINT INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "point-1.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",
    "create POINT INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE POINT INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",
    "CREATE POINT index FOR (n:Person) on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE POINT INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE POINT index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE POINT INDEX FOR (n:Person) ON (n.name) OPTIONS {}""",
    "create or REPLACE POINT INDEX FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE POINT INDEX FOR (n:A) ON (n.p)",
    "create or REPLACE POINT INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE POINT INDEX foo FOR (n:A) ON (n.p)",
    "create POINT INDEX IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE POINT INDEX IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create POINT INDEX foo IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE POINT INDEX foo IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create POINT INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE POINT INDEX FOR ()-[n:R]-() ON (n.p)",
    "create POINT INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE POINT INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create POINT INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE POINT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create POINT INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE POINT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create POINT INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE POINT INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create POINT INDEX $foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE POINT INDEX $foo FOR ()-[n:R]-() ON (n.p)",
    "CREATE POINT index FOR ()-[n:R]->() on (n.name) OPtiONS {indexProvider: 'point-1.0'}" ->
      """CREATE POINT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "point-1.0"}""",
    "create POINT INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'point-1.0', indexConfig: {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}" ->
      """CREATE POINT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "point-1.0", indexConfig: {`spatial.cartesian.max`: [100.0, 100.0], `spatial.cartesian.min`: [-100.0, -100.0]}}""",
    "create POINT INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0,40.0], `spatial.wgs-84.min`: [-60.0,-40.0] }}" ->
      """CREATE POINT INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`spatial.wgs-84.max`: [60.0, 40.0], `spatial.wgs-84.min`: [-60.0, -40.0]}}""",
    "CREATE POINT index FOR ()-[n:R]-() on (n.name) OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE POINT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE POINT index FOR ()<-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE POINT INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {}""",
    "create or REPLACE POINT INDEX FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE POINT INDEX FOR ()-[n:R]-() ON (n.p)",
    "create or REPLACE POINT INDEX foo FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE POINT INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create POINT INDEX IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE POINT INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",
    "create POINT INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE POINT INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    // vector

    "create VECTOR INDEX FOR (n:A) ON (n.p)" ->
      "CREATE VECTOR INDEX FOR (n:A) ON (n.p)",
    "create VECTOR INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE VECTOR INDEX FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create VECTOR INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE VECTOR INDEX foo FOR (n:A) ON (n.p)",
    "create VECTOR INDEX `foo` FOR (n:A) ON (n.p)" ->
      "CREATE VECTOR INDEX foo FOR (n:A) ON (n.p)",
    "create VECTOR INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)" ->
      "CREATE VECTOR INDEX `$foo` FOR (n:A) ON (n.p1, n.p2, n.p3)",
    "create VECTOR INDEX $foo FOR (n:A) ON (n.p1, n.p2)" ->
      "CREATE VECTOR INDEX $foo FOR (n:A) ON (n.p1, n.p2)",
    "CREATE VECTOR index FOR (n:Person) on (n.name) OPtiONS {indexProvider: 'vector-1.0'}" ->
      """CREATE VECTOR INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "vector-1.0"}""",
    "create vector INDEX for (n:Person) ON (n.name) OPTIONS {`indexProvider`: 'vector-1.0', indexConfig: {`vector.dimensions`:50, `vector.similarity_function`: 'cosine' }}" ->
      """CREATE VECTOR INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider: "vector-1.0", indexConfig: {`vector.dimensions`: 50, `vector.similarity_function`: "cosine"}}""",
    "create VECTOR INDEX myIndex for (n:Person) ON (n.name) OPTIONS {indexConfig: {`vector.dimensions`:50, `vector.similarity_function`: 'euclidean' }}" ->
      """CREATE VECTOR INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`vector.dimensions`: 50, `vector.similarity_function`: "euclidean"}}""",
    "CREATE VECTOR index FOR (n:Person) on (n.name) OPtiONS {`nonValidOption` : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE VECTOR INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE VECTOR index FOR (n:Person) on (n.name) OPtiONS {}" ->
      """CREATE VECTOR INDEX FOR (n:Person) ON (n.name) OPTIONS {}""",
    "create or REPLACE VECTOR INDEX FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE VECTOR INDEX FOR (n:A) ON (n.p)",
    "create or REPLACE VECTOR INDEX foo FOR (n:A) ON (n.p)" ->
      "CREATE OR REPLACE VECTOR INDEX foo FOR (n:A) ON (n.p)",
    "create VECTOR INDEX IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE VECTOR INDEX IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create VECTOR INDEX foo IF not EXISTS FOR (n:A) ON (n.p)" ->
      "CREATE VECTOR INDEX foo IF NOT EXISTS FOR (n:A) ON (n.p)",
    "create VECTOR INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.p)",
    "create VECTOR INDEX FOR ()<-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create VECTOR INDEX foo FOR ()<-[n:R]->() ON (n.p)" ->
      "CREATE VECTOR INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create VECTOR INDEX `foo` FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE VECTOR INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create VECTOR INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)" ->
      "CREATE VECTOR INDEX `$foo` FOR ()-[n:R]-() ON (n.p1, n.p2, n.p3)",
    "create VECTOR INDEX $foo FOR ()-[n:R]-() ON (n.p1, n.p2)" ->
      "CREATE VECTOR INDEX $foo FOR ()-[n:R]-() ON (n.p1, n.p2)",
    "CREATE VECTOR index FOR ()-[n:R]-() on (n.name) OPtiONS {indexProvider: 'vector-2.0'}" ->
      """CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "vector-2.0"}""",
    "create vector INDEX for ()-[n:R]-() ON (n.name) OPTIONS {`indexProvider`: 'vector-2.0', indexConfig: {`vector.dimensions`:50, `vector.similarity_function`: 'cosine' }}" ->
      """CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {indexProvider: "vector-2.0", indexConfig: {`vector.dimensions`: 50, `vector.similarity_function`: "cosine"}}""",
    "create VECTOR INDEX myIndex for ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`vector.dimensions`:50, `vector.similarity_function`: 'euclidean' }}" ->
      """CREATE VECTOR INDEX myIndex FOR ()-[n:R]-() ON (n.name) OPTIONS {indexConfig: {`vector.dimensions`: 50, `vector.similarity_function`: "euclidean"}}""",
    "CREATE VECTOR index FOR ()-[n:R]-() on (n.name) OPtiONS {`nonValidOption` : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE VECTOR index FOR ()-[n:R]-() on (n.name) OPtiONS {}" ->
      """CREATE VECTOR INDEX FOR ()-[n:R]-() ON (n.name) OPTIONS {}""",
    "create or REPLACE VECTOR INDEX FOR ()-[n:R]->() ON (n.p)" ->
      "CREATE OR REPLACE VECTOR INDEX FOR ()-[n:R]-() ON (n.p)",
    "create or REPLACE VECTOR INDEX foo FOR ()<-[n:R]-() ON (n.p)" ->
      "CREATE OR REPLACE VECTOR INDEX foo FOR ()-[n:R]-() ON (n.p)",
    "create VECTOR INDEX IF not EXISTS FOR ()<-[n:R]->() ON (n.p)" ->
      "CREATE VECTOR INDEX IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",
    "create VECTOR INDEX foo IF not EXISTS FOR ()-[n:R]-() ON (n.p)" ->
      "CREATE VECTOR INDEX foo IF NOT EXISTS FOR ()-[n:R]-() ON (n.p)",

    // drop

    "drop INDEX foo" ->
      "DROP INDEX foo",
    "drop INDEX `foo`" ->
      "DROP INDEX foo",
    "drop INDEX `$foo`" ->
      "DROP INDEX `$foo`",
    "drop INDEX $foo" ->
      "DROP INDEX $foo",
    "drop INDEX foo if EXISTS" ->
      "DROP INDEX foo IF EXISTS"
  )

  def constraintCommandTests(): Seq[(String, String)] = Seq(
    "create CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS KEY" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT `foo` FOR (n:A) REQUIRE n.p IS NODE KEY" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT $foo FOR (n:A) REQUIRE (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT $foo FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create OR replace CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY" ->
      "CREATE OR REPLACE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT foo IF NOT EXISTS FOR (n:A) REQUIRE (n.p) IS NODE KEY" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS FOR (n:A) REQUIRE (n.p) IS NODE KEY",
    "create CONSTRAINT FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY",
    "create CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY",
    "create CONSTRAINT `foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY",
    "create CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY" ->
      "CREATE CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS NODE KEY",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS NODE KEY OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY OPTIONS {indexProvider: "range-1.0"}""",
    "create CONSTRAINT myConstraint FOR (n:A) require (n.p) IS NODE KEY OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE CONSTRAINT myConstraint FOR (n:A) REQUIRE (n.p) IS NODE KEY OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create CONSTRAINT FOR (n:A) require (n.p) IS NODE KEY OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified' }}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS NODE KEY OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS NODE KEY OPtiONS {}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS NODE KEY OPTIONS {}""",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create CONSTRAINT foo FOR ()-[r:R]->() REQUIRE r.p IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create CONSTRAINT `foo` FOR ()<-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create CONSTRAINT `$foo` FOR ()<-[r:R]->() REQUIRE (r.p) IS REL KEY" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create OR replace CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY" ->
      "CREATE OR REPLACE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create CONSTRAINT foo IF NOT EXISTS FOR ()-[r:R]-() REQUIRE (r.p) IS KEY" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY",
    "create CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY",
    "create CONSTRAINT `foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY",
    "create CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP KEY",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPTIONS {indexProvider: "range-1.0"}""",
    "create CONSTRAINT myConstraint FOR ()-[r:R]-() require (r.p) IS RELATIONSHIP KEY OPTIONS {`indexProvider`: 'range-1.0', indexConfig: {}}" ->
      """CREATE CONSTRAINT myConstraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create CONSTRAINT FOR ()-[r:R]-() require (r.p) IS RELATIONSHIP KEY OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified' }}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPtiONS {}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP KEY OPTIONS {}""",
    "create CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "create CONSTRAINT foo FOR (n:A) REQUIRE n.p IS UNIQUE" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "create CONSTRAINT `foo` FOR (n:A) REQUIRE (n.p) IS NODE UNIQUE" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "create CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "create CONSTRAINT IF NoT ExistS FOR (n:A) REQUIRE (n.p) IS UNIQUE" ->
      "CREATE CONSTRAINT IF NOT EXISTS FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "create or REPLACE CONSTRAINT foo FOR (n:A) REQUIRE n.p IS UNIQUE" ->
      "CREATE OR REPLACE CONSTRAINT foo FOR (n:A) REQUIRE (n.p) IS UNIQUE",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS UNIQUE OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}""",
    "create CONSTRAINT myConstraint FOR (n:A) require (n.p) IS UNIQUE OPTIONS {`indexProvider`: 'range-1.0', indexConfig: { }}" ->
      """CREATE CONSTRAINT myConstraint FOR (n:A) REQUIRE (n.p) IS UNIQUE OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create CONSTRAINT FOR (n:A) require (n.p) IS UNIQUE OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS UNIQUE OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE constraint FOR (n:A) REQUIRE (n.p) IS UNIQUE OPtiONS {}" ->
      """CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p) IS UNIQUE OPTIONS {}""",
    "create CONSTRAINT FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE",
    "create CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE",
    "create CONSTRAINT `foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT foo FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE",
    "create CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE" ->
      "CREATE CONSTRAINT `$foo` FOR (n:A) REQUIRE (n.p1, n.p2) IS UNIQUE",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create CONSTRAINT foo FOR ()-[r:R]->() REQUIRE r.p IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create CONSTRAINT `foo` FOR ()<-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create CONSTRAINT `$foo` FOR ()<-[r:R]->() REQUIRE (r.p) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create CONSTRAINT $foo FOR ()<-[r:R]->() REQUIRE (r.p) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT $foo FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create CONSTRAINT IF NoT ExistS FOR ()-[r:R]-() REQUIRE (r.p) IS REL UNIQUE" ->
      "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "create or REPLACE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE r.p IS UNIQUE" ->
      "CREATE OR REPLACE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP UNIQUE OPtiONS {indexProvider: 'range-1.0'}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}""",
    "create CONSTRAINT myConstraint FOR ()-[r:R]-() require (r.p) IS RELATIONSHIP UNIQUE OPTIONS {`indexProvider`: 'range-1.0', indexConfig: { }}" ->
      """CREATE CONSTRAINT myConstraint FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE OPTIONS {indexProvider: "range-1.0", indexConfig: {}}""",
    "create CONSTRAINT FOR ()-[r:R]-() require (r.p) IS RELATIONSHIP UNIQUE OPTIONS {indexConfig: {someConfig: 'toShowItCanBePrettified'}}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE OPTIONS {indexConfig: {someConfig: "toShowItCanBePrettified"}}""",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP UNIQUE OPtiONS {nonValidOption : 42, `backticks.stays.when.needed`: 'theAnswer'}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE OPTIONS {nonValidOption: 42, `backticks.stays.when.needed`: "theAnswer"}""",
    "CREATE constraint FOR ()-[r:R]-() REQUIRE (r.p) IS RELATIONSHIP UNIQUE OPtiONS {}" ->
      """CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS UNIQUE OPTIONS {}""",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS UNIQUE",
    "create CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS UNIQUE",
    "create CONSTRAINT `foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS UNIQUE",
    "create CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS RELATIONSHIP UNIQUE" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p1, r.p2) IS UNIQUE",
    "create CONSTRAINT FOR (a:A) REQUIRE (a.p) is not null" ->
      "CREATE CONSTRAINT FOR (a:A) REQUIRE (a.p) IS NOT NULL",
    "create CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS NoT NulL" ->
      "CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS NOT NULL",
    "create CONSTRAINT `foo` FOR (a:A) REQUIRE (a.p) IS NOT NULL OPTIONS {}" ->
      "CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS NOT NULL OPTIONS {}",
    "create CONSTRAINT `foo` FOR (a:A) REQUIRE a.p IS NOT NULL OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS NOT NULL OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",
    "create CONSTRAINT `$foo` FOR (a:A) REQUIRE a.p IS NOT NULL" ->
      "CREATE CONSTRAINT `$foo` FOR (a:A) REQUIRE (a.p) IS NOT NULL",
    "create OR replace CONSTRAINT FOR (a:A) REQUIRE a.p IS NOT NULL" ->
      "CREATE OR REPLACE CONSTRAINT FOR (a:A) REQUIRE (a.p) IS NOT NULL",
    "create CONSTRAINT foo if not EXISTS FOR (a:A) REQUIRE a.p IS NOT NULL" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS FOR (a:A) REQUIRE (a.p) IS NOT NULL",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE r.p is not nULl" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL",
    "create CONSTRAINT foo FOR ()-[r:R]->() REQUIRE (r.p) IS NOT NULL" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL",
    "create CONSTRAINT `foo` FOR ()<-[r:R]-() REQUIRE r.p is NOT null" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL",
    "create CONSTRAINT `$foo` FOR ()<-[r:R]->() REQUIRE r.p IS NOT NULL OPTIONS {}" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL OPTIONS {}",
    "create CONSTRAINT $foo FOR ()<-[r:R]->() REQUIRE r.p IS NOT NULL OPTIONS {}" ->
      "CREATE CONSTRAINT $foo FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL OPTIONS {}",
    "create CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE r.p IS NOT NULL OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",
    "create CONSTRAINT IF not exists FOR ()-[r:R]-() REQUIRE r.p IS NOT NULL" ->
      "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL",
    "create or Replace CONSTRAINT foo FOR ()-[r:R]-() REQUIRE r.p IS NOT NULL" ->
      "CREATE OR REPLACE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS NOT NULL",
    "create CONSTRAINT FOR (a:A) REQUIRE (a.p) is typed boolean" ->
      "CREATE CONSTRAINT FOR (a:A) REQUIRE (a.p) IS :: BOOLEAN",
    "create CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS :: int" ->
      "CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS :: INTEGER",
    "create CONSTRAINT `foo` FOR (a:A) REQUIRE (a.p) IS TYPED STriNG OPTIONS {}" ->
      "CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS :: STRING OPTIONS {}",
    "create CONSTRAINT `foo` FOR (a:A) REQUIRE a.p :: FLOAT OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS :: FLOAT OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",
    "create CONSTRAINT `$foo` FOR (a:A) REQUIRE a.p IS :: varChar" ->
      "CREATE CONSTRAINT `$foo` FOR (a:A) REQUIRE (a.p) IS :: STRING",
    "create OR replace CONSTRAINT FOR (a:A) REQUIRE a.p IS TYPED time with timezone" ->
      "CREATE OR REPLACE CONSTRAINT FOR (a:A) REQUIRE (a.p) IS :: ZONED TIME",
    "create CONSTRAINT foo if not EXISTS FOR (a:A) REQUIRE a.p IS :: LIST<date not null>" ->
      "CREATE CONSTRAINT foo IF NOT EXISTS FOR (a:A) REQUIRE (a.p) IS :: LIST<DATE NOT NULL>",
    "create CONSTRAINT FOR (a:A) REQUIRE (a.p) is typed boolean | list<float not null> | int | list<point not null> | string" ->
      "CREATE CONSTRAINT FOR (a:A) REQUIRE (a.p) IS :: BOOLEAN | STRING | INTEGER | LIST<FLOAT NOT NULL> | LIST<POINT NOT NULL>",
    "create CONSTRAINT foo FOR (a:A) REQUIRE (a.p) is typed any<boolean | string | point>" ->
      "CREATE CONSTRAINT foo FOR (a:A) REQUIRE (a.p) IS :: BOOLEAN | STRING | POINT",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE r.p is tyPED bool" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS :: BOOLEAN",
    "create CONSTRAINT foo FOR ()-[r:R]->() REQUIRE (r.p) IS :: int" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS :: INTEGER",
    "create CONSTRAINT `foo` FOR ()<-[r:R]-() REQUIRE r.p is TYPED STRING" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS :: STRING",
    "create CONSTRAINT `$foo` FOR ()<-[r:R]->() REQUIRE r.p :: signed INTEGER OPTIONS {}" ->
      "CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS :: INTEGER OPTIONS {}",
    "create CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE r.p IS :: point OPtiONS {notAllowedOptions: 'butParseThem', `backticks.stays.when.needed`: 'toThrowNiceError'}" ->
      """CREATE CONSTRAINT `$foo` FOR ()-[r:R]-() REQUIRE (r.p) IS :: POINT OPTIONS {notAllowedOptions: "butParseThem", `backticks.stays.when.needed`: "toThrowNiceError"}""",
    "create CONSTRAINT IF not exists FOR ()-[r:R]-() REQUIRE r.p IS TYPED timestamp without timezone" ->
      "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]-() REQUIRE (r.p) IS :: LOCAL DATETIME",
    "create or Replace CONSTRAINT foo FOR ()-[r:R]-() REQUIRE r.p IS :: list<duration NOT NULL>" ->
      "CREATE OR REPLACE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS :: LIST<DURATION NOT NULL>",
    "create CONSTRAINT FOR ()-[r:R]-() REQUIRE r.p :: point   |  LIST  < timestamp with timezone   not    null>  |   bool" ->
      "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.p) IS :: BOOLEAN | POINT | LIST<ZONED DATETIME NOT NULL>",
    "create CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) is typed any<boolean | string | point>" ->
      "CREATE CONSTRAINT foo FOR ()-[r:R]-() REQUIRE (r.p) IS :: BOOLEAN | STRING | POINT",
    "drop CONSTRAINT foo" ->
      "DROP CONSTRAINT foo",
    "drop CONSTRAINT `foo`" ->
      "DROP CONSTRAINT foo",
    "drop CONSTRAINT `$foo`" ->
      "DROP CONSTRAINT `$foo`",
    "drop CONSTRAINT $foo" ->
      "DROP CONSTRAINT $foo",
    "drop CONSTRAINT foo IF exists" ->
      "DROP CONSTRAINT foo IF EXISTS"
  )

  def showCommandTests(): Seq[(String, String)] = Seq[(String, String)](
    // show indexes

    "show index" ->
      "SHOW ALL INDEXES",
    "show all inDEXES" ->
      "SHOW ALL INDEXES",
    "show RAnGE index" ->
      "SHOW RANGE INDEXES",
    "show FULltEXT index" ->
      "SHOW FULLTEXT INDEXES",
    "show tEXT index" ->
      "SHOW TEXT INDEXES",
    "show pOInt index" ->
      "SHOW POINT INDEXES",
    "show vecTOR index" ->
      "SHOW VECTOR INDEXES",
    "show loOKup index" ->
      "SHOW LOOKUP INDEXES",
    "show \nindex\n" ->
      "SHOW ALL INDEXES",
    "show index WHERE uniqueness = 'UNIQUE'" ->
      """SHOW ALL INDEXES
        |  WHERE uniqueness = "UNIQUE"""".stripMargin,
    "show lookup index  YIELD *" ->
      """SHOW LOOKUP INDEXES
        |YIELD *""".stripMargin,
    "show index  YIELD * Return DISTINCT type" ->
      """SHOW ALL INDEXES
        |YIELD *
        |RETURN DISTINCT type""".stripMargin,
    "show poInt index  YIELD * Return DISTINCT type" ->
      """SHOW POINT INDEXES
        |YIELD *
        |RETURN DISTINCT type""".stripMargin,
    "show fulltext index YIELD * where name = 'neo4j' Return *" ->
      """SHOW FULLTEXT INDEXES
        |YIELD *
        |  WHERE name = "neo4j"
        |RETURN *""".stripMargin,
    "show index yield name order by name skip 1 limit 1" ->
      """SHOW ALL INDEXES
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,
    "show text index yield name return name" ->
      """SHOW TEXT INDEXES
        |YIELD name
        |RETURN name""".stripMargin,

    // show constraints

    "show constraints" ->
      "SHOW ALL CONSTRAINTS",
    "show exist constraint" ->
      "SHOW PROPERTY EXISTENCE CONSTRAINTS",
    "show property existence constraint" ->
      "SHOW PROPERTY EXISTENCE CONSTRAINTS",
    "SHOW NODE EXIST constraint" ->
      "SHOW NODE PROPERTY EXISTENCE CONSTRAINTS",
    "SHOW NODE property EXIST constraint" ->
      "SHOW NODE PROPERTY EXISTENCE CONSTRAINTS",
    "show relationship EXIST cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",
    "show relationship EXISTENCE cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",
    "show rel EXIST cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",
    "show rel property EXISTence cOnStRaInTs" ->
      "SHOW RELATIONSHIP PROPERTY EXISTENCE CONSTRAINTS",
    "show unique constraint" ->
      "SHOW PROPERTY UNIQUENESS CONSTRAINTS",
    "show node unique constraint" ->
      "SHOW NODE PROPERTY UNIQUENESS CONSTRAINTS",
    "show REL unique constraint" ->
      "SHOW RELATIONSHIP PROPERTY UNIQUENESS CONSTRAINTS",
    "show Relationship unique constraint" ->
      "SHOW RELATIONSHIP PROPERTY UNIQUENESS CONSTRAINTS",
    "show uniqueness constraint" ->
      "SHOW PROPERTY UNIQUENESS CONSTRAINTS",
    "show node uniqueness constraint" ->
      "SHOW NODE PROPERTY UNIQUENESS CONSTRAINTS",
    "show REL uniqueness constraint" ->
      "SHOW RELATIONSHIP PROPERTY UNIQUENESS CONSTRAINTS",
    "show Relationship uniqueness constraint" ->
      "SHOW RELATIONSHIP PROPERTY UNIQUENESS CONSTRAINTS",
    "show key CONSTRAINTS" ->
      "SHOW KEY CONSTRAINTS",
    "show node key CONSTRAINTS" ->
      "SHOW NODE KEY CONSTRAINTS",
    "show rel key CONSTRAINTS" ->
      "SHOW RELATIONSHIP KEY CONSTRAINTS",
    "show relationship key CONSTRAINTS" ->
      "SHOW RELATIONSHIP KEY CONSTRAINTS",
    "show property type CONSTRAINTS" ->
      "SHOW PROPERTY TYPE CONSTRAINTS",
    "show node property type CONSTRAINTS" ->
      "SHOW NODE PROPERTY TYPE CONSTRAINTS",
    "show rel property type CONSTRAINTS" ->
      "SHOW RELATIONSHIP PROPERTY TYPE CONSTRAINTS",
    "show relationship property type CONSTRAINTS" ->
      "SHOW RELATIONSHIP PROPERTY TYPE CONSTRAINTS",
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
      """SHOW PROPERTY UNIQUENESS CONSTRAINTS
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

    // show procedures

    "show procedure" ->
      "SHOW PROCEDURES",
    "show procEDUREs" ->
      "SHOW PROCEDURES",
    "show procedures executable" ->
      "SHOW PROCEDURES EXECUTABLE BY CURRENT USER",
    "show procedures executable BY cuRRent USer" ->
      "SHOW PROCEDURES EXECUTABLE BY CURRENT USER",
    "show procedures executable BY USer" ->
      "SHOW PROCEDURES EXECUTABLE BY USer",
    "show \nprocedure\n executable" ->
      "SHOW PROCEDURES EXECUTABLE BY CURRENT USER",
    "show procEDUREs WHERE name = 'my.proc'" ->
      """SHOW PROCEDURES
        |  WHERE name = "my.proc"""".stripMargin,
    "show procedure  YIELD *" ->
      """SHOW PROCEDURES
        |YIELD *""".stripMargin,
    "show procedure  YIELD * Return DISTINCT mode" ->
      """SHOW PROCEDURES
        |YIELD *
        |RETURN DISTINCT mode""".stripMargin,
    "show procedure YIELD * where name = 'neo4j' Return *" ->
      """SHOW PROCEDURES
        |YIELD *
        |  WHERE name = "neo4j"
        |RETURN *""".stripMargin,
    "show procedure yield name order by name skip 1 limit 1" ->
      """SHOW PROCEDURES
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,

    // show functions

    "show function" ->
      "SHOW ALL FUNCTIONS",
    "show all function" ->
      "SHOW ALL FUNCTIONS",
    "show built in function" ->
      "SHOW BUILT IN FUNCTIONS",
    "show user defined function" ->
      "SHOW USER DEFINED FUNCTIONS",
    "show funcTIONs" ->
      "SHOW ALL FUNCTIONS",
    "show functions executable" ->
      "SHOW ALL FUNCTIONS EXECUTABLE BY CURRENT USER",
    "show all functions executable BY cuRRent USer" ->
      "SHOW ALL FUNCTIONS EXECUTABLE BY CURRENT USER",
    "show functions executable BY USer" ->
      "SHOW ALL FUNCTIONS EXECUTABLE BY USer",
    "show built in \nfunction\n executable" ->
      "SHOW BUILT IN FUNCTIONS EXECUTABLE BY CURRENT USER",
    "show funcTIONs WHERE name = 'my.proc'" ->
      """SHOW ALL FUNCTIONS
        |  WHERE name = "my.proc"""".stripMargin,
    "show function  YIELD *" ->
      """SHOW ALL FUNCTIONS
        |YIELD *""".stripMargin,
    "show user defined function  YIELD * Return DISTINCT mode" ->
      """SHOW USER DEFINED FUNCTIONS
        |YIELD *
        |RETURN DISTINCT mode""".stripMargin,
    "show function YIELD * where name = 'neo4j' Return *" ->
      """SHOW ALL FUNCTIONS
        |YIELD *
        |  WHERE name = "neo4j"
        |RETURN *""".stripMargin,
    "show function yield name order by name skip 1 limit 1" ->
      """SHOW ALL FUNCTIONS
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,

    // show transactions

    "show transaction" ->
      "SHOW TRANSACTIONS",
    "show transACTIONs" ->
      "SHOW TRANSACTIONS",
    "show transactions 'db1-transaction-123'" ->
      """SHOW TRANSACTIONS "db1-transaction-123"""",
    "show transactions $param" ->
      "SHOW TRANSACTIONS $param",
    "show transactions ['db1-transaction-123', 'db1-transaction-123']" ->
      """SHOW TRANSACTIONS ["db1-transaction-123", "db1-transaction-123"]""",
    "show transactions db1-transaction-123" ->
      """SHOW TRANSACTIONS (db1 - transaction) - 123""",
    "show transactions 'db1-transaction-123', 'db2-transaction-456'" ->
      """SHOW TRANSACTIONS "db1-transaction-123", "db2-transaction-456"""",
    "show \ntransaction\n 'db1-transaction-123'" ->
      """SHOW TRANSACTIONS "db1-transaction-123"""",
    "show transACTIONs WHERE transactionId = 'db1-transaction-123'" ->
      """SHOW TRANSACTIONS
        |  WHERE transactionId = "db1-transaction-123"""".stripMargin,
    "show transaction  YIELD *" ->
      """SHOW TRANSACTIONS
        |YIELD *""".stripMargin,
    "show transaction  YIELD * Return DISTINCT database" ->
      """SHOW TRANSACTIONS
        |YIELD *
        |RETURN DISTINCT database""".stripMargin,
    "show transaction YIELD * where database = 'neo4j' Return *" ->
      """SHOW TRANSACTIONS
        |YIELD *
        |  WHERE database = "neo4j"
        |RETURN *""".stripMargin,
    "show transaction yield currentQueryId order by currentQueryId skip 1 limit 1" ->
      """SHOW TRANSACTIONS
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,

    // terminate transactions

    "terminate transaction" ->
      "TERMINATE TRANSACTIONS",
    "terminate transactions 'db1-transaction-123'" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"""",
    "terminate transactions $param" ->
      "TERMINATE TRANSACTIONS $param",
    "terminate transactions ['db1-transaction-123']" ->
      """TERMINATE TRANSACTIONS ["db1-transaction-123"]""",
    "terminate transactions x+2" ->
      """TERMINATE TRANSACTIONS x + 2""",
    "terminate transactions 'db1-transaction-123', 'db2-transaction-456'" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123", "db2-transaction-456"""",
    "terminate \ntransaction\n 'db1-transaction-123'" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"""",
    "terminate transaction 'db1-transaction-123'  YIELD *" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD *""".stripMargin,
    "terminate transaction 'db1-transaction-123'  YIELD * Return DISTINCT database" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD *
        |RETURN DISTINCT database""".stripMargin,
    "terminate transaction 'db1-transaction-123' YIELD * where database = 'neo4j' Return *" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD *
        |  WHERE database = "neo4j"
        |RETURN *""".stripMargin,
    "terminate transaction 'db1-transaction-123' yield currentQueryId order by currentQueryId skip 1 limit 1" ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,

    // combine show and terminate transactions

    "show transaction terminate transaction" ->
      """SHOW TRANSACTIONS
        |TERMINATE TRANSACTIONS""".stripMargin,
    """terminate transaction $x+'123' yield transactionId AS txIdT
      |show transaction txIdT yield transactionId AS txIdS""".stripMargin ->
      """TERMINATE TRANSACTIONS $x + "123"
        |YIELD transactionId AS txIdT
        |SHOW TRANSACTIONS txIdT
        |YIELD transactionId AS txIdS""".stripMargin,
    """terminate transaction 'db1-transaction-123' yield currentQueryId order by currentQueryId skip 1 limit 1
      |show transaction 'db1-transaction-123' yield currentQueryId order by currentQueryId skip 1 limit 1""".stripMargin ->
      """TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1
        |SHOW TRANSACTIONS "db1-transaction-123"
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,
    """show transaction 'db1-transaction-123' where database = 'neo4j'
      |terminate transaction 'db1-transaction-123', 'db1-transaction-123', 'db1-transaction-123' yield currentQueryId order by currentQueryId skip 1 limit 1
      |terminate transaction 'db1-transaction-123' YIELD * where database = 'neo4j'
      |show transaction 'db1-transaction-123', 'db1-transaction-123' yield currentQueryId order by currentQueryId skip 1 limit 1
      |Return *""".stripMargin ->
      """SHOW TRANSACTIONS "db1-transaction-123"
        |  WHERE database = "neo4j"
        |TERMINATE TRANSACTIONS "db1-transaction-123", "db1-transaction-123", "db1-transaction-123"
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1
        |TERMINATE TRANSACTIONS "db1-transaction-123"
        |YIELD *
        |  WHERE database = "neo4j"
        |SHOW TRANSACTIONS "db1-transaction-123", "db1-transaction-123"
        |YIELD currentQueryId
        |  ORDER BY currentQueryId ASCENDING
        |  SKIP 1
        |  LIMIT 1
        |RETURN *""".stripMargin,

    // show settings

    "show setting" ->
      "SHOW SETTINGS",
    "show seTTIng" ->
      "SHOW SETTINGS",
    "show settings 'foo'" ->
      """SHOW SETTINGS "foo"""",
    "show settings $param" ->
      "SHOW SETTINGS $param",
    "show settings $`some escaped param`" ->
      "SHOW SETTINGS $`some escaped param`",
    "show settings 'foo', 'bar'" ->
      """SHOW SETTINGS "foo", "bar"""",
    "show settings $param+'.list'" ->
      """SHOW SETTINGS $param + ".list"""",
    "show settings [ 'foo', 'bar' ]" ->
      """SHOW SETTINGS ["foo", "bar"]""",
    "show \nsetting\n 'foo'" ->
      """SHOW SETTINGS "foo"""",
    "show setTing WHERE name = 'foo'" ->
      """SHOW SETTINGS
        |  WHERE name = "foo"""".stripMargin,
    "show setting 'foo' WHERE name = 'foo'" ->
      """SHOW SETTINGS "foo"
        |  WHERE name = "foo"""".stripMargin,
    "show settings $list WHERE name = 'foo'" ->
      """SHOW SETTINGS $list
        |  WHERE name = "foo"""".stripMargin,
    "show settings $`escaped list` WHERE name = 'foo'" ->
      """SHOW SETTINGS $`escaped list`
        |  WHERE name = "foo"""".stripMargin,
    "show setting  YIELD *" ->
      """SHOW SETTINGS
        |YIELD *""".stripMargin,
    "show setting  YIELD * Return DISTINCT name" ->
      """SHOW SETTINGS
        |YIELD *
        |RETURN DISTINCT name""".stripMargin,
    "show setting YIELD * where name STARTS WITH 'dbms' Return *" ->
      """SHOW SETTINGS
        |YIELD *
        |  WHERE name STARTS WITH "dbms"
        |RETURN *""".stripMargin,
    "show setting yield name order by name skip 1 limit 1" ->
      """SHOW SETTINGS
        |YIELD name
        |  ORDER BY name ASCENDING
        |  SKIP 1
        |  LIMIT 1""".stripMargin,
    "SHOW setting 'foo', 'bar' yield name, description, isDynamic WHERE isDynamic RETURN *" ->
      """SHOW SETTINGS "foo", "bar"
        |YIELD name, description, isDynamic
        |  WHERE isDynamic
        |RETURN *""".stripMargin,
    "SHOW setting $list yield name, description, isExplicitlySet WHERE isExplicitlySet" ->
      """SHOW SETTINGS $list
        |YIELD name, description, isExplicitlySet
        |  WHERE isExplicitlySet""".stripMargin
  )

  def administrationTests(): Seq[(String, String)] = Seq[(String, String)](
    // user commands
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
    "Show Users wiTH AUth" ->
      "SHOW USERS WITH AUTH",
    "Show Users wiTH AUth where user = 'neo4j'" ->
      """SHOW USERS WITH AUTH
        |  WHERE user = "neo4j"""".stripMargin,
    "Show Users wiTH AUth YIELD * where user = 'neo4j' Return *" ->
      """SHOW USERS WITH AUTH
        |  YIELD *
        |    WHERE user = "neo4j"
        |  RETURN *""".stripMargin,
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
    "create user abc set password $password set home database neo4j" ->
      "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED SET HOME DATABASE neo4j",
    "create user abc set password $password set home database a.b" ->
      "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED SET HOME DATABASE a.b",
    "create user abc set password $password set home database `a.b`" ->
      "CREATE USER abc SET PASSWORD $password CHANGE REQUIRED SET HOME DATABASE `a.b`",
    "create user abc set auth 'native' { set password $password }" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD $password
        |    SET PASSWORD CHANGE REQUIRED
        |  }""".stripMargin,
    "create user abc set auth 'native' { set password $password } set home database foo" ->
      """CREATE USER abc SET HOME DATABASE foo
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD $password
        |    SET PASSWORD CHANGE REQUIRED
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id 'cba' }" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "cba"
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id $id }" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "foo" {
        |    SET ID $id
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id 'cba' } set auth 'bar' { set id 'cba' }" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "bar" {
        |    SET ID "cba"
        |  }
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "cba"
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id 'cba' } set password $password" ->
      """CREATE USER abc SET PASSWORD $password CHANGE REQUIRED
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "cba"
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id 'cba' } set auth 'native' {set password 'bar'}" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD '******'
        |    SET PASSWORD CHANGE REQUIRED
        |  }
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "cba"
        |  }""".stripMargin,
    "create user abc set home database foo  set auth 'foo' { set id $id } set auth 'native' { set password change not required set password $password }" ->
      """CREATE USER abc SET HOME DATABASE foo
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD $password
        |    SET PASSWORD CHANGE NOT REQUIRED
        |  }
        |  SET AUTH PROVIDER "foo" {
        |    SET ID $id
        |  }""".stripMargin,
    "create user abc set auth 'foo' { set id 'cba' } set home database neo4j" ->
      """CREATE USER abc SET HOME DATABASE neo4j
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "cba"
        |  }""".stripMargin,
    """create user abc set auth "prov'id'er" { set id "i'd" }""" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "prov'id'er" {
        |    SET ID "i'd"
        |  }""".stripMargin,
    """create user abc set auth 'prov"id"er' { set id 'i"d' }""" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER 'prov"id"er' {
        |    SET ID 'i"d'
        |  }""".stripMargin,
    """create user abc set auth "prov\"id\"er" { set id "i\"d" }""" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER 'prov"id"er' {
        |    SET ID 'i"d'
        |  }""".stripMargin,
    """create user abc set auth 'prov\'id\'er' { set id 'i\'d' }""" ->
      """CREATE USER abc
        |  SET AUTH PROVIDER "prov'id'er" {
        |    SET ID "i'd"
        |  }""".stripMargin,
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
    "alter user abc set status active set home database db1.db2 set password change not required" ->
      "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE SET HOME DATABASE db1.db2",
    "alter user abc set status active set home database `db1.db2` set password change not required" ->
      "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE SET HOME DATABASE `db1.db2`",
    "alter user abc set status active set password change not required" ->
      "ALTER USER abc SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE",
    "alter user abc set home database null" ->
      "ALTER USER abc SET HOME DATABASE null", // this is the string "null"
    "alter user abc remove home database" ->
      "ALTER USER abc REMOVE HOME DATABASE",
    "alter user abc if exists remove home database" ->
      "ALTER USER abc IF EXISTS REMOVE HOME DATABASE",
    "alter user abc remove home database set home database foo" ->
      "ALTER USER abc SET HOME DATABASE foo",
    "alter user abc remove home database set status suspended" ->
      "ALTER USER abc REMOVE HOME DATABASE SET STATUS SUSPENDED",
    "alter user abc set auth 'native' {set password 'foo'}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD '******'
        |  }""".stripMargin,
    "alter user abc set auth 'native' {set password $password}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD $password
        |  }""".stripMargin,
    "alter user abc set auth 'native' {set password change not required}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD CHANGE NOT REQUIRED
        |  }""".stripMargin,
    "alter user abc set auth 'native' {set password change not required} set status active" ->
      """ALTER USER abc SET STATUS ACTIVE
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD CHANGE NOT REQUIRED
        |  }""".stripMargin,
    "alter user abc set auth 'native' {set password 'foo' set password change not required}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD '******'
        |    SET PASSWORD CHANGE NOT REQUIRED
        |  }""".stripMargin,
    "alter user abc set auth 'native' {set password change not required set password $password}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD $password
        |    SET PASSWORD CHANGE NOT REQUIRED
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id 'bar'}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "bar"
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id $id}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "foo" {
        |    SET ID $id
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id 'bar'} set password 'foo'" ->
      """ALTER USER abc SET PASSWORD '******'
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "bar"
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id 'bar'} set auth 'native' {set password 'foo'}" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD '******'
        |  }
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "bar"
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id $id} set auth 'native' {set password 'foo'} set home database foo" ->
      """ALTER USER abc SET HOME DATABASE foo
        |  SET AUTH PROVIDER "native" {
        |    SET PASSWORD '******'
        |  }
        |  SET AUTH PROVIDER "foo" {
        |    SET ID $id
        |  }""".stripMargin,
    "alter user abc set auth 'foo' {set id 'bar'} set home database neo4j set status active" ->
      """ALTER USER abc SET STATUS ACTIVE SET HOME DATABASE neo4j
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "bar"
        |  }""".stripMargin,
    """alter user abc set auth "prov'id'er" { set id "i'd" }""" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "prov'id'er" {
        |    SET ID "i'd"
        |  }""".stripMargin,
    """alter user abc set auth 'prov"id"er' { set id 'i"d' }""" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER 'prov"id"er' {
        |    SET ID 'i"d'
        |  }""".stripMargin,
    """alter user abc set auth "prov\"id\"er" { set id "i\"d" }""" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER 'prov"id"er' {
        |    SET ID 'i"d'
        |  }""".stripMargin,
    """alter user abc set auth 'prov\'id\'er' { set id 'i\'d' }""" ->
      """ALTER USER abc
        |  SET AUTH PROVIDER "prov'id'er" {
        |    SET ID "i'd"
        |  }""".stripMargin,
    "alter user abc remove all auth" ->
      "ALTER USER abc REMOVE ALL AUTH PROVIDERS",
    "alter user abc remove auth 'foo'" ->
      """ALTER USER abc REMOVE AUTH PROVIDERS "foo"""",
    "alter user abc remove auth ['foo', 'bar']" ->
      """ALTER USER abc REMOVE AUTH PROVIDERS ["foo", "bar"]""",
    "alter user abc remove all auth set auth 'foo' {set id 'bar'}" ->
      """ALTER USER abc REMOVE ALL AUTH PROVIDERS
        |  SET AUTH PROVIDER "foo" {
        |    SET ID "bar"
        |  }""".stripMargin,
    "alter user abc remove auth $provider remove home database" ->
      """ALTER USER abc REMOVE HOME DATABASE REMOVE AUTH PROVIDERS $provider""",
    "alter user abc remove auth 'foo' remove home database set password $password" ->
      """ALTER USER abc REMOVE HOME DATABASE REMOVE AUTH PROVIDERS "foo" SET PASSWORD $password""",
    "alter user abc remove auth 'native' set password $password" ->
      """ALTER USER abc REMOVE AUTH PROVIDERS "native" SET PASSWORD $password""",
    "alter user abc remove all auth remove auth 'foo'" ->
      """ALTER USER abc REMOVE ALL AUTH PROVIDERS""",
    "alter user abc remove auth $param remove all auth" ->
      """ALTER USER abc REMOVE ALL AUTH PROVIDERS""",
    "alter user abc remove auth 'bar' remove auth 'foo'" ->
      """ALTER USER abc REMOVE AUTH PROVIDERS "bar" REMOVE AUTH PROVIDERS "foo"""",
    "alter user abc remove auth $param remove auth ['foo', 'bar'] remove auth 'foo'" ->
      """ALTER USER abc REMOVE AUTH PROVIDERS $param REMOVE AUTH PROVIDERS ["foo", "bar"] REMOVE AUTH PROVIDERS "foo"""",
    """alter user abc remove auth "prov'id'er" """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS "prov'id'er"""".stripMargin,
    """alter user abc remove auth 'prov"id"er' """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS 'prov"id"er'""".stripMargin,
    """alter user abc remove auth "prov\"id\"er" """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS 'prov"id"er'""".stripMargin,
    """alter user abc remove auth 'prov\'id\'er' """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS "prov'id'er"""".stripMargin,
    """alter user abc remove auth ["prov'id'er"] """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS ["prov'id'er"]""".stripMargin,
    """alter user abc remove auth ['prov"id"er'] """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS ['prov"id"er']""".stripMargin,
    """alter user abc remove auth ["prov\"id\"er"] """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS ['prov"id"er']""".stripMargin,
    """alter user abc remove auth ['prov\'id\'er'] """ ->
      """ALTER USER abc REMOVE AUTH PROVIDERS ["prov'id'er"]""".stripMargin,
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

    // role commands
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

    // show privileges
    "show supported privileges" ->
      "SHOW SUPPORTED PRIVILEGES",
    "show supported privileges where action = 'access'" ->
      """SHOW SUPPORTED PRIVILEGES
        |  WHERE action = "access"""".stripMargin,
    "Show supported privileges YIELD * where action = 'traverse' Return *" ->
      """SHOW SUPPORTED PRIVILEGES
        |  YIELD *
        |    WHERE action = "traverse"
        |  RETURN *""".stripMargin,
    "Show privileges YIELD * Return DISTINCT action, target" ->
      """SHOW ALL PRIVILEGES
        |  YIELD *
        |  RETURN DISTINCT action, target""".stripMargin,
    "show supported privileges yield action, target order by action skip 1 limit 1 where target='database'" ->
      """SHOW SUPPORTED PRIVILEGES
        |  YIELD action, target
        |    ORDER BY action ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE target = "database"""".stripMargin,
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

    // database commands
    "show databases" ->
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
    "show default database" ->
      "SHOW DEFAULT DATABASE",
    "show database foO_Bar_42" ->
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
    "create database foO_Bar_42" ->
      "CREATE DATABASE foO_Bar_42",
    "create database $foo" ->
      "CREATE DATABASE $foo",
    "create database `foO_Bar_42`" ->
      "CREATE DATABASE foO_Bar_42",
    "create database `foO_Bar_42` if not exists" ->
      "CREATE DATABASE foO_Bar_42 IF NOT EXISTS",
    "create or replace database `foO_Bar_42`" ->
      "CREATE OR REPLACE DATABASE foO_Bar_42",
    "CREATE DATABASE foo topology 1 primary" ->
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY",
    "CREATE DATABASE foo topology 0 primary" ->
      "CREATE DATABASE foo TOPOLOGY 0 PRIMARIES",
    "CREATE DATABASE foo topology 1 primaries" ->
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY",
    "CREATE DATABASE foo topology 2 PRIMARY" ->
      "CREATE DATABASE foo TOPOLOGY 2 PRIMARIES",
    "CREATE DATABASE foo topology 1 primary 2 secondary" ->
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES",
    "CREATE DATABASE foo topology 1 primaries 2 secondaries" ->
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES",
    "CREATE DATABASE foo topology 0 secondary" ->
      "CREATE DATABASE foo TOPOLOGY 0 SECONDARIES",
    "CREATE DATABASE foo if not exists topology 1 primaries 2 secondaries  options {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'} wait" ->
      "CREATE DATABASE foo IF NOT EXISTS TOPOLOGY 1 PRIMARY 2 SECONDARIES OPTIONS {existingData: \"use\", existingDataSeedInstance: \"84c3ee6f-260e-47db-a4b6-589c807f2c2e\"} WAIT",
    "CREATE DATABASE foo TOPOLOGY 1 SECONDARY 2 PRIMARY" ->
      "CREATE DATABASE foo TOPOLOGY 2 PRIMARIES 1 SECONDARY",
    "create database `graph.db`" ->
      "CREATE DATABASE `graph.db`",
    "create database graph.db" ->
      "CREATE DATABASE `graph.db`",
    "create database graph.db wait" ->
      "CREATE DATABASE `graph.db` WAIT",
    "create database graph.db nowait" ->
      "CREATE DATABASE `graph.db`",
    "create database graph.db if not exists wait" ->
      "CREATE DATABASE `graph.db` IF NOT EXISTS WAIT",
    "create database graph.db options {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'} wait" ->
      "CREATE DATABASE `graph.db` OPTIONS {existingData: \"use\", existingDataSeedInstance: \"84c3ee6f-260e-47db-a4b6-589c807f2c2e\"} WAIT",
    "create database graph.db options {`backticked key`: 'use'} WAIT" ->
      "CREATE DATABASE `graph.db` OPTIONS {`backticked key`: \"use\"} WAIT",
    "create database graph.db options $ops wait" ->
      "CREATE DATABASE `graph.db` OPTIONS $ops WAIT",
    "create composite database composite" ->
      "CREATE COMPOSITE DATABASE composite",
    "create composite database `composite.DB`" ->
      "CREATE COMPOSITE DATABASE `composite.DB`",
    "create or replace composite database composite" ->
      "CREATE OR REPLACE COMPOSITE DATABASE composite",
    "create composite database composite if not exists" ->
      "CREATE COMPOSITE DATABASE composite IF NOT EXISTS",
    "create composite database composite if not exists wait 10 seconds" ->
      "CREATE COMPOSITE DATABASE composite IF NOT EXISTS WAIT 10 SECONDS",
    "create composite database $c" ->
      "CREATE COMPOSITE DATABASE $c",
    "create or replace composite database $c" ->
      "CREATE OR REPLACE COMPOSITE DATABASE $c",
    "create composite database $c if not exists" ->
      "CREATE COMPOSITE DATABASE $c IF NOT EXISTS",
    "create composite database $c options {existingData: 'use'}" ->
      "CREATE COMPOSITE DATABASE $c OPTIONS {existingData: \"use\"}",
    "DROP database foO_Bar_42" ->
      "DROP DATABASE foO_Bar_42 DESTROY DATA",
    "DROP database $foo" ->
      "DROP DATABASE $foo DESTROY DATA",
    "DROP database foO_Bar_42 if EXISTS" ->
      "DROP DATABASE foO_Bar_42 IF EXISTS DESTROY DATA",
    "DROP database blah if EXISTS WAIT" ->
      "DROP DATABASE blah IF EXISTS DESTROY DATA WAIT",
    "DROP database foO_Bar_42 dump Data" ->
      "DROP DATABASE foO_Bar_42 DUMP DATA",
    "DROP database foO_Bar_42 Destroy DATA" ->
      "DROP DATABASE foO_Bar_42 DESTROY DATA",
    "DROP composite database foo Destroy DATA" ->
      "DROP COMPOSITE DATABASE foo DESTROY DATA",
    "DROP composite database $foo DUMP DATA" ->
      "DROP COMPOSITE DATABASE $foo DUMP DATA",
    "alter database foo set ACCESS read only" ->
      "ALTER DATABASE foo SET ACCESS READ ONLY".stripMargin,
    "alteR databaSe foo if EXISTS SEt access read WRITE" ->
      "ALTER DATABASE foo IF EXISTS SET ACCESS READ WRITE".stripMargin,
    "ALTER DATABASE foo SET topology 1 primary" ->
      "ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY",
    "ALTER DATABASE foo SET topology 1 primaries" ->
      "ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY",
    "ALTER DATABASE foo SET topology 2 PRIMARY" ->
      "ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES",
    "ALTER DATABASE foo SET topology 1 primary 2 secondary" ->
      "ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES",
    "ALTER DATABASE foo SET topology 1 primaries 2 secondaries" ->
      "ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES",
    "ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 2 PRIMARY" ->
      "ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES 1 SECONDARY",
    "ALTER DATABASE foo SET ACCESS read write SET TOPOLOGY 1 SECONDARY 2 PRIMARY" ->
      "ALTER DATABASE foo SET ACCESS READ WRITE SET TOPOLOGY 2 PRIMARIES 1 SECONDARY",
    "ALTER DATABASE foo SET OPTION key1 1" ->
      "ALTER DATABASE foo SET OPTION key1 1",
    "ALTER DATABASE foo SET OPTION `backticked option` 1" ->
      "ALTER DATABASE foo SET OPTION `backticked option` 1",
    "ALTER DATABASE foo SET OPTION key1 1 SET OPTION key2 true" ->
      "ALTER DATABASE foo SET OPTION key1 1 SET OPTION key2 true",
    "alter database foo set option KEY_A 'value' set option KEY_B 1.0" ->
      "ALTER DATABASE foo SET OPTION KEY_A \"value\" SET OPTION KEY_B 1.0",
    "alter database foo set option KEY_A \"a value\" set option KEY_B 1.0" ->
      "ALTER DATABASE foo SET OPTION KEY_A \"a value\" SET OPTION KEY_B 1.0",
    "alter database foo SET ACCESS read write set option KEY_A \"a value\" set option `some complex option key` 1.0" ->
      "ALTER DATABASE foo SET ACCESS READ WRITE SET OPTION KEY_A \"a value\" SET OPTION `some complex option key` 1.0",
    "alter database foo set option KEY_A \"a value\" SET ACCESS read write set option KEY_B 1.0" ->
      "ALTER DATABASE foo SET ACCESS READ WRITE SET OPTION KEY_A \"a value\" SET OPTION KEY_B 1.0",
    "ALTER DATABASE foo SET OPTION key1 1 set topology 1 PRIMARY" ->
      "ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET OPTION key1 1",
    "ALTER DATABASE foo REMOVE OPTION key1" ->
      "ALTER DATABASE foo REMOVE OPTION key1",
    "ALTER DATABASE foo REMOVE OPTION `backticked option`" ->
      "ALTER DATABASE foo REMOVE OPTION `backticked option`",
    "ALTER DATABASE foo REMOVE OPTION key1 REMOVE OPTION key2 REMOVE OPTION `some complex option key`" ->
      "ALTER DATABASE foo REMOVE OPTION key1 REMOVE OPTION key2 REMOVE OPTION `some complex option key`",
    "ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 2 PRIMARY wait" ->
      "ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES 1 SECONDARY WAIT",
    "ALTER DATABASE foo SET ACCESS READ WRITE WAIT 10" ->
      "ALTER DATABASE foo SET ACCESS READ WRITE WAIT 10 SECONDS",
    "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 sec" ->
      "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 SECONDS",
    "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 second" ->
      "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 SECONDS",
    "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 seconds" ->
      "ALTER DATABASE foo SET ACCESS READ ONLY WAIT 10 SECONDS",
    "start database $foo" ->
      "START DATABASE $foo",
    "start database foO_Bar_42" ->
      "START DATABASE foO_Bar_42",
    "start database graph.db" ->
      "START DATABASE graph.db",
    "stop database $foo" ->
      "STOP DATABASE $foo",
    "stop database foO_Bar_42" ->
      "STOP DATABASE foO_Bar_42",

    // alias commands
    "create alias alias FOR database database" ->
      "CREATE ALIAS alias FOR DATABASE database",
    "create alias alias if not exists for database database" ->
      "CREATE ALIAS alias IF NOT EXISTS FOR DATABASE database",
    "create or replace alias alias FOR database database" ->
      "CREATE OR REPLACE ALIAS alias FOR DATABASE database",
    "create alias composite.alias FOR database database" ->
      "CREATE ALIAS composite.alias FOR DATABASE database",
    "create alias composite.`alias.mine` FOR database database" ->
      "CREATE ALIAS composite.`alias.mine` FOR DATABASE database",
    "create alias `composite.alias.mine` FOR database database" ->
      "CREATE ALIAS `composite.alias.mine` FOR DATABASE database",
    "create or replace alias alias FOR database database properties {foo:7}" ->
      "CREATE OR REPLACE ALIAS alias FOR DATABASE database PROPERTIES {foo: 7}",
    "create or replace alias alias FOR database database properties { foo : $param }" ->
      "CREATE OR REPLACE ALIAS alias FOR DATABASE database PROPERTIES {foo: $param}",
    "create alias alias FOR database database at 'url' user user password 'password'" ->
      """CREATE ALIAS alias FOR DATABASE database AT "url" USER user PASSWORD '******'""",
    "create alias alias IF NOT EXISTS FOR database database at 'url' user user password 'password'" ->
      """CREATE ALIAS alias IF NOT EXISTS FOR DATABASE database AT "url" USER user PASSWORD '******'""",
    "create alias composite.alias FOR database database at 'url' user user password 'password'" ->
      """CREATE ALIAS composite.alias FOR DATABASE database AT "url" USER user PASSWORD '******'""",
    "create or replace alias alias FOR database database at 'url' user user password 'password' driver { ssl_enforced: $val }" ->
      """CREATE OR REPLACE ALIAS alias FOR DATABASE database AT "url" USER user PASSWORD '******' DRIVER {ssl_enforced: $val}""",
    "create alias $alias if not exists FOR database $database at $url user $user password $password driver { }" ->
      """CREATE ALIAS $alias IF NOT EXISTS FOR DATABASE $database AT $url USER $user PASSWORD $password DRIVER {}""",
    "create alias $alias if not exists FOR database $database at $url user $user password $password driver { } properties { }" ->
      """CREATE ALIAS $alias IF NOT EXISTS FOR DATABASE $database AT $url USER $user PASSWORD $password DRIVER {} PROPERTIES {}""",
    "create alias $alias if not exists FOR database $database at $url user $user password $password properties { foo: 'bar' }" ->
      """CREATE ALIAS $alias IF NOT EXISTS FOR DATABASE $database AT $url USER $user PASSWORD $password PROPERTIES {foo: "bar"}""",
    "alter alias alias if exists set database target database" ->
      "ALTER ALIAS alias IF EXISTS SET DATABASE TARGET database",
    "alter alias alias set database target database" ->
      "ALTER ALIAS alias SET DATABASE TARGET database",
    "alter alias $alias set database target $database at $url user $user password $password driver $driver" ->
      "ALTER ALIAS $alias SET DATABASE TARGET $database AT $url USER $user PASSWORD $password DRIVER $driver",
    "alter alias alias if exists set database target database at 'url'" ->
      """ALTER ALIAS alias IF EXISTS SET DATABASE TARGET database AT "url"""",
    "alter alias alias set database user user" ->
      "ALTER ALIAS alias SET DATABASE USER user",
    "alter alias alias set database password 'password'" ->
      "ALTER ALIAS alias SET DATABASE PASSWORD '******'",
    "alter alias composite.alias set database driver { ssl_enforced: true }" ->
      "ALTER ALIAS composite.alias SET DATABASE DRIVER {ssl_enforced: true}",
    "drop alias alias for database" ->
      "DROP ALIAS alias FOR DATABASE",
    "drop alias alias if exists for database" ->
      "DROP ALIAS alias IF EXISTS FOR DATABASE",
    "drop alias composite.alias for database" ->
      "DROP ALIAS composite.alias FOR DATABASE",
    "show alias for database" ->
      "SHOW ALIASES FOR DATABASE",
    "show aliases for database" ->
      "SHOW ALIASES FOR DATABASE",
    "show alias a.BLAh for database" ->
      "SHOW ALIAS a.BLAh FOR DATABASE",
    "show alias $foo for database" ->
      "SHOW ALIAS $foo FOR DATABASE",
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

    // server commands
    "enable server 'serverName'" ->
      """ENABLE SERVER "serverName"""".stripMargin,
    "enable server $param" ->
      "ENABLE SERVER $param",
    "enable server 'serverName' options {}" ->
      """ENABLE SERVER "serverName" OPTIONS {}""".stripMargin,
    "enable server 'serverName' options { tags:[ 'a' , 'b' ] }" ->
      """ENABLE SERVER "serverName" OPTIONS {tags: ["a", "b"]}""".stripMargin,
    "enable server 'serverName' options $optionsParam" ->
      """ENABLE SERVER "serverName" OPTIONS $optionsParam""".stripMargin,
    "alter server 'serverName' set options {}" ->
      """ALTER SERVER "serverName" SET OPTIONS {}""".stripMargin,
    "alter server 'serverName' set options { tags:[ 'a' , 'b' ] }" ->
      """ALTER SERVER "serverName" SET OPTIONS {tags: ["a", "b"]}""".stripMargin,
    "alter server 'serverName' set options $optionsParam" ->
      """ALTER SERVER "serverName" SET OPTIONS $optionsParam""".stripMargin,
    "rename server 'serverName' to 'newName'" ->
      """RENAME SERVER "serverName" TO "newName"""",
    "rename server 'serverName' to $new" ->
      """RENAME SERVER "serverName" TO $new""",
    "rename server $old to 'newName'" ->
      """RENAME SERVER $old TO "newName"""",
    "rename server $old to $new" ->
      """RENAME SERVER $old TO $new""",
    "drop server 'abc'" ->
      """DROP SERVER "abc"""",
    "drop server $abc" ->
      "DROP SERVER $abc",
    "deallocate databases from server 'abc'" ->
      """DEALLOCATE DATABASES FROM SERVER "abc"""",
    "deallocate database from server $name, 'abc'" ->
      """DEALLOCATE DATABASES FROM SERVERS $name, "abc"""",
    "dryrun deallocate databases from server 'abc'" ->
      """DRYRUN DEALLOCATE DATABASES FROM SERVER "abc"""",
    "dryrun deallocate database from server $name, 'abc'" ->
      """DRYRUN DEALLOCATE DATABASES FROM SERVERS $name, "abc"""",
    "reallocate database" ->
      """REALLOCATE DATABASES""",
    "reallocate databases" ->
      """REALLOCATE DATABASES""",
    "dryrun reallocate database" ->
      """DRYRUN REALLOCATE DATABASES""",
    "dryrun reallocate databases" ->
      """DRYRUN REALLOCATE DATABASES""",
    "show servers" ->
      "SHOW SERVERS",
    "show servers YIELD * where name = 'serverId' Return *" ->
      """SHOW SERVERS
        |  YIELD *
        |    WHERE name = "serverId"
        |  RETURN *""".stripMargin,
    "show servers YIELD * Return DISTINCT name" ->
      """SHOW SERVERS
        |  YIELD *
        |  RETURN DISTINCT name""".stripMargin,
    "show servers yield name order by name skip 1 limit 1 where name='serverId'" ->
      """SHOW SERVERS
        |  YIELD name
        |    ORDER BY name ASCENDING
        |    SKIP 1
        |    LIMIT 1
        |    WHERE name = "serverId"""".stripMargin
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
          s"$action traverse on graph * for (a) where a.b=1 $preposition role" ->
            s"$action TRAVERSE ON GRAPH * FOR (a) WHERE a.b = 1 $preposition role",
          s"$action traverse on graph * for (a) where not a.b=1 $preposition role" ->
            s"$action TRAVERSE ON GRAPH * FOR (a) WHERE NOT a.b = 1 $preposition role",
          s"$action traverse on graph * for (a) where a.b = 1 (*) $preposition role" ->
            s"$action TRAVERSE ON GRAPH * FOR (a) WHERE a.b = 1 $preposition role",
          s"$action traverse on graph foo for (n) where n.a=true $preposition role" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n) WHERE n.a = true $preposition role",
          s"$action traverse on graph $$foo for (:A {a:$$foo}) $preposition $$role" ->
            s"$action TRAVERSE ON GRAPH $$foo FOR (n:A) WHERE n.a = $$foo $preposition $$role",
          s"$action traverse on graph FoO for (Bar) where Bar.fOO IS NULL $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE Bar.fOO IS NULL $preposition role",
          s"$action traverse on graph FoO for (Bar) where Not Bar.fOO IS NULL $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IS NULL $preposition role",
          s"$action traverse on graph `#%¤` for (`()/&`) where `()/&`.`¤`='&' $preposition role" ->
            s"""$action TRAVERSE ON GRAPH `#%¤` FOR (`()/&`) WHERE `()/&`.`¤` = "&" $preposition role""",
          s"$action traverse on graph foo for (n:A|B|C) where n.prop<>true $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE n.prop <> true $preposition x, y, z",
          s"$action traverse on graph foo for (n:A|B|C) where NOT n.prop<>true $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE NOT n.prop <> true $preposition x, y, z",
          s"$action traverse on graph foo for (n:A|B|C {prop:true}) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE n.prop = true $preposition x, y, z",
          s"$action traverse on graph foo for (n:A|B|C where n.prop<>true) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE n.prop <> true $preposition x, y, z",
          s"$action traverse on graph foo for (n:A|B|C where n.prop is not null) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE n.prop IS NOT NULL $preposition x, y, z",
          s"$action traverse on graph foo for (n:A|B|C where not n.prop is not null) $preposition x,y,z" ->
            s"$action TRAVERSE ON GRAPH foo FOR (n:A|B|C) WHERE NOT n.prop IS NOT NULL $preposition x, y, z",
          s"$action traverse on graph FoO for (Bar) where Bar.fOO in [$$foo] $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE Bar.fOO IN [$$foo] $preposition role",
          s"$action traverse on graph FoO for (Bar) where not Bar.fOO in [1] $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [1] $preposition role",
          s"$action traverse on graph FoO for (Bar) where not Bar.fOO in [1,2,3] $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [1, 2, 3] $preposition role",
          s"$action traverse on graph FoO for (Bar) where not Bar.fOO in [1,'string',false] $preposition role" ->
            s"""$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [1, "string", false] $preposition role""",
          s"$action traverse on graph FoO FOR (Bar) WHERE not Bar.fOO in $$foo $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN $$foo $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE 1 > Bar.fOO $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE 1 > Bar.fOO $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE not Bar.fOO > $$foo $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO > $$foo $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE not Bar.fOO >= 1.0 $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO >= 1.0 $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE not Bar.fOO < $$foo $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO < $$foo $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE Bar.fOO <= 1.0 $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE Bar.fOO <= 1.0 $preposition role",
          s"$action traverse on graph FoO FOR (Bar) WHERE not Bar.fOO <= $$foo $preposition role" ->
            s"$action TRAVERSE ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO <= $$foo $preposition role",
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
          s"$action READ {`x\u0885y`} on graph `x\u0885y` nodes `x\u0885y` $preposition `x\u0885y`" ->
            s"$action READ {`x\u0885y`} ON GRAPH `x\u0885y` NODE `x\u0885y` $preposition `x\u0885y`",
          s"$action read { `&bar` } on graph `#%¤` nodes `()/&` (*) $preposition role" ->
            s"$action READ {`&bar`} ON GRAPH `#%¤` NODE `()/&` $preposition role",
          s"$action read {foo,bar} on graph foo nodes A,B,C (*) $preposition x,y,$$z" ->
            s"$action READ {foo, bar} ON GRAPH foo NODES A, B, C $preposition x, y, $$z",
          s"$action read {*} on graph * for (n:A) where n.prop = 1 $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (n:A) WHERE n.prop = 1 $preposition role",
          s"$action read {*} on graph * for (n:A) where not n.prop = 1 $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (n:A) WHERE NOT n.prop = 1 $preposition role",
          s"$action read {*} on graph * for (n) where n.p=$$foo $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (n) WHERE n.p = $$foo $preposition role",
          s"$action read {*} on graph * foR (N) WHERe N.p <>2 $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (N) WHERE N.p <> 2 $preposition role",
          s"$action read {*} on graph * foR (N) WHERe Not N.p <>2 $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (N) WHERE NOT N.p <> 2 $preposition role",
          s"$action read {*} on graph foo For (n:Ab) where n.p is null $preposition role" ->
            s"$action READ {*} ON GRAPH foo FOR (n:Ab) WHERE n.p IS NULL $preposition role",
          s"$action read {*} on graph foo For (n:Ab) where NOT n.p is null $preposition role" ->
            s"$action READ {*} ON GRAPH foo FOR (n:Ab) WHERE NOT n.p IS NULL $preposition role",
          s"$action read {*} on graph foo FOR (ab:A) where ab.p is not null $preposition role" ->
            s"$action READ {*} ON GRAPH foo FOR (ab:A) WHERE ab.p IS NOT NULL $preposition role",
          s"$action read {*} on graph foo FOR (ab:A) where not ab.p is not null $preposition role" ->
            s"$action READ {*} ON GRAPH foo FOR (ab:A) WHERE NOT ab.p IS NOT NULL $preposition role",
          s"$action read {bar} on graph FoO for (:A {prop: 1}) $preposition role" ->
            s"$action READ {bar} ON GRAPH FoO FOR (n:A) WHERE n.prop = 1 $preposition role",
          s"$action read { `&bar` } on graph `#%¤` for (`%¤`:`()/&`) where `%¤`.`¤` <> '#' $preposition role" ->
            s"""$action READ {`&bar`} ON GRAPH `#%¤` FOR (`%¤`:`()/&`) WHERE `%¤`.`¤` <> "#" $preposition role""",
          s"$action read {*} on graph foo for (n:A|B|C where n.prop<>true) $preposition x,y,z" ->
            s"$action READ {*} ON GRAPH foo FOR (n:A|B|C) WHERE n.prop <> true $preposition x, y, z",
          s"$action READ {bar} on graph foo for (n:A|B|C where n.prop is not null) $preposition x,y,z" ->
            s"$action READ {bar} ON GRAPH foo FOR (n:A|B|C) WHERE n.prop IS NOT NULL $preposition x, y, z",
          s"$action read {*} on graph * for (n:A|B) where n.p=1 $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (n:A|B) WHERE n.p = 1 $preposition role",
          s"$action read {*} on graph * for (:A|B {p:1}) $preposition role" ->
            s"$action READ {*} ON GRAPH * FOR (n:A|B) WHERE n.p = 1 $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE Bar.fOO in [1] $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO IN [1] $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO in [$$foo] $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [$$foo] $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO in [$$foo, $$bar] $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [$$foo, $$bar] $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO in $$foo $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN $$foo $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE Bar.fOO > 1 $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO > 1 $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO > $$foo $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO > $$foo $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO >= 1.0 $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO >= 1.0 $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not $$foo < Bar.fOO $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT $$foo < Bar.fOO $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE 1.0 <= Bar.fOO $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE 1.0 <= Bar.fOO $preposition role",
          s"$action read {*} on graph FoO FOR (Bar) WHERE not Bar.fOO <= $$foo $preposition role" ->
            s"$action READ {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO <= $$foo $preposition role",
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
          s"$action match {*} on graph * for (n) where n.prop = $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n) WHERE n.prop = $$foo $preposition role",
          s"$action match {*} on graph * for (n) where not n.prop = true $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n) WHERE NOT n.prop = true $preposition role",
          s"$action match {*} on graph * for (n:A) WHERE n.prop is not NULL (*) $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n:A) WHERE n.prop IS NOT NULL $preposition role",
          s"$action match {*} on graph foo for (:A{prop:true}) $preposition role" ->
            s"$action MATCH {*} ON GRAPH foo FOR (n:A) WHERE n.prop = true $preposition role",
          s"$action match {*} on graph foo for (aB:A) where aB.a <> 'ba' $preposition role" ->
            s"""$action MATCH {*} ON GRAPH foo FOR (aB:A) WHERE aB.a <> "ba" $preposition role""",
          s"$action match {*} on graph foo for (aB:A) where nOT aB.a <> 'ba' $preposition role" ->
            s"""$action MATCH {*} ON GRAPH foo FOR (aB:A) WHERE NOT aB.a <> "ba" $preposition role""",
          s"$action match {bar} on graph foo FoR (a:A) where a.bar = 'bar' $preposition role" ->
            s"""$action MATCH {bar} ON GRAPH foo FOR (a:A) WHERE a.bar = "bar" $preposition role""",
          s"$action match {*} on graph * for (n:A|B) WHERE n.prop is null $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n:A|B) WHERE n.prop IS NULL $preposition role",
          s"$action match {*} on graph * for (n:A|B) WHERE NOT n.prop is null $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n:A|B) WHERE NOT n.prop IS NULL $preposition role",
          s"$action match {*} on graph * for (n:A|B {prop:1}) $preposition role" ->
            s"$action MATCH {*} ON GRAPH * FOR (n:A|B) WHERE n.prop = 1 $preposition role",
          s"$action match {bar} on graph foo for (n:A|B|C where n.prop<>true) $preposition x,y,z" ->
            s"$action MATCH {bar} ON GRAPH foo FOR (n:A|B|C) WHERE n.prop <> true $preposition x, y, z",
          s"$action MATCH {*} on graph foo for (n:A|B|C where n.prop is not null) $preposition x,y,z" ->
            s"$action MATCH {*} ON GRAPH foo FOR (n:A|B|C) WHERE n.prop IS NOT NULL $preposition x, y, z",
          s"$action MATCH {*} on graph foo for (n:A|B|C where not n.prop is not null) $preposition x,y,z" ->
            s"$action MATCH {*} ON GRAPH foo FOR (n:A|B|C) WHERE NOT n.prop IS NOT NULL $preposition x, y, z",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO in [TRUE] $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO IN [true] $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO in [TRUE, false] $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO IN [true, false] $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not Bar.fOO in [$$foo] $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN [$$foo] $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not Bar.fOO in $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO IN $$foo $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO > 1 $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO > 1 $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not Bar.fOO > $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO > $$foo $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not 1.0 >= Bar.fOO $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT 1.0 >= Bar.fOO $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO >= $$foo $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO < 1 $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not Bar.fOO < $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO < $$foo $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE Bar.fOO <= 1.0 $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE Bar.fOO <= 1.0 $preposition role",
          s"$action match {*} on graph FoO FOR (Bar) WHERE not Bar.fOO <= $$foo $preposition role" ->
            s"$action MATCH {*} ON GRAPH FoO FOR (Bar) WHERE NOT Bar.fOO <= $$foo $preposition role",
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
            s"$action MERGE {Foo, BAR} ON DEFAULT GRAPH ELEMENTS * $preposition role"
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
              s"$action $databaseAction on databases `F.o.O` $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE `F.o.O` $preposition role",
              s"$action $databaseAction on databases F.o.O $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DATABASE F.`o.O` $preposition role",
              s"$action $databaseAction on home database $preposition role" ->
                s"$action $prettifiedDatabaseAction ON HOME DATABASE $preposition role",
              s"$action $databaseAction on default database $preposition role" ->
                s"$action $prettifiedDatabaseAction ON DEFAULT DATABASE $preposition role"
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
          s"$action terminate transaction (`\u0885`) on database `\u0885` $preposition `\u0885`" ->
            s"$action TERMINATE TRANSACTION (`\u0885`) ON DATABASE `\u0885` $preposition `\u0885`",
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
          s"$action set auth on dbms $preposition role" ->
            s"$action SET AUTH ON DBMS $preposition role",
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
          s"$action alter database on dbms $preposition role" ->
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
            s"$action SHOW ALIAS ON DBMS $preposition role",
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
          s"$action execute procedure math.`sin.`. on dbms $preposition role" ->
            s"$action EXECUTE PROCEDURE `math.sin..` ON DBMS $preposition role",
          s"$action execute procedure `..math..sin..`.`..math..sin..` on dbms $preposition role" ->
            s"$action EXECUTE PROCEDURE `..math..sin.....math..sin..` ON DBMS $preposition role",
          s"$action execute boosted procedure * on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE * ON DBMS $preposition role",
          s"$action execute boosted procedures * on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE * ON DBMS $preposition role",
          s"$action execute boosted procedure math.`s/n`, `ma/*`.`*a?`,math.cos on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE math.`s/n`, `ma/*`.*a?, math.cos ON DBMS $preposition role",
          s"$action execute boosted procedure `math.` on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED PROCEDURE math. ON DBMS $preposition role",
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
          s"$action execute functions `*.` on dbms $preposition role" ->
            s"$action EXECUTE USER DEFINED FUNCTION *. ON DBMS $preposition role",
          s"$action execute boosted function math.sin, ma*.`*/a?`,math.`c%s` on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION math.sin, ma*.`*/a?`, math.`c%s` ON DBMS $preposition role",
          s"$action execute boosted function ma*., math.si*. on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION ma*., math.si*. ON DBMS $preposition role",
          s"$action execute boosted user function apoc.math on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION apoc.math ON DBMS $preposition role",
          s"$action execute boosted user defined functions ??? on dbms $preposition role" ->
            s"$action EXECUTE BOOSTED USER DEFINED FUNCTION ??? ON DBMS $preposition role",
          s"$action impersonate on dbms $preposition role" ->
            s"$action IMPERSONATE (*) ON DBMS $preposition role",
          s"$action impersonate (*) on dbms $preposition role, $$paramrole" ->
            s"$action IMPERSONATE (*) ON DBMS $preposition role, $$paramrole",
          s"$action impersonate (foo,bar) on dbms $preposition role" ->
            s"$action IMPERSONATE (foo, bar) ON DBMS $preposition role",
          s"$action server management on dbms $preposition role" ->
            s"$action SERVER MANAGEMENT ON DBMS $preposition role",
          s"$action server management on dbms $preposition role, $$paramrole" ->
            s"$action SERVER MANAGEMENT ON DBMS $preposition role, $$paramrole",
          s"$action show server on dbms $preposition role" ->
            s"$action SHOW SERVERS ON DBMS $preposition role",
          s"$action show servers on dbms $preposition role, $$paramrole" ->
            s"$action SHOW SERVERS ON DBMS $preposition role, $$paramrole",
          s"$action show setting * on dbms $preposition role" ->
            s"$action SHOW SETTING * ON DBMS $preposition role",
          s"$action show settings * on dbms $preposition role" ->
            s"$action SHOW SETTING * ON DBMS $preposition role",
          s"$action show setting math.sin, ma*.`*/a?`,math.`c%s` on dbms $preposition role" ->
            s"$action SHOW SETTING math.sin, ma*.`*/a?`, math.`c%s` ON DBMS $preposition role",
          s"$action show setting `*/a?`. on dbms $preposition role" ->
            s"$action SHOW SETTING `*/a?`. ON DBMS $preposition role",
          s"$action load on all data $preposition role" -> s"$action LOAD ON ALL DATA $preposition role",
          s"$action load on cidr '192.168.1.6/20' $preposition role" -> s"""$action LOAD ON CIDR "192.168.1.6/20" $preposition role""",
          s"$action load on cidr '192.168.1.6/20' $preposition `\u0885`" -> s"""$action LOAD ON CIDR "192.168.1.6/20" $preposition `\u0885`""",
          s"$action load on cidr $$cidr $preposition role" -> s"""$action LOAD ON CIDR $$cidr $preposition role""",
          s"$action load on url 'ftp://www.data.com/mydata/*' $preposition role" -> s"""$action LOAD ON URL "ftp://www.data.com/mydata/*" $preposition role""",
          s"$action load on url $$url $preposition role" -> s"""$action LOAD ON URL $$url $preposition role"""
        )
    }
  }

  tests foreach {
    case (inputString, expected) if parsesSameInAllCypherVersions(inputString) =>
      test(inputString) {
        val statementJavaCc = rewriteASTDifferences(JavaCCParser.parse(inputString, OpenCypherExceptionFactory(None)))
        prettifier.asString(statementJavaCc) should equal(expected)

        CypherVersion.values().foreach { version =>
          val statement = rewriteASTDifferences(parseAntlr(version, inputString))
          statement shouldBe statementJavaCc
          prettifier.asString(statement) should equal(expected)
        }
      }
    case (inputString, expectedNotCypher5) =>
      test(inputString) {
        // The two Cypher 5 parsers should get the same values
        val statementJavaCc = JavaCCParser.parse(inputString, OpenCypherExceptionFactory(None))
        val statementCypher5 = parseAntlr(CypherVersion.Cypher5, inputString)
        statementCypher5 shouldBe statementJavaCc
        prettifier.asString(statementJavaCc) should equal(prettifier.asString(statementCypher5))

        CypherVersion.values().toList.diff(Seq(CypherVersion.Cypher5)).foreach { version =>
          val statement = parseAntlr(version, inputString)
          prettifier.asString(statement) should equal(expectedNotCypher5)
        }
      }
  }

  /**
   * There are some AST changes done at the parser level for semantic analysis that won't affect the plan.
   * This rewriter can be expanded to update those parts.
   */
  def rewriteASTDifferences(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case u: UnionDistinct => u.copy(differentReturnOrderAllowed = true)(u.position)
      case u: UnionAll      => u.copy(differentReturnOrderAllowed = true)(u.position)
    }))
  }

  private def parseAntlr(version: CypherVersion, cypher: String): Statement =
    AstParserFactory(version)(cypher, Neo4jCypherExceptionFactory(cypher, None), None).singleStatement()

  private def parsesSameInAllCypherVersions(inputString: String): Boolean = {
    // to compare case insensitively
    val inputLowerCase = inputString.toLowerCase

    // showing constraints differs between Cypher 5 and Cypher 6
    // removing 'database' as that is the privileges and not the show command
    !(inputLowerCase.matches(".*show.*constraint.*") && !inputLowerCase.contains("database"))
  }
}
