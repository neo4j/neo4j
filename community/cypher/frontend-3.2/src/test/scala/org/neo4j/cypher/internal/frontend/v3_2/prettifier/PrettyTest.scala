/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.prettifier

import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class PrettyTest extends CypherFunSuite {

  test("should upcase keywords") {
    actual("create (n)") should equal(expected("CREATE (n)"))
  }

  test("should not break INDEX ON") {
    actual("create index on :Person(name)") should equal(expected("CREATE INDEX ON :Person(name)"))
  }

  test("should not break on ASC") {
    actual("RETURN n order by n.name asc") should equal(expected("RETURN n ORDER BY n.name ASC"))
  }

  test("should not break CREATE in FOREACH") {
    actual("match p=(n) foreach(x in p | create (x)--())") should equal(expected(
      "MATCH p = (n)%n" +
      "FOREACH (x IN p |%n" +
      "  CREATE (x)--()%n" +
      ")"))
  }

  test("should not break CREATE in complex FOREACH") {
    actual("match p=(n) foreach(x in p | create (x)--() set x.foo = 'bar') return distinct p;") should equal(
      expected(
        "MATCH p = (n)%n" +
        "FOREACH (x IN p |%n" +
        "  CREATE (x)--()%n" +
        "  SET x.foo = 'bar'%n" +
        ")%n" +
        "RETURN DISTINCT p")
    )
  }

  test("should not break STARTS WITH ") {
    actual("return 'apartment' starts with 'apa' as x") should equal(
      expected("RETURN 'apartment' STARTS WITH 'apa' AS x")
    )
  }

  test("should not break ENDS WITH ") {
    actual("return 'apartment' ends with 'apa' as x") should equal(
      expected("RETURN 'apartment' ENDS WITH 'apa' AS x")
    )
  }

  test("should not break CONSTRAINT ON") {
    actual("create constraint on (person:Person) assert person.age is unique") should equal(
      expected("CREATE CONSTRAINT ON (person:Person) ASSERT person.age IS UNIQUE")
    )
  }

  test("should break ON CREATE") {
    actual("merge (n) on create set n.age=32") should equal(expected("MERGE (n)%n  ON CREATE SET n.age = 32"))
  }

  test("should correctly handle parenthesis in MATCH") {
    actual("match (a)-->(b) return b") should equal(expected("MATCH (a)-->(b)%nRETURN b"))
  }

  test("should upcase multiple keywords") {
    actual("match (n) where n.name='B' return n") should equal(expected("MATCH (n)%n  WHERE n.name = 'B'%nRETURN n"))
  }

  test("should upcase multiple keywords 2") {
    actual("match (a) where a.name='A' return a.age as SomethingTotallyDifferent") should equal(
      expected("MATCH (a)%n  WHERE a.name = 'A'%nRETURN a.age AS SomethingTotallyDifferent")
    )
  }

  test("should not break CREATE UNIQUE") {
    actual("start me=node(3) match p1=(me)-[*2]-(friendOfFriend) " +
      "create p2=(me)-[:MARRIED_TO]-(wife { name: \"Gunhild\" }) " +
      "create unique p3=(wife)-[:KNOWS]-(friendOfFriend) " +
      "return p1,p2,p3") should equal(
      expected(
        "START me = node(3)%n" +
          "MATCH p1 = (me)-[*2]-(friendOfFriend)%n" +
          "CREATE p2 = (me)-[:MARRIED_TO]-(wife {name: 'Gunhild'})%n" +
          "CREATE UNIQUE p3 = (wife)-[:KNOWS]-(friendOfFriend)%n" +
          "RETURN p1, p2, p3"
      ))
  }

  test("should not break WHERE in comprehensions") {
    actual("return [x in range(0,10) where x + 2 = 0 | x^3] as result") should equal(
      expected("RETURN [x IN RANGE(0, 10) WHERE x + 2 = 0 | x ^ 3] AS result")
    )
  }

  test("should upcase extra keywords") {
    actual("match (david)--(otherPerson)-->() where david.name='David' with otherPerson, count(*) as foaf where foaf > 1 return otherPerson") should equal(
      expected("MATCH (david)--(otherPerson)-->()%n  WHERE david.name = 'David'%nWITH otherPerson, COUNT(*) AS foaf%n  WHERE foaf > 1%nRETURN otherPerson")
    )

  }

  test("should not break after OPTIONAL") {
    actual("optional MATCH (n)-->(x) return n, x") should equal(expected("OPTIONAL MATCH (n)-->(x)%nRETURN n, x"))
  }


  test("should handle LOAD CSV") {
    actual("LOAD CSV FROM 'f' AS line") should equal(expected("LOAD CSV FROM 'f' AS line"))
  }

  test("should handle LOAD CSV WITH HEADERS") {
    actual("LOAD CSV wiTh HEADERS FROM 'f' AS line") should equal(expected("LOAD CSV WITH HEADERS FROM 'f' AS line"))

  }

  test("should prettify and break LOAD CSV") {
    actual("MATCH (n) LOAD CSV FROM \"f\" AS line return (n)") should equal(
      expected("MATCH (n)%nLOAD CSV FROM 'f' AS line%nRETURN (n)")
    )
  }

  test("should not break after DETACH in DETACH DELETE") {
    actual("MATCH (n) DETACH DELETE (n)") should equal(
      expected("MATCH (n)%nDETACH DELETE (n)")
    )
  }

  test("should prettify with correct string quotes") {
    actual("mATCH (a) WhERE a.name='A' RETURN a.age > 30, \"I'm a literal\", (a)-->()") should equal(
      expected("MATCH (a)%n  WHERE a.name = 'A'%nRETURN a.age > 30, \"I'm a literal\", (a)-->()")
    )
  }

  test("should handle join hints") {
    actual("match (a:A)-->(b:B) USING join ON b return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING JOIN ON b%nRETURN a.prop")
    )
  }

  test("should handle index hints") {
    actual("match (a:A)-->(b:B) USING index b:B ( prop ) return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING INDEX b:B(prop)%nRETURN a.prop")
    )
  }

  test("should handle scan hints") {
    actual("match (a:A)-->(b:B) USING scan b:B return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING SCAN b:B%nRETURN a.prop")
    )
  }

  test("should handle CALL YIELD") {
    actual("match (n) call db.indexes yield state RETURN *") should equal(
      expected("MATCH (n)%nCALL db.indexes YIELD state%nRETURN *"))
  }

  test("MERGE should start on a new line") {
    actual("MERGE (a:A) MERGE (b:B) MERGE (a)-[:T]->(b) RETURN *") should equal(expected(
      "MERGE (a:A)%nMERGE (b:B)%nMERGE (a)-[:T]->(b)%nRETURN *"))
  }

  test("UNWIND should start on a new line") {
    actual("WITH [1,2,2] AS coll UNWIND coll AS x RETURN collect(x)", preserveColumnNames = true) should equal(
      expected("WITH [1, 2, 2] AS coll%nUNWIND coll AS x%nRETURN collect(x)"))
  }

  test("expression precedence does not bother me") {
    actual("return (1 + 2) * 3") should equal(
      expected("RETURN (1 + 2) * 3"))
  }

  private val parser = new CypherParser

  private def actual(text: String, preserveColumnNames: Boolean = false): String = {
    val pretty = Pretty(preserveColumnNames)
    val ast = parser.parse(text)
    val reformatted = pretty.pretty(pretty.show(ast)).layout
    val secondRound = parser.parse(reformatted)
    ast should equal(secondRound) // This means that the pretty printed query, when parsed, returns the original AST
    reformatted
  }

  private def expected(text: String) = String.format(text)
}
