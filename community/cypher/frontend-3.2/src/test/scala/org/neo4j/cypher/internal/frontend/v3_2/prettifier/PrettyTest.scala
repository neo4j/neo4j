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

  //CATEGORY A: all tests simply change the formatting. The semantics of the query itself remains unaltered.
  //Moreover, the formatting changes are deemed lightweight enough to be applied across the board: manual, TCK and the browser
  test("handle parentheses in MATCH") {
    actual("match (a)-->(b) return b") should equal(
      expected("MATCH (a)-->(b)%nRETURN b"))
  }

  test("handle LOAD CSV") {
    actual("LOAD CSV FROM 'f' AS line") should equal(
      expected("LOAD CSV FROM 'f' AS line"))
  }

  test("handle LOAD CSV WITH HEADERS") {
    actual("LOAD CSV wiTh HEADERS FROM 'f' AS line") should equal(
      expected("LOAD CSV WITH HEADERS FROM 'f' AS line"))
  }

  test("prettify with correct string quotes: use double when single used in literal") {
    actual("mATCH (a) WhERE a.name='A' RETURN a.age > 30, \"I'm a literal\", (a)-->()") should equal(
      expected("MATCH (a)%n  WHERE a.name = 'A'%nRETURN a.age > 30, \"I'm a literal\", (a)-->()")
    )
  }

  test("expression precedence does not bother me") {
    actual("return (1 + 2) * 3") should equal(
      expected("RETURN (1 + 2) * 3"))
  }

  //Casing tests
  test("upper case keywords") {
    actual("create (n)") should equal(
      expected("CREATE (n)"))
  }

  test("upper case multiple keywords") {
    actual("match (n) where n.name='B' return n") should equal(
      expected("MATCH (n)%n  WHERE n.name = 'B'%nRETURN n"))
  }

  test("upper case multiple keywords 2") {
    actual("match (a) where a.name='A' return a.age as somethingTotallyDifferent") should equal(
      expected("MATCH (a)%n  WHERE a.name = 'A'%nRETURN a.age AS somethingTotallyDifferent")
    )
  }

  test("upper case extra keywords") {
    actual("match (david)--(otherPerson)-->() where david.name='David' with otherPerson, count(*) as foaf where foaf > 1 return otherPerson") should equal(
      expected("MATCH (david)--(otherPerson)-->()%n  WHERE david.name = 'David'%nWITH otherPerson, COUNT(*) AS foaf%n  WHERE foaf > 1%nRETURN otherPerson")
    )
  }

  test("lower case the value `null`") {
    actual("WITH NULL AS n1, Null AS n2 RETURN n1, n2") should equal(
      expected("WITH null AS n1, null AS n2%nRETURN n1, n2")
    )
  }

  test("upper case `IS [NOT] NULL`") {
    actual("match (n:Vehicle) where n.model is not null and n.make is Null return n.name") should equal(
      expected("MATCH (n:Vehicle)%n  WHERE n.model IS NOT NULL AND n.make IS NULL%nRETURN n.name")
    )
  }

  test("lower case boolean literals") {
    actual("WITH TRUE AS n1, False AS n2 RETURN n1, n2") should equal(
      expected("WITH true AS n1, false AS n2%nRETURN n1, n2")
    )
  }

  test("start a function name with a lower case character") {
    actual("match (n:Person) where Upper(n.city) = 'JOHANNESBURG' return n.name") should equal(
      expected("MATCH (n:Person)%n  WHERE upper(n.city) = 'JOHANNESBURG'%nRETURN n.name")
    )
  }

  //Spacing tests
  test("no space between key and colon in literal map") {
    actual("MATCH (n:Person {age : 45}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45})%nRETURN n.name"))
  }

  test("no space between opening brace and the first key in literal map") {
    actual("MATCH (n:Person { age: 45}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45})%nRETURN n.name"))
  }

  test("one space between colon and value in literal map") {
    actual("MATCH (n:Person {age:45, name:  'Anne'}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45, name: 'Anne'})%nRETURN n.name"))
  }

  test("no space between value and comma in literal map") {
    actual("MATCH (n:Person {age: 45 , name: 'Anne'}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45, name: 'Anne'})%nRETURN n.name"))
  }

  test("one space between comma and the next key in literal map") {
    actual("MATCH (n:Person {age: 45,name: 'Anne',  eyeColour: 'grey'}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45, name: 'Anne', eyeColour: 'grey'})%nRETURN n.name"))
  }

  test("no space between the last value and the closing brace in literal map") {
    actual("MATCH (n:Person {age: 45 }) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45})%nRETURN n.name"))
  }

  test("one space between label predicate and property predicate") {
    actual("MATCH (n:Person{age: 45})-->(m:Person  {age: 30}) RETURN n.name") should equal(
      expected("MATCH (n:Person {age: 45})-->(m:Person {age: 30})%nRETURN n.name"))
  }

  test("one space between relationship type predicate and property predicate") {
    actual("MATCH (n)-[:KNOWS{since: 2015}]->(m)-[:KNOWS  {since: 2010}]->(o) RETURN n.name") should equal(
      expected("MATCH (n)-[:KNOWS {since: 2015}]->(m)-[:KNOWS {since: 2010}]->(o)%nRETURN n.name"))
  }

  test("no space in patterns") {
    actual("MATCH (n:Person) - [:DRIVES] -> (:Vehicle) RETURN n.name") should equal(
      expected("MATCH (n:Person)-[:DRIVES]->(:Vehicle)%nRETURN n.name"))
  }

  test("wrapping space around operators") {
    actual("MATCH p=(m)-->(n) WHERE m.age<>n.age RETURN n.name") should equal(
      expected("MATCH p = (m)-->(n)%n  WHERE m.age <> n.age%nRETURN n.name"))
  }

  test("no space in label predicates") {
    actual("MATCH (p:  Person  : Owner ) RETURN p.name") should equal(
      expected("MATCH (p:Person:Owner)%nRETURN p.name"))
  }

  test("one space after each comma in lists and enumerations") {
    actual("WITH ['a','b',3.14] AS list RETURN list,2,3,4") should equal(
      expected("WITH ['a', 'b', 3.14] AS list%nRETURN list, 2, 3, 4"))
  }

  //Backtick tests
  test("handle backticks in a label") {
    actual("MATCH (n:`Person Employee`) RETURN n.name") should equal(
      expected("MATCH (n:`Person Employee`)%nRETURN n.name"))
  }

  test("handle backticks in an aliased column") {
    actual("MATCH (n) RETURN n.name AS `Column With Space`") should equal(
      expected("MATCH (n)%nRETURN n.name AS `Column With Space`"))
  }

  test("handle backticks in a relationship type") {
    actual("MATCH (n)-[r:`Has connection`]->(:Person) RETURN n.name") should equal(
      expected("MATCH (n)-[r:`Has connection`]->(:Person)%nRETURN n.name"))
  }

  test("handle backticks in a relationship type when multiple relationship types are given") {
    actual("MATCH (n)-[r:`Has connection`|`Has address`]->(:Person) RETURN n.name") should equal(
      expected("MATCH (n)-[r:`Has connection`|`Has address`]->(:Person)%nRETURN n.name"))
  }

  test("handle backticks in a key in a property map") {
    actual("MATCH (n {`first name`: 'John'}) RETURN n.name") should equal(
      expected("MATCH (n {`first name`: 'John'})%nRETURN n.name"))
  }

  test("handle backticks in a property name") {
    actual("MATCH (n) WHERE n.`first name` = 'John' RETURN n.name") should equal(
      expected("MATCH (n)%n  WHERE n.`first name` = 'John'%nRETURN n.name"))
  }

  //indenting tests for subclauses
  test("WHERE starts on a new line and is indented") {
    actual("MATCH (n) WHERE n.name = 'John' RETURN n.name") should equal(
      expected("MATCH (n)%n  WHERE n.name = 'John'%nRETURN n.name"))
  }

  test("ON CREATE starts on a new line and is indented") {
    actual("merge (n) on create set n.age=32") should equal(
      expected("MERGE (n)%n  ON CREATE SET n.age = 32"))
  }

  test("ON MATCH starts on a new line and is indented") {
    actual("merge (n) on match set n.age=32") should equal(
      expected("MERGE (n)%n  ON MATCH SET n.age = 32"))
  }

  test("SKIP starts on a new line and is indented") {
    actual("match (n) return n.name skip 3") should equal(
      expected("MATCH (n)%nRETURN n.name%n  SKIP 3"))
  }

  test("LIMIT starts on a new line and is indented") {
    actual("match (n) return n.name limit 3") should equal(
      expected("MATCH (n)%nRETURN n.name%n  LIMIT 3"))
  }

  test("ORDER BY starts on a new line and is indented") {
    actual("match (n) return n.name, n.age order by n.age") should equal(
      expected("MATCH (n)%nRETURN n.name, n.age%n  ORDER BY n.age"))
  }

  //positive line-breaking tests
  test("MATCH, WITH and RETURN start on new lines") {
    actual("match (n) with n as m return m.name") should equal(
      expected("MATCH (n)%nWITH n AS m%nRETURN m.name"))
  }

  test("SET starts on a new line") {
    actual("match (n) set n.surname = 'Smith' return n.name") should equal(
      expected("MATCH (n)%nSET n.surname = 'Smith'%nRETURN n.name"))
  }

  test("DELETE starts on a new line") {
    actual("match (n:Cat) delete n") should equal(
      expected("MATCH (n:Cat)%nDELETE n"))
  }

  test("REMOVE starts on a new line") {
    actual("match (n:Cat) remove n.size return n.name") should equal(
      expected("MATCH (n:Cat)%nREMOVE n.size%nRETURN n.name"))
  }

  test("LOAD CSV starts on a new line") {
    actual("MATCH (n) LOAD CSV FROM \"f\" AS line return (n)") should equal(
      expected("MATCH (n)%nLOAD CSV FROM 'f' AS line%nRETURN (n)")
    )
  }

  test("join hints start on a new line") {
    actual("match (a:A)-->(b:B) USING join ON b return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING JOIN ON b%nRETURN a.prop")
    )
  }

  test("index hints start on a new line") {
    actual("match (a:A)-->(b:B) USING index b:B ( prop ) return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING INDEX b:B(prop)%nRETURN a.prop")
    )
  }

  test("scan hints start on a new line") {
    actual("match (a:A)-->(b:B) USING scan b:B return a.prop") should equal(
      expected("MATCH (a:A)-->(b:B)%nUSING SCAN b:B%nRETURN a.prop")
    )
  }

  test("CALL starts on a new line and YIELD does not") {
    actual("match (n) call db.indexes yield state RETURN *") should equal(
      expected("MATCH (n)%nCALL db.indexes YIELD state%nRETURN *"))
  }

  test("MERGE starts on a new line") {
    actual("MERGE (a:A) MERGE (b:B) MERGE (a)-[:T]->(b) RETURN *") should equal(
      expected("MERGE (a:A)%nMERGE (b:B)%nMERGE (a)-[:T]->(b)%nRETURN *"))
  }

  test("UNWIND starts on a new line") {
    actual("WITH [1,2,2] AS coll UNWIND coll AS x RETURN collect(x)", preserveColumnNames = true) should equal(
      expected("WITH [1, 2, 2] AS coll%nUNWIND coll AS x%nRETURN collect(x)"))
  }

  //TODO
  //positive line-breaking tests with width
  test("WHERE in comprehensions starts on a new line and is indented for a given width") {
    actualInsideWidth("return [x in range(0,10) where x + 2 = 0 | x^3] as result", 26) should equal(
      expected(
        "RETURN [x IN range(0, 10)%n" +
          "  WHERE x + 2 = 0 | x ^ 3]%n" +
          "  AS result")
    )
  }

  //negative line-breaking tests
  test("no break on INDEX ON") {
    actual("create index on :Person(name)") should equal(
      expected("CREATE INDEX ON :Person(name)"))
  }

  test("no break on CONSTRAINT ON") {
    actual("create constraint on (person:Person) assert person.age is unique") should equal(
      expected("CREATE CONSTRAINT ON (person:Person) ASSERT person.age IS UNIQUE")
    )
  }

  test("no break on DESC") {
    actual("RETURN n order by n.name desc") should equal(
      expected("RETURN n%n  ORDER BY n.name DESC"))
  }

  test("no break on CREATE in FOREACH") {
    actual("match p=(n) foreach(x in p | create (x)--())") should equal(
      expected(
        "MATCH p = (n)%n" +
          "FOREACH (x IN p |%n" +
          "  CREATE (x)--()%n" +
          ")"))
  }

  test("no break on CREATE in complex FOREACH") {
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

  test("no break on STARTS WITH") {
    actual("return 'apartment' starts with 'apa' as x") should equal(
      expected("RETURN 'apartment' STARTS WITH 'apa' AS x")
    )
  }

  test("no break on ENDS WITH") {
    actual("return 'apartment' ends with 'apa' as x") should equal(
      expected("RETURN 'apartment' ENDS WITH 'apa' AS x")
    )
  }

  test("no break on CONTAINS") {
    actual("return 'apartment' contains 'apa' as x") should equal(
      expected("RETURN 'apartment' CONTAINS 'apa' AS x")
    )
  }

  test("no break on CREATE UNIQUE") {
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

  test("no break on WHERE in comprehensions") {
    actual("return [x in range(0,10) where x + 2 = 0 | x^3] as result") should equal(
      expected("RETURN [x IN range(0, 10) WHERE x + 2 = 0 | x ^ 3] AS result")
    )
  }

  test("no break after OPTIONAL") {
    actual("optional MATCH (n)-->(x) return n, x") should equal(
      expected("OPTIONAL MATCH (n)-->(x)%nRETURN n, x"))
  }

  test("no break after DETACH in DETACH DELETE") {
    actual("MATCH (n) DETACH DELETE (n)") should equal(
      expected("MATCH (n)%nDETACH DELETE (n)")
    )
  }

  //CATEGORY B: In contrast to CATEGORY A, these tests reformat the query in more 'drastic' ways.
  //The semantics remain unaltered (for these, see CATEGORY C), but the changes in this category
  //change constructs enough such that they ought not to be used to format users' queries in the browser.
  //These are therefore safe only to be used in the TCK and manual
  //An example to illustrate: assume we have query Q: "match (N) WITH n.age as SomeAge RETURN rand() as MyRand, SomeAge".
  //For the manual & TCK, Q' is perfectly legal: "MATCH (n) WITH n.age AS someAge RETURN rand() AS myRand, someAge"
  //For the browser, Q' is illegal. Instead, Q'' is ok: "MATCH (n) WITH n.age AS SomeAge RETURN rand() AS MyRand, SomeAge"
  //Thus, in the browser, we do NOT alter aliased variables

  //TODO add a test & change Pretty s.t. the query below displays properly wrt aliased cols
  test("start a variable name with a lower case character and show discrepancy in projected aliased names") {
    actual("match (N) with rand() as MyRand return N.age, MyRand", false, false) should equal(
      expected("MATCH (n)%nWITH rand() AS myRand%nRETURN n.age, myRand")
    )
  }

  //CATEGORY C: In addition to re-formatting the query, changes here also alter the semantics of the query.
  //Therefore, these actually pertain to the data modelling aspect and ought not to be taken lightly.
  //The AST is changed as a result of the semantic changes, and so we can't compare the 'before' and 'after' versions
  //This is only safe to be used in the manual and the TCK, where appropriate
  test("upper case relationship types") {
    actual("MATCH (m:Cat)-[:likes]->(n) RETURN m.name", false, false) should equal(
      expected("MATCH (m:Cat)-[:LIKES]->(n)%nRETURN m.name")
    )
  }

  test("upper case multiple relationship types") {
    actual("MATCH (m)-[:likes|loathes]->(n) RETURN m.name", false, false) should equal(
      expected("MATCH (m)-[:LIKES|LOATHES]->(n)%nRETURN m.name")
    )
  }

  test("labels start with an upper case character") {
    actual("match (n:editor) return n.name", false, false) should equal(
      expected("MATCH (n:Editor)%nRETURN n.name")
    )
  }

  test("labels in camel case instead of other delimiters") {
    actual("match (n:editor_in_chief) return n.name", false, false) should equal(
      expected("MATCH (n:EditorInChief)%nRETURN n.name")
    )
  }

  test("start a property key with a lower case character") {
    actual("match (n {Prop: 0}) return n.Prop", false, false) should equal(
      expected("MATCH (n {prop: 0})%nRETURN n.prop")
    )
  }

  test("start a parameter name with a lower case character") {
    actual("match (n {prop: $Param}) return count(n)", false, false) should equal(
      expected("MATCH (n {prop: $param})%nRETURN count(n)")
    )
  }

  test("no space within function call parentheses") {
    actual("RETURN split( 'original', 'i' )", false, false) should equal(
      expected("RETURN split('original', 'i')"))
  }

  //END

//  test("for debugging a tricky query") {
//    val pretty = new Pretty(preserveColumnNames = true) //changeSemanticOK = true / false
//    val parser = new CypherParser
//    val query = "match (n)"
//    val ast = parser.parse(query)
//    val reformatted = pretty.pretty(pretty.show(ast)).layout
//    val prettyDoc = pretty.show(ast)
//
//    val prettyString = pretty.pretty(prettyDoc, 500)
//    println("<-*******************")
//    println(prettyString.layout)
//    println("********************->")
//
//    //    val secondRound = parser.parse(reformatted)
//    //    ast should equal(secondRound)
//  }

  //Do for line breaks wrt keywords... and also for pattern breaking
//  test("experiment") {
//    val pretty = new Pretty(true)
//    val parser = new CypherParser
//    val ast = parser.parse("MATCH (m:cat)-[:likes]->(n) RETURN m.name")
//    val prettyDoc = pretty.show(ast)
//
//    (100 to 0 by -1) foreach { i =>
//      println("*" * i)
//      val prettyString = pretty.pretty(prettyDoc, i)
//      println(prettyString.layout)
//    }
//  }

  private val parser = new CypherParser

  private def actual(text: String, preserveColumnNames: Boolean = false, checkSemanticsUnchanged: Boolean = true): String = {
    val pretty = Pretty(preserveColumnNames)
    val ast = parser.parse(text)
    val reformatted = pretty.pretty(pretty.show(ast)).layout
//    println("++++++++")
//    println(text)
//    println("-------")
//    println(reformatted)

    if (checkSemanticsUnchanged)
      ast should equal(parser.parse(reformatted)) // This means that the pretty printed query, when parsed, returns the original AST

    reformatted
  }

  private def actualInsideWidth(text: String, width: Int): String = {
    val pretty = Pretty(preserveColumnNames = false)
    val ast = parser.parse(text)
    val reformatted = pretty.pretty(pretty.show(ast), width).layout
    //println(reformatted)
    val secondRound = parser.parse(reformatted)
    ast should equal(secondRound) // This means that the pretty printed query, when parsed, returns the original AST
    reformatted
  }

  private def expected(text: String) = String.format(text)
}