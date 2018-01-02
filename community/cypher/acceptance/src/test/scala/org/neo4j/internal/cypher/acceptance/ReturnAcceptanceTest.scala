/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CustomMatchers
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}
import org.neo4j.graphdb._

import scala.util.Random

class ReturnAcceptanceTest extends ExecutionEngineFunSuite with CustomMatchers with NewPlannerTestSupport {

  test("should choke on an invalid unicode literal") {
    val query = "RETURN '\\uH' AS a"

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

  test("should accept a valid unicode literal") {
    val query = "RETURN '\\uf123' AS a"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("a" -> "\uF123")))
  }

  test("limit 0 should return an empty result") {
    createNode()
    createNode()
    createNode()
    val result = executeWithAllPlanners("match n return n limit 0")
    result should be(empty)
  }

  test("should not support sorting on things after distinct has removed it") {
    createNode("name" -> "A", "age" -> 13)
    createNode("name" -> "B", "age" -> 12)
    createNode("name" -> "C", "age" -> 11)

    intercept[SyntaxException](executeWithAllPlanners( """
match (a) where id(a) in [1,2,3,1]
return distinct a.name
order by a.age""").toList)

  }

  test("should not allow ordering on nodes") {
    createNode()
    createNode()

    intercept[SyntaxException](executeWithAllPlanners("match (n) where id(n) in [0,1] return n order by n").toList)
  }

  // EXCEPTION EXPECTED ABOVE THIS ROW

  test("expose problem with aliasing") {
    createNode("nisse")
    executeWithAllPlanners("match n return n.name, count(*) as foo order by n.name")
  }

  test("distinct on nullable values") {
    createNode("name" -> "Florescu")
    createNode()
    createNode()

    val result = executeWithAllPlanners("match a return distinct a.name").toList

    result should equal(List(Map("a.name" -> "Florescu"), Map("a.name" -> null)))
  }

  test("return all identifiers") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val r = relate(a, b)

    val result = executeWithAllPlanners("match p=(a:Start)-->b return *").toList
    val first = result.head
    first.keys should equal(Set("a", "b", "p"))

    first("p") should equal(PathImpl(a, r, b))
  }

  test("issue 508") {
    createNode()

    val q = "match n set n.x=[1,2,3] return length(n.x)"

    executeWithRulePlanner(q).toList should equal(List(Map("length(n.x)" -> 3)))
  }

  test("long or double") {
    val result = executeWithAllPlanners("return 1, 1.5").toList.head

    result("1") should haveType[java.lang.Long]
    result("1.5") should haveType[java.lang.Double]
  }

  test("square function returns decimals") {
    val result = executeWithAllPlanners("return sqrt(12.96)").toList

    result should equal(List(Map("sqrt(12.96)" -> 3.6)))
  }

  test("maths inside aggregation") {
    val andres = createNode("name" -> "Andres")
    val michael = createNode("name" -> "Michael")
    val peter = createNode("name" -> "Peter")
    val bread = createNode("type" -> "Bread")
    val veg = createNode("type" -> "Veggies")
    val meat = createNode("type" -> "Meat")

    relate(andres, bread, "ATE", Map("times" -> 10))
    relate(andres, veg, "ATE", Map("times" -> 8))

    relate(michael, veg, "ATE", Map("times" -> 4))
    relate(michael, bread, "ATE", Map("times" -> 6))
    relate(michael, meat, "ATE", Map("times" -> 9))

    relate(peter, veg, "ATE", Map("times" -> 7))
    relate(peter, bread, "ATE", Map("times" -> 7))
    relate(peter, meat, "ATE", Map("times" -> 4))

    executeWithAllPlanners(
      """
    match me-[r1:ATE]->food<-[r2:ATE]-you
    where id(me) = 1

    with me,count(distinct r1) as H1,count(distinct r2) as H2,you
    match me-[r1:ATE]->food<-[r2:ATE]-you

    return me,you,sum((1-ABS(r1.times/H1-r2.times/H2))*(r1.times+r2.times)/(H1+H2))""").dumpToString()
  }


  test("should not be confused by rewriting about what is a relationship and what not") {
    val andres = createNode("name" -> "Andres")
    val michael = createNode("name" -> "Michael")
    val peter = createNode("name" -> "Peter")
    val bread = createNode("type" -> "Bread")
    val veg = createNode("type" -> "Veggies")
    val meat = createNode("type" -> "Meat")

    relate(andres, bread, "ATE", Map("times" -> 10))
    relate(andres, veg, "ATE", Map("times" -> 8))

    relate(michael, veg, "ATE", Map("times" -> 4))
    relate(michael, bread, "ATE", Map("times" -> 6))
    relate(michael, meat, "ATE", Map("times" -> 9))

    relate(peter, veg, "ATE", Map("times" -> 7))
    relate(peter, bread, "ATE", Map("times" -> 7))
    relate(peter, meat, "ATE", Map("times" -> 4))

    executeWithAllPlanners(
      """
    match me-[r1]->you

    with 1 AS x
    match me-[r1]->food<-[r2]-you

    return r1.times""").dumpToString()
  }

  test("should return shortest paths if using a ridiculously unhip cypher") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val result = eengine.execute("match a, c where id(a) = 0 and id(c) = 2 return shortestPath(a-[*]->c)").columnAs[Path]("shortestPath(a-[*]->c)").toList.head
    result.endNode() should equal(c)
    result.startNode() should equal(a)
    result.length() should equal(2)
  }

  test("should return shortest path") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val c = createLabeledNode("End")
    relate(a, b)
    relate(b, c)

    val result = executeWithAllPlanners("match (a:Start), (c:End) return shortestPath(a-[*]->c)").columnAs[Path]("shortestPath(a-[*]->c)").toList.head
    result.endNode() should equal(c)
    result.startNode() should equal(a)
    result.length() should equal(2)
  }

  test("array prop output") {
    createNode("foo" -> Array(1, 2, 3))

    val result = executeWithAllPlanners("match n return n").dumpToString()

    result should include ("[1,2,3]")
  }

  test("map output") {
    val result = executeWithAllPlanners("return {a:1, b:'foo'}").dumpToString()

    result should ( include ("""{a -> 1, b -> "foo"}""") or include ("""{b -> "foo", a -> 1}""") )
  }

  test("should be able to return predicate result") {
    createNode()
    val result = executeWithAllPlanners("match a return id(a) = 0, a is null").toList
    result should equal(List(Map("id(a) = 0" -> true, "a is null" -> false)))
  }

  test("literal_collection") {
    val result = executeWithAllPlanners("return length([[],[]]+[[]]) as l").toList
    result should equal(List(Map("l" -> 3)))
  }

  test("array property should be accessible as collection") {
    createNode()
    val result = executeWithRulePlanner("match n SET n.array = [1,2,3,4,5] RETURN tail(tail(n.array))").
      toList.
      head("tail(tail(n.array))").
      asInstanceOf[Iterable[_]]

    result.toList should equal(List(3, 4, 5))
  }

  test("getting top x when we have less than x left") {
    val r = new Random(1337)
    val nodes = (0 to 15).map(x => createNode("count" -> x)).sortBy(x => r.nextInt(100))

    val result = executeWithAllPlanners("MATCH a RETURN a.count ORDER BY a.count SKIP 10 LIMIT 10", "nodes" -> nodes)

    result.toList should equal(List(
      Map("a.count" -> 10),
      Map("a.count" -> 11),
      Map("a.count" -> 12),
      Map("a.count" -> 13),
      Map("a.count" -> 14),
      Map("a.count" -> 15)
    ))
  }

  test("substring with default length") {
    val result = executeWithAllPlanners("return substring('0123456789', 1) as s")

    result.toList should equal(List(Map("s" -> "123456789")))
  }

  test("sort columns do not leak") {
    //GIVEN
    val result = executeWithAllPlanners("match n return * order by id(n)")

    //THEN
    result.columns should equal(List("n"))
  }

  test("should allow expression alias in order by with distinct") {
    createNode()

    //WHEN
    val result = executeWithAllPlanners(
      """MATCH (n)
        RETURN distinct ID(n) as id
        ORDER BY id DESC""")

    //THEN DOESN'T THROW EXCEPTION
    result.toList should equal(List(Map("id" -> 0)))
  }

  test("columns should not change when using order by and distinct") {
    val n = createNode()
    val result = executeWithAllPlanners("match n return distinct n order by id(n)")

    result.toList should equal(List(Map("n" -> n)))
  }

  test("allow queries with only return") {
    val result = executeWithAllPlanners("RETURN 'Andres'").toList

    result should equal(List(Map("'Andres'" -> "Andres")))
  }

  test("should allow distinct followed by order by") {
    // given a database with one node
    createNode()

    // then shouldn't throw
    executeWithAllPlanners("match x RETURN DISTINCT x as otherName ORDER BY x.name ")
  }

  test("should propagate null through math funcs") {
    val result = executeWithAllPlanners("return 1 + (2 - (3 * (4 / (5 ^ (6 % null))))) as A")
    result.toList should equal(List(Map("A" -> null)))
  }

  test("should be able to index into nested literal lists") {
    executeWithAllPlanners("RETURN [[1]][0][0]").toList
    // should not throw an exception
  }

  test("should be able to alias expressions") {
    createNode("id" -> 42)
    val result = executeWithAllPlanners("match (a) return a.id as a, a.id")
    result.toList should equal(List(Map("a" -> 42, "a.id" -> 42)))
  }

  test("should not get into a neverending loop") {
    val n = createNode("id" -> 42)
    val result = executeWithAllPlanners("MATCH n RETURN n, count(n) + 3")
    result.toList should equal(List(Map("n" -> n, "count(n) + 3" -> 4)))
  }

  test("renaming in multiple steps should still work") {
    val result = executeWithRulePlanner(
      """CREATE (m)
        |WITH {FIRST: id(m)} AS m
        |WITH {SECOND: m.FIRST} AS m
        |RETURN m.SECOND""".stripMargin)
      .columnAs[Any]("m.SECOND")
      .toList

    result shouldNot contain(null)
  }

  test("aggregating by an array property has a correct definition of equality") {
    executeWithRulePlanner(
      """    create
        |    (_0  {a:[1,2,3]}),
        |    (_1  {a:[1,2,3]})
      """.stripMargin
    )

    val result = executeWithAllPlanners("MATCH (a) WITH a.a AS a, count(*) AS count RETURN count")
    result.toList should equal(List(Map("count" -> 2)))
  }

  test("reusing identifier names should not be problematic") {
    val result = executeWithAllPlanners(
      """MATCH (person:Person       )<-                               -(message)<-[like]-(liker:Person)
        |WITH                 like.creationDate AS likeTime, person AS person
        |ORDER BY likeTime         , message.id
        |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person AS person
        |RETURN latestLike.likeTime AS likeTime
        |ORDER BY likeTime
        | """.stripMargin, "1" -> 42, "2" -> 10
    )

    result shouldBe empty
  }

  test("compiled runtime should support literal expressions") {
    val result = executeWithAllPlanners("RETURN 1")

    result.toList should equal(List(Map("1" -> 1)))
  }

  test("compiled runtime should support addition of collections") {
    val result = executeWithAllPlanners("RETURN [1,2,3] + [4, 5] AS FOO")

    result.toComparableResult should equal(List(Map("FOO" -> List(1, 2, 3, 4, 5))))
  }

  test("compiled runtime should support addition of item to collection") {
    val result = executeWithAllPlanners("""RETURN [1,2,3] + 4 AS FOO""")

    result.toComparableResult should equal(List(Map("FOO" -> List(1, 2, 3, 4))))
  }

  test("Should return correct scala objects") {
    val query = "RETURN {uid: 'foo'} AS params"

    val rows = executeWithAllPlanners(query).columnAs[Map[String, Any]]("params").toList

    rows.head should equal(Map("uid" -> "foo"))
  }

  test("distinct inside aggregation should work with collections inside maps") {
    val propertyCollection = Array("A", "B")
    createNode("array" -> propertyCollection)
    createNode("array" -> propertyCollection)

    val result = executeWithAllPlanners("MATCH (n) RETURN count(distinct {foo: n.array}) as count")

    result.toList should equal(List(Map("count" -> 1)))
  }

  test("distinct should work with collections inside maps") {
    val propertyCollection = Array("A", "B")
    createNode("array" -> propertyCollection)
    createNode("array" -> propertyCollection)

    val result = executeWithAllPlanners("MATCH (n) WITH distinct {foo: n.array} as map RETURN count(*)")

    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("distinct inside aggregation should work with nested collections inside map") {
    val propertyCollection = Array("A", "B")
    createNode("array" -> propertyCollection)
    createNode("array" -> propertyCollection)

    val result = executeWithAllPlanners("MATCH (n) RETURN count(distinct {foo: [[n.array, n.array], [n.array, n.array]]}) as count")

    result.toList should equal(List(Map("count" -> 1)))
  }

  test("distinct inside aggregation should work with nested collections of maps inside map") {
    val propertyCollection = Array("A", "B")
    createNode("array" -> propertyCollection)
    createNode("array" -> propertyCollection)

    val result = executeWithAllPlanners("MATCH (n) RETURN count(distinct {foo: [{bar: n.array}, {baz: {apa: n.array}}]}) as count")

    result.toList should equal(List(Map("count" -> 1)))
  }

  test("returning * and additional aliased columns should not give duplicate returned columns") {
    val result = executeWithAllPlanners("WITH 1337 as foo RETURN *, 42 as bar ORDER BY bar")

    result.toList should equal(List(Map("foo" -> 1337, "bar" -> 42)))
  }

  test("returning * and additional unaliased columns should not give duplicate returned columns") {
    val result = executeWithAllPlanners("WITH 1337 as foo RETURN *, 42 ORDER BY foo")

    result.toList should equal(List(Map("foo" -> 1337, "42" -> 42)))
  }

  test("returning * and additional unaliased columns should not give duplicate returned columns 2") {
    val n = createNode(Map("foo" -> 42))

    executeWithAllPlanners("MATCH (n) RETURN *, n.foo ORDER BY n.foo SKIP 0 LIMIT 5 ").toList should equal(List(Map("n"-> n, "n.foo" -> 42)))
  }

  test("accessing a list with null should return null") {
    executeWithAllPlanners("RETURN [1,2,3][null] AS result").toList should equal(List(Map("result" -> null)))
  }

  test("accessing a list with null as lower bound should return null") {
    executeWithAllPlanners("RETURN [1,2,3][null..5] AS result").toList should equal(List(Map("result" -> null)))
  }

  test("accessing a list with null as upper bound should return null") {
    executeWithAllPlanners("RETURN [1,2,3][1..null] AS result").toList should equal(List(Map("result" -> null)))
  }

  test("accessing a map with null should return null") {
    executeWithAllPlanners("RETURN {key: 1337}[null] AS result").toList should equal(List(Map("result" -> null)))
  }
}
