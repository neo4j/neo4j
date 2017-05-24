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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.PathImpl
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.kernel.impl.proc.Procedures

class PatternComprehensionAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("pattern comprehension nested in pattern comprehension") {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerFunction(classOf[TestFunction])
    val tag     = createLabeledNode("Tag")
    val content = createLabeledNode("Content")
    val user    = createLabeledNode(Map("name" -> "Michael Hunger"), "User")
    relate(content, tag, "TAGGED")
    relate(user, content, "CREATED")

    val query =
      """
        |MATCH (Tag:Tag)
        |RETURN
        |[ x IN test.nodeList() | x
        |  {
        |    tagged :
        |      [ (x)<-[:TAGGED]-(x_tagged:Content)  | x_tagged
        |        {
        |          owner :
        |            head(
        |              [ (x_tagged)<-[:CREATED]-(x_tagged_owner:User)  | x_tagged_owner
        |                {
        |                  .name
        |                }
        |              ]
        |            )
        |        }
        |      ]
        |  }
        | ][0..5] AS related
      """.stripMargin

    val result = executeWithCostPlannerOnly(query)
    result.toList should equal(
      List(Map("related" -> List(Map("tagged" -> Vector(Map("owner" -> Map("name" -> "Michael Hunger"))))))))
  }

  test("pattern comprehension nested in function call") {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerFunction(classOf[TestFunction])

    val n1 = createLabeledNode("Tweet")
    val n2 = createLabeledNode("User")
    relate(n2, n1, "POSTED")

    val query = """MATCH(t:Tweet) WITH t LIMIT 1
               |WITH collect(t) AS tweets
               |RETURN test.toSet([ tweet IN tweets | [ (tweet)<-[:POSTED]-(user) | user] ]) AS users""".stripMargin

    val result = executeWithCostPlannerOnly(query)
    result.toList should equal(List(Map("users" -> List(List(n2)))))
  }

  test("pattern comprehension outside function call") {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerFunction(classOf[TestFunction])

    val n1 = createLabeledNode("Tweet")
    val n2 = createLabeledNode("User")
    relate(n2, n1, "POSTED")

    val query = """MATCH(t:Tweet) WITH t LIMIT 1
                  |WITH collect(t) AS tweets
                  |WITH [ tweet IN tweets | [ (tweet)<-[:POSTED]-(user) | user] ] AS pattern
                  |RETURN test.toSet(pattern) AS users""".stripMargin

    val result = executeWithCostPlannerOnly(query)
    result.toList should equal(List(Map("users" -> List(List(n2)))))
  }

  test("with named path") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val n3 = createLabeledNode("NotEnd")
    val r = relate(n1, n2)
    relate(n2, n3)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() WHERE last(nodes(p)):End | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("miniscule WHERE clause") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val n3 = createLabeledNode("NotEnd")
    val r = relate(n1, n2)
    relate(n2, n3)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() where last(nodes(p)):End | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->(b) WHERE head([p IN ['foo'] | true ]) | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in projection") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | {path: p, other: [p IN ['foo'] | true ]} ] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(Map("path" -> PathImpl(n1, r, n2), "other" -> List(true))))))
  }

  test("one relationship out") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")
    val n2 = createLabeledNode(Map("x" -> 2), "START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)
    relate(n2, n4)
    relate(n2, n5)

    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-->(other) | other.x] as coll")

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(5, 4, 3)),
      Map("n.x" -> 2, "coll" -> Seq(5, 4))
    ))
    result should use("RollUpApply")
  }

  test("one relationship out with filtering") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")
    val n2 = createLabeledNode(Map("x" -> 2), "START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)
    val n6 = createNode("x" -> 6)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)
    relate(n1, n6)
    relate(n2, n4)
    relate(n2, n6)

    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-->(other) WHERE other.x % 2 = 0 | other.x] as coll")
    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(6, 4)),
      Map("n.x" -> 2, "coll" -> Seq(6, 4))
    ))
    result should use("RollUpApply")
  }

  test("find self relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")

    relate(n1, n1, "x"->"A")
    relate(n1, n1, "x"->"B")
    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-[r]->(n) | r.x] as coll")

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq("B", "A"))
    ))
    result should use("RollUpApply")
  }

  test("pattern comprehension built on a null yields null") {
    val result = executeWithCostPlannerOnly("optional match (n:MISSING) return [(n)-->(n) | n.x] as coll")
    result.toList should equal(List(
      Map("coll" -> null)
    ))
    result should use("RollUpApply")
  }

  test("pattern comprehension used in a WHERE query should work") {

    val a = createLabeledNode("START")
    val b = createLabeledNode("START")

    relate(a, createNode("x" -> 1))
    relate(a, createNode("x" -> 2))
    relate(a, createNode("x" -> 3))

    relate(b, createNode("x" -> 2))
    relate(b, createNode("x" -> 4))
    relate(b, createNode("x" -> 6))


    val result = executeWithCostPlannerOnly(
      """match (n:START)
        |where [(n)-->(other) | other.x] = [3,2,1]
        |return n""".stripMargin)

    result.toList should equal(List(
      Map("n" -> a)
    ))
    result should use("RollUpApply")
  }

  test("using pattern comprehension as grouping key") {
    val n1 = createLabeledNode("START")
    val n2 = createLabeledNode("START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)

    relate(n2, n3)
    relate(n2, n4)
    relate(n2, n5)

    val result = executeWithCostPlannerOnly("match (n:START) return count(*), [(n)-->(other) | other.x] as coll")
    result.toList should equal(List(
      Map("count(*)" -> 2, "coll" -> Seq(5, 4, 3))
    ))
    result should use("RollUpApply")
  }

  test("aggregating pattern comprehensions") {
    val n1 = createLabeledNode("START")
    val n2 = createLabeledNode("START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)
    val n6 = createNode("x" -> 6)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)

    relate(n2, n3)
    relate(n2, n4)
    relate(n2, n6)

    val result = executeWithCostPlannerOnly(
      """match (n:START)
        |return collect( [(n)-->(other) | other.x] ) as coll""".stripMargin)
    result.toList should equal(List(
      Map("coll" -> Seq(Seq(5, 4, 3), Seq(6, 4, 3)))
    ))
  }

  test("simple expansion using pattern comprehension") {
    val a = createNode("name" -> "Mats")
    val b = createNode("name" -> "Max")
    val c = createNode()
    relate(a, b)
    relate(b, c)
    relate(c, a)

    val query = "MATCH (a) RETURN [(a)-->() | a.name] AS list"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("list" -> List("Mats")), Map("list" -> List("Max")), Map("list" -> List(null))))
  }

  test("size on unbound pattern comprehension should work fine") {
    val a = createLabeledNode("Start")
    relate(a, createNode())
    relate(a, createNode())
    relate(createLabeledNode("Start"), createNode())

    val query = "RETURN size([(:Start)-->() | 1]) AS size"

    val result = executeWithCostPlannerOnly(query)
    result.toList should equal(List(Map("size" -> 3)))
  }

  test("pattern comprehension inside list comprehension") {
    val a = createLabeledNode("X", "A")
    val m1 = createLabeledNode("Y")
    relate(a, m1)
    relate(m1, createLabeledNode("Y"))
    relate(m1, createLabeledNode("Y"))

    val b = createLabeledNode("X", "B")
    val m2 = createNode()
    relate(b, m2)
    relate(m2, createLabeledNode("Y"))
    relate(m2, createNode())

    val query = """MATCH p = (n:X)-->() RETURN n, [x IN nodes(p) | size([(x)-->(y:Y) | 1])] AS list"""

    val result = executeWithCostPlannerOnly(query)
    result.toSet should equal(Set(
      Map("n" -> a, "list" -> Seq(1, 2)),
      Map("n" -> b, "list" -> Seq(0, 1))
    ))
  }

  test("pattern comprehension in RETURN following a WITH") {
    val query = """MATCH (e:X) WITH e LIMIT 5 RETURN [(e) --> (t) | t { .amount }]"""

    executeWithCostPlannerOnly(query).toList //does not throw
  }

  test("pattern comprehension play nice with map projections") {
    val movie = createLabeledNode(Map("title" -> "The Shining"), "Movie")
    val actor1 = createNode("name" -> "Actor1")
    val actor2 = createNode("name" -> "Actor2")
    relate(actor1, movie, "ACTED_IN")
    relate(actor2, movie, "ACTED_IN")
    val query = """match (m:Movie) return m { .title, cast: [(m)<-[:ACTED_IN]-(p) | p.name] }"""

    executeWithCostPlannerOnly(query).toList //does not throw
  }

  test("pattern comprehension play nice with OPTIONAL MATCH") {
    val p1 = createLabeledNode(Map("name" -> "Tom Cruise"), "Person")
    val p2 = createLabeledNode(Map("name" -> "Ron Howard"), "Person")
    val p3 = createLabeledNode(Map("name" -> "Keanu Reeves"), "Person")

    relate(p1, createLabeledNode(Map("title" -> "Cocktail"), "Movie"), "ACTED")
    relate(p2, createLabeledNode(Map("title" -> "Cocoon"), "Movie"), "DIRECTED")
    relate(p3, createLabeledNode(Map("title" -> "The Matrix"), "Movie"), "ACTED")

    val query =
      """match (a:Person)
        |where a.name in ['Ron Howard', 'Keanu Reeves', 'Tom Cruise']
        |optional match (a)-[:ACTED_IN]->(movie:Movie)
        |return a.name as name,  [(a)-[:DIRECTED]->(dirMovie:Movie) | dirMovie.title] as dirMovie""".stripMargin

    val result = executeWithCostPlannerOnly(query)
    result.toList should equal(List(
      Map("name" -> "Tom Cruise", "dirMovie" -> Seq()),
      Map("name" -> "Ron Howard", "dirMovie" -> Seq("Cocoon")),
      Map("name" -> "Keanu Reeves", "dirMovie" -> Seq())
    ))
  }

}
