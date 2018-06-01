/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.PathImpl
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.kernel.impl.proc.Procedures

class PatternComprehensionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  val expectedToSucceed = Configs.Interpreted - Configs.Version2_3
  val expectedToSucceedRestricted = expectedToSucceed - Configs.AllRulePlanners

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

    val result = executeWith(expectedToSucceedRestricted, query)
    result.toList should equal(
      List(Map("related" -> List(Map("tagged" -> Vector(Map("owner" -> Map("name" -> "Michael Hunger"))))))))
  }

  test("bug found when binding to already existing variables") {

    innerExecuteDeprecated(
      """create
        |(_0:`Decision`  {`id`:"d1"}),
        |(_1:`FilterValue`  {`value`:500}),
        |(_2:`FilterCharacteristic`  {`id`:"c1"}),
        |(_3:`Decision`  {`id`:"d2"}),
        |(_4:`FilterValue`  {`value`:1000}),
        |(_5:`Decision`  {`id`:"d3"}),
        |(_1)-[:`SET_ON`]->(_2),
        |(_1)-[:`SET_FOR`]->(_0),
        |(_4)-[:`SET_ON`]->(_2),
        |(_4)-[:`SET_FOR`]->(_3)""".stripMargin, Map())

    val query =
      """WITH {c1:[100,50000]} AS rangeFilters
        |MATCH (childD:Decision)
        | WHERE ALL(key IN keys(rangeFilters)
        |   WHERE size([(childD)<-[:SET_FOR]-(filterValue)-[:SET_ON]->(filterCharacteristic) WHERE filterCharacteristic.id = key  AND (rangeFilters[key])[0] <= filterValue.value <= (rangeFilters[key])[1] | 1]
        | ) > 0)
        | RETURN childD.id""".stripMargin

    val result = executeWith(expectedToSucceedRestricted, query)

    result.toList should equal(
      List(
        Map("childD.id" -> "d1"),
        Map("childD.id" -> "d2")))
  }

  test("pattern comprehension nested in function call") {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerFunction(classOf[TestFunction])

    val n1 = createLabeledNode("Tweet")
    val n2 = createLabeledNode("User")
    relate(n2, n1, "POSTED")

    val query = """MATCH(t:Tweet) WITH t LIMIT 1
               |WITH collect(t) AS tweets
               |RETURN test.toSet([ tweet IN tweets | [ (tweet)<-[:POSTED]-(user) | user] ]) AS users""".stripMargin

    val result = executeWith(expectedToSucceedRestricted, query)
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

    val result = executeWith(expectedToSucceedRestricted, query)
    result.toList should equal(List(Map("users" -> List(List(n2)))))
  }

  test("with named path") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | p] AS list"

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val n3 = createLabeledNode("NotEnd")
    val r = relate(n1, n2)
    relate(n2, n3)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() WHERE last(nodes(p)):End | p] AS list"

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("miniscule WHERE clause") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val n3 = createLabeledNode("NotEnd")
    val r = relate(n1, n2)
    relate(n2, n3)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() where last(nodes(p)):End | p] AS list"

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->(b) WHERE head([p IN ['foo'] | true ]) | p] AS list"

    val result = executeWith(expectedToSucceedRestricted, query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in projection") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | {path: p, other: [p IN ['foo'] | true ]} ] AS list"

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceedRestricted,
      "match (n:START) return n.x, [(n)-->(other) | other.x] as coll",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(5, 4, 3)),
      Map("n.x" -> 2, "coll" -> Seq(5, 4))
    ))
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

    val result = executeWith(expectedToSucceedRestricted,
      "match (n:START) return n.x, [(n)-->(other) WHERE other.x % 2 = 0 | other.x] as coll",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(6, 4)),
      Map("n.x" -> 2, "coll" -> Seq(6, 4))
    ))
  }

  test("find self relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")

    relate(n1, n1, "x"->"A")
    relate(n1, n1, "x"->"B")
    val result = executeWith(expectedToSucceedRestricted,
      "match (n:START) return n.x, [(n)-[r]->(n) | r.x] as coll",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq("B", "A"))
    ))
  }

  test("pattern comprehension built on a null yields null") {
    val result = executeWith(expectedToSucceed,
      "optional match (n:MISSING) return [(n)-->(n) | n.x] as coll",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(
      Map("coll" -> null)
    ))
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


    val result = executeWith(expectedToSucceedRestricted,
      """match (n:START)
        |where [(n)-->(other) | other.x] = [3,2,1]
        |return n""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(
      Map("n" -> a)
    ))
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

    val result = executeWith(expectedToSucceed,
      "match (n:START) return count(*), [(n)-->(other) | other.x] as coll",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("RollUpApply"), expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(
      Map("count(*)" -> 2, "coll" -> Seq(5, 4, 3))
    ))
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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("list" -> List("Mats")), Map("list" -> List("Max")), Map("list" -> List(null))))
  }

  test("size on unbound pattern comprehension should work fine") {
    val a = createLabeledNode("Start")
    relate(a, createNode())
    relate(a, createNode())
    relate(createLabeledNode("Start"), createNode())

    val query = "RETURN size([(:Start)-->() | 1]) AS size"

    val result = executeWith(expectedToSucceedRestricted, query)
    result.toList should equal(List(Map("size" -> 3)))
  }

  // FAIL: Ouch, no suitable slot for key   UNNAMED58 = -[0]-
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

    val query = """MATCH p = (n:X)-[s]->(apa) RETURN n, [x IN nodes(p) | size([(x)-[r]->(y:Y) | 1])] AS list"""

    val result = executeWith(expectedToSucceedRestricted, query)
    result.toSet should equal(Set(
      Map("n" -> a, "list" -> Seq(1, 2)),
      Map("n" -> b, "list" -> Seq(0, 1))
    ))
  }

  test("pattern comprehension in RETURN following a WITH") {
    val query = """MATCH (e:X) WITH e LIMIT 5 RETURN [(e) --> (t) | t { .amount }]"""

    executeWith(expectedToSucceedRestricted, query).toList //does not throw
  }

  test("pattern comprehension play nice with map projections") {
    val movie = createLabeledNode(Map("title" -> "The Shining"), "Movie")
    val actor1 = createNode("name" -> "Actor1")
    val actor2 = createNode("name" -> "Actor2")
    relate(actor1, movie, "ACTED_IN")
    relate(actor2, movie, "ACTED_IN")
    val query = """match (m:Movie) return m { .title, cast: [(m)<-[:ACTED_IN]-(p) | p.name] }"""

    executeWith(expectedToSucceedRestricted, query).toList //does not throw
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

    val result = executeWith(expectedToSucceedRestricted, query)
    result.toList should equal(List(
      Map("name" -> "Tom Cruise", "dirMovie" -> Seq()),
      Map("name" -> "Ron Howard", "dirMovie" -> Seq("Cocoon")),
      Map("name" -> "Keanu Reeves", "dirMovie" -> Seq())
    ))
  }

  test("pattern comprehension in unwind with empty db") {
    val query =
      """
        | unwind [(a)-->(b) | b ] as c
        | return c
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.Before3_3AndRule, query)
    result.toList should equal(List.empty)
  }

  test("pattern comprehension in unwind with hits") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2)
    relate(node2, node3)

    val query =
      """
        | unwind [(a)-->(b) | b ] as c
        | return c
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.Before3_3AndRule, query)
    result.toList should equal(List(Map("c" -> node2), Map("c" -> node3)))
  }
}
