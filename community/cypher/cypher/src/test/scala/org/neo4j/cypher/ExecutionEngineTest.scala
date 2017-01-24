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
package org.neo4j.cypher

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer.QueryEvent
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.NeoStoreDataSource
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction
import org.neo4j.logging.NullLogProvider
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.cypher.ExecutionEngineHelper.createEngine

import scala.collection.JavaConverters._
import scala.collection.mutable

class ExecutionEngineTest extends ExecutionEngineFunSuite
with QueryStatisticsTestSupport with CreateTempFileTestSupport with NewPlannerTestSupport {
  test("shouldGetRelationshipById") {
    val n = createNode()
    val r = relate(n, createNode(), "KNOWS")

    val result = executeWithAllPlannersAndCompatibilityMode("match ()-[r]->() where id(r) = 0 return r")

    result.columnAs[Relationship]("r").toList should equal(List(r))
  }

  test("shouldFilterOnGreaterThan") {
    val n = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("match(node) where 0<1 return node")

    result.columnAs[Node]("node").toList should equal(List(n))
  }

  test("shouldFilterOnRegexp") {
    val n1 = createNode(Map("name" -> "Andres"))
    val n2 = createNode(Map("name" -> "Jim"))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match(node) where node.name =~ 'And.*' return node"
    )
    result.columnAs[Node]("node").toList should equal(List(n1))
  }

  test("shouldGetOtherNode") {
    val node: Node = createNode()

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(s"match (node) where id(node) = ${node.getId} return node")

    result.columnAs[Node]("node").toList should equal(List(node))
  }

  test("shouldGetRelationship") {
    val node: Node = createNode()
    val rel: Relationship = relate(createNode(), node, "yo")

    val result = executeWithAllPlannersAndCompatibilityMode(s"match ()-[rel]->() where id(rel) = ${rel.getId} return rel")

    result.columnAs[Relationship]("rel").toList should equal(List(rel))
  }

  test("shouldGetTwoNodes") {
    val node1: Node = createNode()
    val node2: Node = createNode()

    val result = executeWithAllPlannersAndCompatibilityMode(s"match (node) where id(node) in [${node1.getId}, ${node2.getId}] return node")

    result.columnAs[Node]("node").toList should equal(List(node1, node2))
  }

  test("shouldGetNodeProperty") {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(s"match (node) where id(node) = ${node.getId} return node.name")

    result.columnAs[String]("node.name").toList should equal(List(name))
  }

  test("shouldOutputTheCartesianProductOfTwoNodes") {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      s"match (n1), (n2) where id(n1) = ${n1.getId} and id(n2) = ${n2.getId} return n1, n2"
    )

    result.toList should equal(List(Map("n1" -> n1, "n2" -> n2)))
  }

  test("executionResultTextualOutput") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      s"match (node)-[rel:KNOWS]->(x) where id(node) = ${n1.getId} return x, node"
    )
    result.dumpToString()
  }

  test("shouldFindNodesByExactIndexLookup") {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName($key = '$value') return n"

    graph.inTx {
      executeWithAllPlannersAndCompatibilityMode(query).toList should equal(List(Map("n" -> n)))
    }
  }

  test("shouldFindNodesByIndexQuery") {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key: $value') return n"

    graph.inTx {
      executeWithAllPlannersAndCompatibilityMode(query).toList should equal(List(Map("n" -> n)))
    }
  }

  test("shouldFindNodesByIndexParameters") {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    indexNode(n, idxName, key, "Andres")

    val query = s"start n=node:$idxName(key = {value}) return n"

    graph.inTx {
      executeWithAllPlannersAndCompatibilityMode(query, "value" -> "Andres").toList should equal(List(Map("n" -> n)))
    }
  }

  test("shouldFindNodesByIndexWildcardQuery") {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key:andr*') return n"

    graph.inTx {
      executeWithAllPlannersAndCompatibilityMode(query).toList should equal(List(Map("n" -> n)))
    }
  }

  test("shouldHandleOrFilters") {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match (n) where id(n) in [${n1.getId}, ${n2.getId}] and (n.name = 'boy' OR n.name = 'girl') return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2))
  }

  test("shouldHandleXorFilters") {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match (n) where id(n) in [${n1.getId}, ${n2.getId}] and (n.name = 'boy' XOR n.name = 'girl') return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2))
  }

  test("shouldHandleNestedAndOrFilters") {
    val n1 = createNode(Map("animal" -> "monkey", "food" -> "banana"))
    val n2 = createNode(Map("animal" -> "cow", "food" -> "grass"))
    val n3 = createNode(Map("animal" -> "cow", "food" -> "banana"))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match (n) where id(n) in [${n1.getId}, ${n2.getId}, ${n3.getId}] " +
        """and (
          (n.animal = 'monkey' AND n.food = 'banana') OR
          (n.animal = 'cow' AND n.food = 'grass')
        ) return n
        """
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2))
  }

  test("shouldBeAbleToOutputNullForMissingProperties") {
    createNode()
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n) where id(n) = 0 return n.name")
    result.toList should equal(List(Map("n.name" -> null)))
  }

  test("magicRelTypeOutput") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n)-[r]->(x) where id(n) = 0 return type(r)")

    result.columnAs[String]("type(r)").toList should equal(List("HATES", "KNOWS"))
  }

  test("shouldReturnPathLength") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = executeWithAllPlannersAndCompatibilityMode("match p = (n)-->(x) where id(n) = 0 return length(p)")

    result.columnAs[Int]("length(p)").toList should equal(List(1))
  }

  test("testZeroLengthVarLenPathInTheMiddle") {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")


    val result = executeWithAllPlannersAndCompatibilityMode("match (a)-[:CONTAINS*0..1]->(b)-[:FRIEND*0..1]->(c) where id(a) = 0 return a,b,c")

    result.toSet should equal(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))
      )
    )
  }

  test("shouldBeAbleToTakeParamsInDifferentTypes") {
    createNodes("A", "B", "C", "D", "E")

    val query =
      """
        |match (pA), (pB), (pC), (pD), (pE)
        |where id(pA) in {a} and id(pB) = {b} and id(pC) in {c} and id(pD) in {0} and id(pE) in {1}
        |return pA, pB, pC, pD, pE
      """.stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(query,
      "a" -> Seq[Long](0),
      "b" -> 1,
      "c" -> Seq(2L).asJava,
      "0" -> Seq(3).asJava,
      "1" -> List(4)
    )

    result.toList should have size 1
  }

  test("parameterTypeErrorShouldBeNicelyExplained") {
    createNode()
    val query = "match (pA) where id(pA) = {a} return pA"

    executeWithAllPlannersAndRuntimesAndCompatibilityMode(query, "a" -> "Andres") should be (empty)
  }

  test("shouldBeAbleToTakeParamsFromParsedStuff") {
    createNodes("A")

    val query = "match (pA) where id(pA) IN {a} return pA"
    val result = executeWithAllPlannersAndCompatibilityMode(query, "a" -> Seq[Long](0))

    result.toList should equal(List(Map("pA" -> node("A"))))
  }

  test("shouldBeAbleToTakeParamsForEqualityComparisons") {
    createNode(Map("name" -> "Andres"))

    val query = "match (a) where id(a) = 0 and a.name = {name} return a"

    executeWithAllPlannersAndRuntimesAndCompatibilityMode(query, "name" -> "Tobias").toList shouldBe empty
    executeWithAllPlannersAndRuntimesAndCompatibilityMode(query, "name" -> "Andres").toList should have size 1
  }

  test("shouldHandlePatternMatchingWithParameters") {
    val a = createNode()
    val b = createNode(Map("name" -> "you"))
    relate(a, b, "KNOW")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (x)-[r]-(friend) where x = {startId} and friend.name = {name} return TYPE(r)", "startId" -> a, "name" -> "you")

    result.toList should equal(List(Map("TYPE(r)" -> "KNOW")))
  }

  test("shouldComplainWhenMissingParams") {
    createNode()
    intercept[ParameterNotFoundException] {
      executeWithAllPlannersAndCompatibilityMode("match (pA) where id(pA) = {a} return pA").toList
    }
  }

  test("shouldSupportMultipleRegexes") {
    val a = createNode(Map("name" -> "Andreas"))

    val result = executeWithAllPlannersAndCompatibilityMode( """
match (a)
where id(a) = 0 AND a.name =~ 'And.*' AND a.name =~ 'And.*'
return a""")

    result.columnAs[Node]("a").toList should equal(List(a))
  }

  test("shouldReturnAnInterableWithAllRelationshipsFromAVarLength") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, c)

    val result = executeWithAllPlannersAndCompatibilityMode( """
match (a)-[r*2]->(c)
where id(a) = 0
return r""")

    result.toList should equal(List(Map("r" -> List(r1, r2))))
  }

  test("shouldHandleCheckingThatANodeDoesNotHaveAProp") {
    val a = createNode()

    val result = executeWithAllPlannersAndCompatibilityMode("match (a) where id(a) = 0 and not exists(a.propertyDoesntExist) return a")
    result.toList should equal(List(Map("a" -> a)))
  }

  test("shouldHandleAggregationAndSortingOnSomeOverlappingColumns") {
    createNode("COL1" -> "A", "COL2" -> "A", "num" -> 1)
    createNode("COL1" -> "B", "COL2" -> "B", "num" -> 2)

    val result = executeWithAllPlannersAndCompatibilityMode( """
match (a)
where id(a) IN [0, 1]
return a.COL1, a.COL2, avg(a.num)
order by a.COL1""")

    result.toList should equal(List(
      Map("a.COL1" -> "A", "a.COL2" -> "A", "avg(a.num)" -> 1),
      Map("a.COL1" -> "B", "a.COL2" -> "B", "avg(a.num)" -> 2)
    ))
  }

  test("shouldAllowAllPredicateOnArrayProperty") {
    val a = createNode("array" -> Array(1, 2, 3, 4))

    val result = executeWithAllPlannersAndCompatibilityMode("match (a) where id(a) = 0 and any(x in a.array where x = 2) return a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("shouldAllowStringComparisonsInArray") {
    val a = createNode("array" -> Array("Cypher duck", "Gremlin orange", "I like the snow"))

    val result = executeWithAllPlannersAndCompatibilityMode("match (a) where id(a) = 0 and single(x in a.array where x =~ '.*the.*') return a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("shouldBeAbleToCompareWithTrue") {
    val a = createNode("first" -> true)

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (a) where id(a) = 0 and a.first = true return a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("shouldToStringArraysPrettily") {
    createNode("foo" -> Array("one", "two"))

    val string = executeWithAllPlannersAndRuntimesAndCompatibilityMode( """match (n) where id(n) = 0 return n.foo""").dumpToString()

    string should include("""["one","two"]""")
  }

  test("shouldIgnoreNodesInParameters") {
    val x = createNode()
    val a = createNode()
    relate(x, a, "X")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (c) where id(c) = 0 match (n)--(c) return n")
    result should have size 1
  }

  test("shouldReturnDifferentResultsWithDifferentParams") {
    val refNode = createNode()
    val a = createNode()

    val b = createNode()
    relate(a, b)

    relate(refNode, a, "X")

    executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (a)-->(b) where a = {a} return b", "a" -> a) should have size 1
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (a)-->(b) where a = {a} return b", "a" -> b) shouldBe empty
  }

  test("should handle parameters names as variables") {
    createNode("bar" -> "Andres")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (foo) where id(foo) = 0 and foo.bar = {foo} return foo.bar", "foo" -> "Andres")
    result.toList should equal(List(Map("foo.bar" -> "Andres")))
  }

  test("shouldHandleComparisonsWithDifferentTypes") {
    createNode("belt" -> 13)

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 and (n.belt = 'white' OR n.belt = false) return n")
    result.toList shouldBe empty
  }

  test("start with node and relationship") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)
    val result = executeWithAllPlannersAndCompatibilityMode("match (a), ()-[r]->() where id(a) = 0 and id(r) = 0 return a,r")

    result.toList should equal(List(Map("a" -> a, "r" -> r)))
  }

  test("first piped query woot") {
    val a = createNode("foo" -> 42)
    createNode("foo" -> 49)

    val q = "match (x) where id(x) in [0,1] with x WHERE x.foo = 42 return x"
    val result = executeWithAllPlannersAndCompatibilityMode(q)

    result.toList should equal(List(Map("x" -> a)))
  }

  test("second piped query woot") {
    createNode()
    val q = "match (x) where id(x) = 0 with count(*) as apa WHERE apa = 1 RETURN apa"
    val result = executeWithAllPlannersAndCompatibilityMode(q)

    result.toList should equal(List(Map("apa" -> 1)))
  }

  test("shouldReturnASimplePath") {
    intercept[MissingIndexException](executeWithAllPlannersAndCompatibilityMode("start a=node:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](executeWithAllPlannersAndCompatibilityMode("start a=node:missingIndex('value') return a").toList)
    intercept[MissingIndexException](executeWithAllPlannersAndCompatibilityMode("start a=relationship:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](executeWithAllPlannersAndCompatibilityMode("start a=relationship:missingIndex('value') return a").toList)
  }

  test("createEngineWithSpecifiedParserVersion") {
    val config = Map[Setting[_], String](GraphDatabaseSettings.cypher_parser_version ->  "2.3")
    val db: GraphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase(config.asJava)
    val engine = createEngine(db)

    try {
      // This syntax is valid today, but should give an exception in 1.5
      engine.execute("create a", Map.empty[String, Any])
    } catch {
      case x: SyntaxException =>
      case _: Throwable => fail("expected exception")
    } finally {
      db.shutdown()
    }
  }

  test("issue 446") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "age" -> 24)
    relate(a, c, "age" -> 38)
    relate(a, d, "age" -> 12)

    val q = "match (n)-[f]->() where id(n)= 0 with n, max(f.age) as age match (n)-[f]->(m) where f.age = age return m"

    executeWithAllPlannersAndCompatibilityMode(q).toList should equal(List(Map("m" -> c)))
  }

  test("issue 432") {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val q = "match p = (n)-[*1..]->(m) where id(n)= 0 return p, last(nodes(p)) order by length(nodes(p)) asc"

    executeWithAllPlannersAndCompatibilityMode(q).toList should have size 1
  }

  test("zero matching subgraphs yield correct count star") {
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where 1 = 0 return count(*)")
    result.toList should equal(List(Map("count(*)" -> 0)))
  }

  test("with should not forget original type") {
    val result = updateWithBothPlannersAndCompatibilityMode("create (a{x:8}) with a.x as foo return sum(foo)")

    result.toList should equal(List(Map("sum(foo)" -> 8)))
  }

  test("with should not forget parameters") {
    graph.inTx(graph.index().forNodes("test"))
    val id = "bar"
    val result = updateWithBothPlannersAndCompatibilityMode("start n=node:test(name={id}) with count(*) as c where c=0 create (x{name:{id}}) return c, x.name as name", "id" -> id).toList

    result should have size 1
    result.head("c").asInstanceOf[Long] should equal(0)
    result.head("name").asInstanceOf[String] should equal(id)
  }

  test("with should not forget parameters2") {
    val id = createNode().getId
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) where id(n) = {id} with n set n.foo={id} return n", "id" -> id).toList

    result should have size 1
    graph.inTx {
      result.head("n").asInstanceOf[Node].getProperty("foo") should equal(id)
    }
  }

  test("shouldAllowArrayComparison") {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n) where id(n) = 0 and n.lotteryNumbers = [42, 87] return n")

    result.toList should equal(List(Map("n" -> node)))
  }

  test("shouldSupportArrayOfArrayOfPrimitivesAsParameterForInKeyword") {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 and n.lotteryNumbers in [[42, 87], [13], [42]] return n")

    result.toList should equal(List(Map("n" -> node)))
  }

  test("params should survive with") {
    val n = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll", "id"->1)

    result.toList should equal(List(Map("coll" -> List(n))))
  }

  test("nodes named r should not pose a problem") {
    val a = createNode()
    val r = createNode("foo"->"bar")
    val b = createNode()

    relate(a,r)
    relate(r,b)

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a)-->(r)-->(b) WHERE id(a) = 0 AND r.foo = 'bar' RETURN b")

    result.toList should equal(List(Map("b" -> b)))
  }

  test("can use variables created inside the foreach") {
    createNode()
    val result = updateWithBothPlanners("match (n) where id(n) = 0 foreach (x in [1,2,3] | create (a { name: 'foo'})  set a.id = x)")

    result.toList shouldBe empty
  }

  test("can alias and aggregate") {
    val a = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 return sum(ID(n)), n as m")

    result.toList should equal(List(Map("sum(ID(n))"->0, "m"->a)))
  }

  test("extract string from node collection") {
    createNode("name"->"a")

    val result = executeWithAllPlannersAndCompatibilityMode("""match (n) where id(n) = 0 with collect(n) as nodes return head(extract(x in nodes | x.name)) + "test" as test """)

    result.toList should equal(List(Map("test" -> "atest")))
  }

  test("filtering in match should not fail") {
    val n = createNode()
    relate(n, createNode("name" -> "Neo"))
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n)-->(me) WHERE id(n) = 0 AND me.name IN ['Neo'] RETURN me.name")

    result.toList should equal(List(Map("me.name"->"Neo")))
  }

  test("unexpected traversal state should never be hit") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n)-[r]->(m) WHERE n = {a} AND m = {b} RETURN *", "a"->a, "b"->c)

    result.toList shouldBe empty
  }

  test("syntax errors should not leave dangling transactions") {

    val engine = createEngine(graph)

    intercept[Throwable](engine.execute("BABY START SMILING, YOU KNOW THE SUN IS SHINING.", Map.empty[String, Any]))

    // Until we have a clean cut way where statement context is injected into cypher,
    // I don't know a non-hairy way to tell if this was done correctly, so here goes:
    val tx = graph.beginTransaction( Type.explicit, AnonymousContext.none() )
    val isTopLevelTx = tx.getClass === classOf[TopLevelTransaction]
    tx.close()

    isTopLevelTx shouldBe true
  }

  test("should add label to node") {
    val a = createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (a) where id(a) = 0 SET a :foo RETURN a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("should add multiple labels to node") {
    val a = createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (a) where id(a) = 0 SET a :foo:bar RETURN a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("should set label on node") {
    val a = createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (a) SET a:foo RETURN a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("should set multiple labels on node") {
    val a = createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (a) where id(a) = 0 SET a:foo:bar RETURN a")

    result.toList should equal(List(Map("a" -> a)))
  }

  test("should filter nodes by single label") {
    // GIVEN
    val a = createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n) WHERE id(n) in [0, 1, 2] AND n:foo RETURN n")

    // THEN
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }

  test("should filter nodes by single negated label") {
    // GIVEN
    createLabeledNode("foo")
    createLabeledNode("foo", "bar")
    val c = createNode()

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n) WHERE id(n) in [0, 1, 2] AND not(n:foo) RETURN n")

    // THEN
    result.toList should equal(List(Map("n" -> c)))
  }

  test("should filter nodes by multiple labels") {
    // GIVEN
    createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n) WHERE id(n) in [0, 1, 2] AND n:foo:bar RETURN n")

    // THEN
    result.toList should equal(List(Map("n" -> b)))
  }

  test("should create index") {
    // GIVEN
    val labelName = "Person"
    val propertyKeys = Seq("name")

    // WHEN
    updateWithBothPlannersAndCompatibilityMode(s"""CREATE INDEX ON :$labelName(${propertyKeys.reduce(_ ++ "," ++ _)})""")

    // THEN
    graph.inTx {
      val indexDefinitions = graph.schema().getIndexes(Label.label(labelName)).asScala.toSet
      indexDefinitions should have size 1

      val actual = indexDefinitions.head.getPropertyKeys.asScala.toIndexedSeq
      propertyKeys should equal(actual)
    }
  }

  test("union ftw") {
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 RETURN 1 as x UNION ALL match (n) where id(n) = 0 RETURN 2 as x")

    // THEN
    result.toList should equal(List(Map("x" -> 1), Map("x" -> 2)))
  }

  test("union distinct") {
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 RETURN 1 as x UNION match (n) where id(n) = 0 RETURN 1 as x")

    // THEN
    result.toList should equal(List(Map("x" -> 1)))
  }

  test("read only database can process has label predicates") {
    //GIVEN
    val engine = createReadOnlyEngine()

    //WHEN
    val result = engine.execute("MATCH (n) WHERE n:NonExistingLabel RETURN n", Map.empty[String, Any])

    //THEN
    result.toList shouldBe empty
  }

  test("should use predicates in the correct place") {
    val advertiser = createNode(Map("name" -> "advertiser1"))
    val thing = createNode(Map("name" -> "Color"))
    val red = createNode(Map("name" -> "red"))
    val p1 = createNode(Map("name" -> "product1"))
    val p4 = createNode(Map("name" -> "product4"))

    relate(advertiser, p1, "adv_has_product")
    relate(advertiser, p4, "adv_has_product")
    relate(thing, red, "aa_has_value")
    relate(p1, red, "ap_has_value")
    relate(p4, red, "ap_has_value")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("""
       MATCH (advertiser) -[:adv_has_product] ->(out) -[:ap_has_value] ->(red)<-[:aa_has_value]- (a)
       WHERE red.name = 'red' AND out.name = 'product1'
       AND id(advertiser) = {1} AND id(a) = {2}
       RETURN out.name""", "1" -> advertiser.getId, "2" -> thing.getId)

    //THEN
    result.toList should equal(List(Map("out.name" -> "product1")))
  }

  test("should not create when match exists") {
    //GIVEN
    val a = createNode()
    val b = createNode()
    relate(a,b,"FOO")

    //WHEN
    val result = updateWithBothPlannersAndCompatibilityMode(
      """MATCH (a), (b)
         WHERE id(a) = 0 AND id(b) = 1
         AND not (a)-[:FOO]->(b)
         CREATE (a)-[new:FOO]->(b)
         RETURN new""")

    //THEN
    result shouldBe empty
    result.queryStatistics().relationshipsCreated should equal(0)
  }

  test("test550") {
    createNode()

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      """MATCH (p) WHERE id(p) = 0
        WITH p
        MATCH (a) WHERE id(a) = 0
        MATCH (a)-->(b)
        RETURN *""")

    //THEN DOESN'T THROW EXCEPTION
    result.toList shouldBe empty
  }

  test("should be able to coalesce nodes") {
    val n = createNode("n")
    val m = createNode("m")
    relate(n,m,"link")
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where id(n) = 0 with coalesce(n,n) as n match (n)--() return n")

    result.toList should equal(List(Map("n" -> n)))
  }

  test("multiple start points should still honor predicates") {
    val e = createNode()
    val p1 = createNode("value"->567)
    val p2 = createNode("value"->0)
    relate(p1,e)
    relate(p2,e)

    indexNode(p1, "stuff", "key", "value")
    indexNode(p2, "stuff", "key", "value")

    val result = executeWithAllPlannersAndCompatibilityMode("start p1=node:stuff('key:*'), p2=node:stuff('key:*') match (p1)--(e), (p2)--(e) where p1.value = 0 and p2.value = 0 AND p1 <> p2 return p1,p2,e")
    result.toList shouldBe empty
  }

  test("should be able to prettify queries") {
    val query = "match (n)-->(x) return n"

    eengine.prettify(query) should equal(String.format("MATCH (n)-->(x)%nRETURN n"))
  }

  test("doctest gone wild") {
    // given
    updateWithBothPlannersAndCompatibilityMode("CREATE (n:Actor {name:'Tom Hanks'})")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("""MATCH (actor:Actor)
                               WHERE actor.name = "Tom Hanks"
                               CREATE (movie:Movie {title:'Sleepless in Seattle'})
                               CREATE (actor)-[:ACTED_IN]->(movie)""")

    // then
    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1, relationshipsCreated = 1)
  }

  test("should iterate all node id sets from start during matching") {
    // given
    val nodes: List[Node] =
      executeWithCostPlannerOnly("CREATE (a)-[:EDGE]->(b), (b)<-[:EDGE]-(c), (a)-[:EDGE]->(c) RETURN [a, b, c] AS nodes")
        .columnAs[List[Node]]("nodes").next().sortBy(_.getId)

    val nodeIds = s"[${nodes.map(_.getId).mkString(",")}]"

    // when
    val result = executeWithAllPlannersAndCompatibilityMode(s"MATCH (src)-[r:EDGE]-(dst) WHERE id(src) IN $nodeIds AND id(dst) IN $nodeIds RETURN r")

    // then
    val relationships: List[Relationship] = result.columnAs[Relationship]("r").toList

    relationships should have size 6
  }

  test("merge should support single parameter") {
    //WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (n:User {foo: {single_param}})", ("single_param", 42))

    //THEN DOESN'T THROW EXCEPTION
    result.toList shouldBe empty
  }

  test("merge should not support map parameters for defining properties") {
    intercept[SyntaxException](executeWithAllPlanners("MERGE (n:User {merge_map})", ("merge_map", Map("email" -> "test"))))
  }

  test("should not hang") {
    // given
    createNode()
    createNode()

    // when
    timeOutIn(2, TimeUnit.SECONDS) {
      executeWithAllPlannersAndCompatibilityMode(
        "MATCH (x)-->(a), (x)-->(b) " +
        "WHERE x.foo > 2 AND x.prop IN ['val'] " +
        "AND id(a) = 0 AND id(b) = 1 " +
        "RETURN x")
    }
    // then
  }

  test("should return null on all comparisons against null") {
    // given

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("return 1 > null as A, 1 < null as B, 1 <= null as C, 1 >= null as D, null <= null as E, null >= null as F")

    // then
    result.toList should equal(List(Map("A" -> null, "B" -> null, "C" -> null, "D" -> null, "E" -> null, "F" -> null)))
  }

  test("should be able to coerce collections to predicates") {
    val n = createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> true), "LABEL")
    createLabeledNode(Map("coll" -> Array[Int](), "bool" -> true), "LABEL")
    createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> false), "LABEL")
    createNode("coll" -> Array(1, 2, 3), "bool" -> true)
    createLabeledNode("LABEL")

    val foundNode = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n:LABEL) where n.coll and n.bool return n").columnAs[Node]("n").next()

    foundNode should equal(n)
  }

  test("should be able to coerce literal collections to predicates") {
    val n = createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> true), "LABEL")

    val foundNode = executeWithAllPlannersAndCompatibilityMode("match (n:LABEL) where [1,2,3] and n.bool return n").columnAs[Node]("n").next()

    foundNode should equal(n)
  }

  test("query should work") {
    assert(executeScalar[Int]("WITH 1 AS x RETURN 1 + x") === 2)
  }

  test("should be able to mix key expressions with aggregate expressions") {
    // Given
    createNode("Foo")

    // when
    val result = executeScalar[Map[String, Any]]("match (n) return { name: n.name, count: count(*) }")

    // then
    result("name") should equal("Foo")
    result("count") should equal(1)
  }

  test("should not mind rewriting NOT queries") {
    val result = updateWithBothPlannersAndCompatibilityMode(" create (a {x: 1}) return a.x is not null as A, a.y is null as B, a.x is not null as C, a.y is not null as D")
    result.toList should equal(List(Map("A" -> true, "B" -> true, "C" -> true, "D" -> false)))
  }

  test("should handle cypher version and periodic commit") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("1,2,3")
      writer.println("4,5,6")
    }
    val result = eengine.execute(s"cypher 2.3 using periodic commit load csv from '$url' as line create x return x", Map.empty[String, Any])
    result should have size 2
  }

  override def databaseConfig(): Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_min_replan_interval -> "0",
    GraphDatabaseSettings.cypher_compiler_tracing -> "true"
  )

  case class PlanningListener(planRequests: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty) extends TimingCompilationTracer.EventListener {
    override def queryCompiled(event: QueryEvent): Unit = {
      if(event.phases().asScala.exists(_.phase() == CompilationPhase.LOGICAL_PLANNING)) {
        planRequests.append(event.query())
      }
    }
  }

  test("should discard plans that are considerably unsuitable") {
    //GIVEN
    val planningListener = PlanningListener()
    kernelMonitors.addMonitorListener(planningListener)

    (0 until 100).foreach { _ => createLabeledNode("Person") }

    // WHEN
    eengine.execute(s"match (n:Person) return n", Map.empty[String, Any]).toList
    planningListener.planRequests should equal(Seq(
      s"match (n:Person) return n"
    ))
    (0 until 301).foreach { _ => createLabeledNode("Person") }
    eengine.execute(s"match (n:Person) return n", Map.empty[String, Any]).toList

    //THEN
    planningListener.planRequests should equal (Seq(
      s"match (n:Person) return n",
      s"match (n:Person) return n"
    ))
  }

  test("should avoid discarding plans that are still somewhat suitable") {
    //GIVEN
    val planningListener = PlanningListener()
    kernelMonitors.addMonitorListener(planningListener)

    (0 until 100).foreach { _ => createLabeledNode("Person") }
    //WHEN
    eengine.execute(s"match (n:Person) return n", Map.empty[String, Any]).toList
    planningListener.planRequests should equal(Seq(
      s"match (n:Person) return n"
    ))
    (0 until 9).foreach { _ => createLabeledNode("Dog") }
    eengine.execute(s"match (n:Person) return n", Map.empty[String, Any]).toList

    //THEN
    planningListener.planRequests should equal(Seq(
      s"match (n:Person) return n"
    ))
  }

  test("replanning should happen after data source restart") {
    val planningListener = PlanningListener()
    kernelMonitors.addMonitorListener(planningListener)

    val result1 = eengine.execute("match (n) return n", Map.empty[String, Any]).toList
    result1 shouldBe empty

    val ds = graph.getDependencyResolver.resolveDependency(classOf[NeoStoreDataSource])
    ds.stop()
    ds.start()

    val result2 = eengine.execute("match (n) return n", Map.empty[String, Any]).toList
    result2 shouldBe empty

    planningListener.planRequests should equal(Seq(
      s"match (n) return n",
      s"match (n) return n"
    ))
  }

  private def createReadOnlyEngine(): ExecutionEngine = {
    FileUtils.deleteRecursively(new File("target/readonly"))
    val old = new TestGraphDatabaseFactory().newEmbeddedDatabase( new File( "target/readonly" ) )
    old.shutdown()
    val db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( new File( "target/readonly" ) )
      .setConfig( GraphDatabaseSettings.read_only, "true" )
      .newGraphDatabase()
    createEngine(db)
  }
}
