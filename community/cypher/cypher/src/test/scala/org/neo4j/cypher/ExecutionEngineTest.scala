/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.hamcrest.CoreMatchers._
import org.neo4j.graphdb._
import org.neo4j.kernel.TopLevelTransaction
import org.neo4j.test.ImpermanentGraphDatabase
import org.junit.Assert._
import scala.collection.JavaConverters._
import org.junit.Test
import java.util.concurrent.TimeUnit
import org.neo4j.cypher.internal.PathImpl
import org.neo4j.graphdb.factory.{GraphDatabaseSettings, GraphDatabaseFactory}

class ExecutionEngineTest extends ExecutionEngineJUnitSuite with QueryStatisticsTestSupport {


  @Test def shouldGetRelationshipById() {
    val n = createNode()
    val r = relate(n, createNode(), "KNOWS")

    val result = execute("start r=rel(0) return r")
    assertEquals(List(r), result.columnAs[Relationship]("r").toList)
  }

  @Test def shouldFilterOnGreaterThan() {
    val n = createNode()
    val result = execute("start node=node(0) where 0<1 return node")

    assertEquals(List(n), result.columnAs[Node]("node").toList)
  }

  @Test def shouldFilterOnRegexp() {
    val n1 = createNode(Map("name" -> "Andres"))
    val n2 = createNode(Map("name" -> "Jim"))

    val result = execute(
      s"start node=node(${n1.getId}, ${n2.getId}) where node.name =~ 'And.*' return node"
    )
    assertEquals(List(n1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetOtherNode() {
    val node: Node = createNode()

    val result = execute(s"start node=node(${node.getId}) return node")
    assertEquals(List(node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetRelationship() {
    val node: Node = createNode()
    val rel: Relationship = relate(createNode(), node, "yo")

    val result = execute(s"start rel=rel(${rel.getId}) return rel")
    assertEquals(List(rel), result.columnAs[Relationship]("rel").toList)
  }

  @Test def shouldGetTwoNodes() {
    val node1: Node = createNode()
    val node2: Node = createNode()

    val result = execute(s"start node=node(${node1.getId}, ${node2.getId}) return node")
    assertEquals(List(node1, node2), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetNodeProperty() {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val result = execute(s"start node=node(${node.getId}) return node.name")
    val list = result.columnAs[String]("node.name").toList
    assertEquals(List(name), list)
  }

  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val result = execute(
      s"start n1=node(${n1.getId}), n2=node(${n2.getId}) return n1, n2"
    )

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def executionResultTextualOutput() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = execute(
      s"start node=node(${n1.getId}) match (node)-[rel:KNOWS]->(x) return x, node"
    )
    result.dumpToString()
  }

  @Test def shouldFindNodesByExactIndexLookup() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName($key = '$value') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldFindNodesByIndexQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key: $value') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldFindNodesByIndexParameters() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    indexNode(n, idxName, key, "Andres")

    val query = s"start n=node:$idxName(key = {value}) return n"

    assertInTx(List(Map("n" -> n)) === execute(query, "value" -> "Andres").toList)
  }

  @Test def shouldFindNodesByIndexWildcardQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key:andr*') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldHandleOrFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}) where n.name = 'boy' OR n.name = 'girl' return n"
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldHandleXorFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}) where n.name = 'boy' XOR n.name = 'girl' return n"
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldHandleNestedAndOrFilters() {
    val n1 = createNode(Map("animal" -> "monkey", "food" -> "banana"))
    val n2 = createNode(Map("animal" -> "cow", "food" -> "grass"))
    val n3 = createNode(Map("animal" -> "cow", "food" -> "banana"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}) " +
        """where
        (n.animal = 'monkey' AND n.food = 'banana') OR
        (n.animal = 'cow' AND n.food = 'grass')
        return n
        """
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToOutputNullForMissingProperties() {
    createNode()
    val result = execute("start n=node(0) return n.name")
    assertEquals(List(Map("n.name" -> null)), result.toList)
  }

  @Test def magicRelTypeOutput() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute("start n = node(0) match n-[r]->x return type(r)")

    assertEquals(List("KNOWS", "HATES"), result.columnAs[String]("type(r)").toList)
  }

  @Test def shouldReturnPathLength() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start n = node(0) match p = n-->x return length(p)")

    assertEquals(List(1), result.columnAs[Int]("length(p)").toList)
  }

  @Test def testZeroLengthVarLenPathInTheMiddle() {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")


    val result = execute("start a=node(0) match a-[:CONTAINS*0..1]->b-[:FRIEND*0..1]->c return a,b,c")

    assertEquals(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))),
      result.toSet)
  }

  @Test def shouldBeAbleToTakeParamsInDifferentTypes() {
    createNodes("A", "B", "C", "D", "E")

    val query =
      """
        |start pA=node({a}), pB=node({b}), pC=node({c}), pD=node({0}), pE=node({1})
        |return pA, pB, pC, pD, pE
      """.stripMargin

    val result = execute(query,
      "a" -> Seq[Long](0),
      "b" -> 1,
      "c" -> Seq(2L).asJava,
      "0" -> Seq(3).asJava,
      "1" -> List(4)
    )

    assertEquals(1, result.toList.size)
  }

  @Test(expected = classOf[CypherTypeException]) def parameterTypeErrorShouldBeNicelyExplained() {
    createNodes("A")

    val query = "start pA=node({a}) return pA"
    execute(query, "a" -> "Andres").toList
  }

  @Test def shouldBeAbleToTakeParamsFromParsedStuff() {
    createNodes("A")

    val query = "start pA = node({a}) return pA"
    val result = execute(query, "a" -> Seq[Long](0))

    assertEquals(List(Map("pA" -> node("A"))), result.toList)
  }

  @Test def shouldBeAbleToTakeParamsForEqualityComparisons() {
    createNode(Map("name" -> "Andres"))

    val query = "start a=node(0) where a.name = {name} return a"

    assert(0 === execute(query, "name" -> "Tobias").toList.size)
    assert(1 === execute(query, "name" -> "Andres").toList.size)
  }

  @Test def shouldHandlePatternMatchingWithParameters() {
    val a = createNode()
    val b = createNode(Map("name" -> "you"))
    relate(a, b, "KNOW")

    val result = execute("start x  = node({startId}) match x-[r]-friend where friend.name = {name} return TYPE(r)", "startId" -> a, "name" -> "you")

    assert(List(Map("TYPE(r)" -> "KNOW")) === result.toList)
  }

  @Test(expected = classOf[ParameterNotFoundException]) def shouldComplainWhenMissingParams() {
    execute("start pA=node({a}) return pA").toList
  }

  @Test def shouldSupportMultipleRegexes() {
    val a = createNode(Map("name" -> "Andreas"))

    val result = execute( """
start a  = node(0)
where a.name =~ 'And.*' AND a.name =~ 'And.*'
return a""")

    assert(List(a) === result.columnAs[Node]("a").toList)
  }



  @Test def shouldReturnAnInterableWithAllRelationshipsFromAVarLength() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, c)

    val result = execute( """
start a  = node(0)
match a-[r*2]->c
return r""")

    assert(List(Map("r" -> List(r1, r2))) === result.toList)
  }

  @Test def shouldHandleCheckingThatANodeDoesNotHaveAProp() {
    val a = createNode()

    val result = execute("start a=node(0) where not has(a.propertyDoesntExist) return a")
    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldHandleAggregationAndSortingOnSomeOverlappingColumns() {
    createNode("COL1" -> "A", "COL2" -> "A", "num" -> 1)
    createNode("COL1" -> "B", "COL2" -> "B", "num" -> 2)

    val result = execute( """
start a  = node(0,1)
return a.COL1, a.COL2, avg(a.num)
order by a.COL1""")

    assert(List(
      Map("a.COL1" -> "A", "a.COL2" -> "A", "avg(a.num)" -> 1),
      Map("a.COL1" -> "B", "a.COL2" -> "B", "avg(a.num)" -> 2)
    ) === result.toList)
  }

  @Test def shouldAllowAllPredicateOnArrayProperty() {
    val a = createNode("array" -> Array(1, 2, 3, 4))

    val result = execute("start a = node(0) where any(x in a.array where x = 2) return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldAllowStringComparisonsInArray() {
    val a = createNode("array" -> Array("Cypher duck", "Gremlin orange", "I like the snow"))

    val result = execute("start a = node(0) where single(x in a.array where x =~ '.*the.*') return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldBeAbleToCompareWithTrue() {
    val a = createNode("first" -> true)

    val result = execute("start a = node(0) where a.first = true return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldToStringArraysPrettily() {
    createNode("foo" -> Array("one", "two"))

    val result = executeLazy( """start n = node(0) return n.foo""")


    val string = result.dumpToString()

    assertThat(string, containsString( """["one","two"]"""))
  }



  @Test def shouldIgnoreNodesInParameters() {
    val x = createNode()
    val a = createNode()
    relate(x, a, "X")

    val result = execute("start c = node(0) match (n)--(c) return n")
    assert(1 === result.size)
  }

  @Test def shouldReturnDifferentResultsWithDifferentParams() {
    val refNode = createNode()
    val a = createNode()

    val b = createNode()
    relate(a, b)

    relate(refNode, a, "X")

    assert(1 === execute("start a = node({a}) match a-->b return b", "a" -> a).size)
    assert(0 === execute("start a = node({a}) match a-->b return b", "a" -> b).size)
  }

  @Test def shouldHandleParametersNamedAsIdentifiers() {
    createNode("bar" -> "Andres")

    val result = execute("start foo=node(0) where foo.bar = {foo} return foo.bar", "foo" -> "Andres")
    assert(List(Map("foo.bar" -> "Andres")) === result.toList)
  }

  @Test def shouldHandleRelationshipIndexQuery() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)
    indexRel(r, "relIdx", "key", "value")

    assertInTx(List(Map("r" -> r)) === execute("start r=relationship:relIdx(key='value') return r").toList)
  }

  @Test def shouldHandleComparisonsWithDifferentTypes() {
    createNode("belt" -> 13)

    val result = execute("start n = node(0) where n.belt = 'white' OR n.belt = false return n")
    assert(List() === result.toList)
  }

  @Test def start_with_node_and_relationship() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)
    val result = execute("start a=node(0), r=relationship(0) return a,r").toList

    assert(List(Map("a" -> a, "r" -> r)) === result)
  }

  @Test def first_piped_query_woot() {
    val a = createNode("foo" -> 42)
    createNode("foo" -> 49)

    val q = "start x=node(0, 1) with x WHERE x.foo = 42 return x"
    val result = execute(q)

    assert(List(Map("x" -> a)) === result.toList)
  }

  @Test def second_piped_query_woot() {
    createNode()
    val q = "start x=node(0) with count(*) as apa WHERE apa = 1 RETURN apa"
    val result = execute(q)

    assert(List(Map("apa" -> 1)) === result.toList)
  }

  @Test def shouldReturnASimplePath() {
    intercept[MissingIndexException](execute("start a=node:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](execute("start a=node:missingIndex('value') return a").toList)
    intercept[MissingIndexException](execute("start a=relationship:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](execute("start a=relationship:missingIndex('value') return a").toList)
  }

  @Test def createEngineWithSpecifiedParserVersion() {
    val db = new ImpermanentGraphDatabase(Map[String, String]("cypher_parser_version" -> "1.9").asJava)
    val engine = new ExecutionEngine(db)

    try {
      // This syntax is valid today, but should give an exception in 1.5
      engine.execute("create a")
    } catch {
      case x: SyntaxException =>
      case _: Throwable => fail("expected exception")
    } finally {
      db.shutdown()
    }
  }


  @Test def issue_446() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "age" -> 24)
    relate(a, c, "age" -> 38)
    relate(a, d, "age" -> 12)

    val q = "start n = node(0) match n-[f]->() with n, max(f.age) as age match n-[f]->m where f.age = age return m"

    assert(execute(q).toList === List(Map("m" -> c)))
  }

  @Test def issue_432() {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val q = "start n = node(0) match p = n-[*1..]->m return p, last(nodes(p)) order by length(nodes(p)) asc"

    assert(execute(q).size === 1)
  }

  @Test def zero_matching_subgraphs_yield_correct_count_star() {
    val result = execute("start n=node(*) where 1 = 0 return count(*)").toList
    assert(List(Map("count(*)" -> 0)) === result)
  }

  @Test def should_return_paths_in_1_9() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "X")
    relate(a, c, "X")
    relate(a, d, "X")

    val result = execute("cypher 1.9 start n = node(0) return n-->()").columnAs[List[Path]]("n-->()").toList.flatMap(p => p.map(_.endNode()))

    assert(result === List(b, c, d))
  }

  @Test
  def var_length_expression_on_1_9() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val resultPath = execute("CYPHER 1.9 START a=node(0), b=node(1) RETURN a-[*]->b as path")
      .toList.head("path").asInstanceOf[List[Path]].head

    assert(resultPath.startNode() === a)
    assert(resultPath.endNode() === b)
    assert(resultPath.lastRelationship() === r)
  }


  @Test
  def optional_expression_used_to_be_supported() {
    graph.inTx {
      val a = createNode()
      val b = createNode()
      val r = relate(a, b)

      val result = execute("CYPHER 1.9 start a=node(0) match a-->b RETURN a-[?]->b").toList
      assert(result === List(Map("a-[?]->b" -> List(PathImpl(a, r, b)))))
    }
  }

  @Test
  def pattern_expression_deep_in_function_call_in_1_9() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a,b)
    relate(a,c)

    graph.inTx {
      execute("CYPHER 1.9 start a=node(0) foreach(n in extract(p in a-->() | last(p)) | set n.touched = true) return a-->()").size
    }
  }

  @Test
  def with_should_not_forget_original_type() {
    val result = execute("create (a{x:8}) with a.x as foo return sum(foo)")

    assert(result.toList === List(Map("sum(foo)" -> 8)))
  }

  @Test
  def with_should_not_forget_parameters() {
    graph.inTx(graph.index().forNodes("test"))
    val id = "bar"
    val result = execute("start n=node:test(name={id}) with count(*) as c where c=0 create (x{name:{id}}) return c, x", "id" -> id).toList

    assert(result.size === 1)
    assert(result(0)("c").asInstanceOf[Long] === 0)
    assertInTx(result(0)("x").asInstanceOf[Node].getProperty("name") === id)
  }

  @Test
  def with_should_not_forget_parameters2() {
    val a = createNode()
    val id = a.getId
    val result = execute("start n=node({id}) with n set n.foo={id} return n", "id" -> id).toList

    assert(result.size === 1)
    assertInTx(result(0)("n").asInstanceOf[Node].getProperty("foo") === id)
  }

  @Test
  def shouldAllowArrayComparison() {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = execute("start n = node(0) where n.lotteryNumbers = [42, 87] return n")

    assert(result.toList === List(Map("n" -> node)))
  }

  @Test
  def shouldSupportArrayOfArrayOfPrimitivesAsParameterForInKeyword() {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = execute("start n = node(0) where n.lotteryNumbers in [[42, 87], [13], [42]] return n")

    assert(result.toList === List(Map("n" -> node)))
  }

  @Test
  def params_should_survive_with() {
    val n = createNode()
    val result = execute("START n=node(0) WITH collect(n) as coll where length(coll)={id} RETURN coll", "id"->1)

    assert(result.toList === List(Map("coll" -> List(n))))
  }

  @Test
  def nodes_named_r_should_not_pose_a_problem() {
    val a = createNode()
    val r = createNode("foo"->"bar")
    val b = createNode()

    relate(a,r)
    relate(r,b)

    val result = execute("START a=node(0) MATCH a-->r-->b WHERE r.foo = 'bar' RETURN b")

    assert(result.toList === List(Map("b" -> b)))
  }

  @Test
  def can_use_identifiers_created_inside_the_foreach() {
    createNode()
    val result = execute("start n=node(0) foreach (x in [1,2,3] | create (a { name: 'foo'})  set a.id = x)")

    assert(result.toList === List())
  }

  @Test
  def can_alias_and_aggregate() {
    val a = createNode()
    val result = execute("start n = node(0) return sum(ID(n)), n as m")

    assert(result.toList === List(Map("sum(ID(n))"->0, "m"->a)))
  }

  @Test
  def extract_string_from_node_collection() {
    createNode("name"->"a")

    val result = execute("""START n = node(0) with collect(n) as nodes return head(extract(x in nodes | x.name)) + "test" as test """)

    assert(result.toList === List(Map("test" -> "atest")))
  }

  @Test
  def filtering_in_match_should_not_fail() {
    val n = createNode()
    relate(n, createNode("name" -> "Neo"))
    val result = execute("START n = node(0) MATCH n-->me WHERE me.name IN ['Neo'] RETURN me.name")

    assert(result.toList === List(Map("me.name"->"Neo")))
  }

  @Test
  def unexpected_traversal_state_should_never_be_hit() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = execute("START n=node({a}), m=node({b}) MATCH n-[r]->m RETURN *", "a"->a, "b"->c)

    assert(result.toList === List())
  }

  @Test def syntax_errors_should_not_leave_dangling_transactions() {

    val engine = new ExecutionEngine(graph)

    intercept[Throwable](engine.execute("BABY START SMILING, YOU KNOW THE SUN IS SHINING."))

    // Until we have a clean cut way where statement context is injected into cypher,
    // I don't know a non-hairy way to tell if this was done correctly, so here goes:
    val tx  = graph.beginTx()
    val isTopLevelTx = tx.getClass === classOf[TopLevelTransaction]
    tx.close()

    assert(isTopLevelTx)
  }

  @Test def should_add_label_to_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a :foo RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_add_multiple_labels_to_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a :foo:bar RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_set_label_on_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a:foo RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_set_multiple_labels_on_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a:foo:bar RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_filter_nodes_by_single_label() {
    // GIVEN
    val a = createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE n:foo RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> a), Map("n" -> b)))
  }

  @Test def should_filter_nodes_by_single_negated_label() {
    // GIVEN
    createLabeledNode("foo")
    createLabeledNode("foo", "bar")
    val c = createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE not(n:foo) RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> c)))
  }

  @Test def should_filter_nodes_by_multiple_labels() {
    // GIVEN
    createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE n:foo:bar RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> b)))
  }

  @Test def should_create_index() {
    // GIVEN
    val labelName = "Person"
    val propertyKeys = Seq("name")

    // WHEN
    execute(s"""CREATE INDEX ON :$labelName(${propertyKeys.reduce(_ ++ "," ++ _)})""")

    // THEN
    graph.inTx {
      val indexDefinitions = graph.schema().getIndexes(DynamicLabel.label(labelName)).asScala.toSet
      assert(1 === indexDefinitions.size)

      val actual = indexDefinitions.head.getPropertyKeys.asScala.toSeq
      assert(propertyKeys == actual)
    }
  }

  @Test def union_ftw() {
    createNode()

    // WHEN
    val result = execute("START n=node(0) RETURN 1 as x UNION ALL START n=node(0) RETURN 2 as x")

    // THEN
    assert(result.toList === List(Map("x" -> 1), Map("x" -> 2)))
  }

  @Test def union_distinct() {
    createNode()

    // WHEN
    val result = execute("START n=node(0) RETURN 1 as x UNION START n=node(0) RETURN 1 as x")

    // THEN
    assert(result.toList === List(Map("x" -> 1)))
  }


  @Test
  def read_only_database_can_process_has_label_predicates() {
    //GIVEN
    val engine = createReadOnlyEngine()

    //WHEN
    val result = engine.execute("MATCH (n) WHERE n:NonExistingLabel RETURN n")

    //THEN
    assert(result.toList === List())
  }

  private def createReadOnlyEngine(): ExecutionEngine = {
    val old = new GraphDatabaseFactory().newEmbeddedDatabase("target/readonly")
    old.shutdown()
    val db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("target/readonly")
            .setConfig( GraphDatabaseSettings.read_only, "true" )
            .newGraphDatabase();
    new ExecutionEngine(db)
  }

  def should_use_predicates_in_the_correct_place() {
    //GIVEN
    val m = execute( """create
                        advertiser = {name:"advertiser1"},
                        thing      = {name:"Color"},
                        red        = {name:"red"},
                        p1         = {name:"product1"},
                        p2         = {name:"product4"},
                        (advertiser)-[:adv_has_product]->(p1),
                        (advertiser)-[:adv_has_product]->(p2),
                        (thing)-[:aa_has_value]->(red),
                        (p1)   -[:ap_has_value]->(red),
                        (p2)   -[:ap_has_value]->(red)
                        return advertiser, thing""").toList.head

    val advertiser = m("advertiser").asInstanceOf[Node]
    val thing = m("thing").asInstanceOf[Node]

    //WHEN
    val result = execute(
      """START advertiser = node({1}), a = node({2})
       MATCH (advertiser) -[:adv_has_product] ->(out) -[:ap_has_value] -> red <-[:aa_has_value]- (a)
       WHERE red.name = 'red' and out.name = 'product1'
       RETURN out.name""", "1" -> advertiser, "2" -> thing)

    //THEN
    assert(result.toList === List(Map("out.name" -> "product1")))
  }

  @Test
  def should_not_create_when_match_exists() {
    //GIVEN
    val a = createNode()
    val b = createNode()
    relate(a,b,"FOO")

    //WHEN
    val result = execute(
      """START a=node(0), b=node(1)
         WHERE not (a)-[:FOO]->(b)
         CREATE (a)-[new:FOO]->(b)
         RETURN new""")

    //THEN
    assert(result.size === 0)
    assert(result.queryStatistics().relationshipsCreated === 0)
  }

  @Test
  def test550() {
    createNode()

    //WHEN
    val result = execute(
      """START p=node(0)
        WITH p
        START a=node(0)
        MATCH a-->b
        RETURN *""")

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List())
  }

  @Test
  def shouldProduceProfileWhenUsingLimit() {
    // GIVEN
    createNode()
    createNode()
    createNode()
    val result = profile("""START n=node(*) RETURN n LIMIT 1""")

    // WHEN
    result.toList

    // THEN PASS
    result.executionPlanDescription()
  }

  @Test
  def should_be_able_to_coalesce_nodes() {
    val n = createNode("n")
    val m = createNode("m")
    relate(n,m,"link")
    val result = execute("start n = node(0) with coalesce(n,n) as n match n--() return n")

    assert(result.toList === List(Map("n" -> n)))
  }

  @Test
  def multiple_start_points_should_still_honor_predicates() {
    val e = createNode()
    val p1 = createNode("value"->567)
    val p2 = createNode("value"->0)
    relate(p1,e)
    relate(p2,e)

    indexNode(p1, "stuff", "key", "value")
    indexNode(p2, "stuff", "key", "value")

    val result = execute("start p1=node:stuff('key:*'), p2=node:stuff('key:*') match (p1)--(e), (p2)--(e) where p1.value = 0 and p2.value = 0 AND p1 <> p2 return p1,p2,e")
    assert(result.toList === List())
  }

  @Test
  def should_be_able_to_prettify_queries() {
    val query = "match (n)-->(x) return n"

    assert(engine.prettify(query) === String.format("MATCH (n)-->(x)%nRETURN n"))
  }

  @Test
  def doctest_gone_wild() {
    // given
    execute("CREATE (n:Actor {name:'Tom Hanks'})")

    // when
    val result = execute("""MATCH (actor:Actor)
                               WHERE actor.name = "Tom Hanks"
                               CREATE (movie:Movie {title:'Sleepless in Seattle'})
                               CREATE (actor)-[:ACTED_IN]->(movie)""")

    // then
    assertStats(result, nodesCreated = 1, propertiesSet = 1, labelsAdded = 1, relationshipsCreated = 1)
  }

  @Test
  def should_iterate_all_node_id_sets_from_start_during_matching() {
    // given
    val nodes: List[Node] =
      execute("CREATE (a)-[:EDGE]->(b), (b)<-[:EDGE]-(c), (a)-[:EDGE]->(c) RETURN [a, b, c] AS nodes")
      .columnAs[List[Node]]("nodes").next().sortBy(_.getId)

    val nodeIds = s"node(${nodes.map(_.getId).mkString(",")})"

    // when
    val result = execute(s"START src=$nodeIds, dst=$nodeIds MATCH src-[r:EDGE]-dst RETURN r")

    // then
    val relationships: List[Relationship] = result.columnAs[Relationship]("r").toList

    assert( 6 === relationships.size )
  }

  @Test
  def merge_should_support_single_parameter() {
    //WHEN
    val result = execute("MERGE (n:User {foo: {single_param}})", ("single_param", 42))

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List())
  }

  @Test
  def merge_should_not_support_map_parameters_for_defining_properties() {
    intercept[SyntaxException](execute("MERGE (n:User {merge_map})", ("merge_map", Map("email" -> "test"))))
  }

  def should_not_hang() {
    // given
    createNode()
    createNode()

    // when
    timeOutIn(2, TimeUnit.SECONDS) {
      execute("START a=node(0), b=node(1) " +
        "MATCH x-->a, x-->b " +
        "WHERE x.foo > 2 AND x.prop IN ['val'] " +
        "RETURN x")
    }
    // then
  }

  @Test
  def should_return_null_on_all_comparisons_against_null() {
    // given

    // when
    val result = execute("return 1 > null as A, 1 < null as B, 1 <= null as C, 1 >= null as D, null <= null as E, null >= null as F")

    // then
    assert(result.toList === List(Map("A" -> null, "B" -> null, "C" -> null, "D" -> null, "E" -> null, "F" -> null)))
  }

  @Test
  def should_be_able_to_coerce_collections_to_predicates() {
    val n = createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> true), "LABEL")

    val foundNode = execute("match (n:LABEL) where n.coll and n.bool return n").columnAs[Node]("n").next()

    assert(foundNode === n)
  }

  @Test
  def should_be_able_to_coerce_literal_collections_to_predicates() {
    val n = createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> true), "LABEL")

    val foundNode = execute("match (n:LABEL) where [1,2,3] and n.bool return n").columnAs[Node]("n").next()

    assert(foundNode === n)
  }

  @Test
  def query_should_work() {
    assert(executeScalar[Int]("WITH 1 AS x RETURN 1 + x") === 2)
  }

  @Test
  def should_be_able_to_mix_key_expressions_with_aggregate_expressions() {
    // Given
    createNode("Foo")

    // when
    val result = executeScalar[Map[String, Any]]("match (n) return { name: n.name, count: count(*) }")

    // then
    assert(result("name") === "Foo")
    assert(result("count") === 1)
  }

  @Test
  def should_not_mind_rewriting_NOT_queries() {
    val result = execute(" create (a {x: 1}) return a.x is not null as A, a.y is null as B, a.x is not null as C, a.y is not null as D")
    assert(result.toList === List(Map("A" -> true, "B" -> true, "C" -> true, "D" -> false)))
  }

  @Test
  def should_not_mind_profiling_union_queries() {
    val result = profile("return 1 as A union return 2 as A")
    assert(result.toList === List(Map("A" -> 1), Map("A" -> 2)))
  }

  @Test
  def should_not_mind_profiling_merge_queries() {
    val result = profile("merge (a {x: 1}) return a.x as A")
    assert(result.toList.head("A") === 1)
  }

  @Test
  def should_not_mind_profiling_optional_match_queries() {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (a:Label {x: 1}) optional match (a)-[:REL]->(b) return a.x as A, b.x as B").toList.head
    assert(result("A") === 1)
    assert(result("B") === null)
  }

  @Test
  def should_not_mind_profiling_optional_match_and_with() {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (n) optional match (n)--(m) with n, m where m is null return n.x as A").toList.head
    assert(result("A") === 1)
  }
}
