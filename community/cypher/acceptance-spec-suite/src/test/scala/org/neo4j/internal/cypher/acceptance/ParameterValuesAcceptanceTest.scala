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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class ParameterValuesAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with QueryStatisticsTestSupport {

  // Not TCK material below; sending graph types or characters as parameters is not supported

  test("should not erase the type of an empty array sent as parameter") {
    import Array._

    Seq(emptyLongArray, emptyShortArray, emptyByteArray, emptyIntArray,
      emptyDoubleArray, emptyFloatArray,
      emptyBooleanArray, Array[String]()).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWithAllPlanners(q, "param" -> array)

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should not erase the type of nonempty arrays sent as parameter") {
    Seq(Array[Long](1l), Array[Short](2), Array[Byte](3), Array[Int](4),
      Array[Double](3.14), Array[Float](5.56f),
      Array[Boolean](false, true), Array[String]("", " ")).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWithAllPlanners(q, "param" -> array)

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should be able to send in node via parameter") {
    // given
    val node = createLabeledNode("Person")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (b) WHERE b = {param} RETURN b", "param" -> node)
    result.toList should equal(List(Map("b" -> node)))
  }

  test("should be able to send in relationship via parameter") {
    // given
    val rel = relate(createLabeledNode("Person"), createLabeledNode("Person"))

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (:Person)-[r]->(:Person) WHERE r = {param} RETURN r", "param" -> rel)
    result.toList should equal(List(Map("r" -> rel)))
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = {param}", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN {param} = 'a'", "param" -> 'a') shouldBe true
  }

  test("removing property when not sure if it is a node or relationship should still work - NODE") {
    val n = createNode("name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> n)

    graph.inTx {
      n.getProperty("lastname") should equal("Anders")
      n.hasProperty("name") should equal(false)
    }
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> r)

    graph.inTx {
      r.getProperty("lastname") should equal("Anders")
      r.hasProperty("name") should equal(false)
    }
  }

  test("parameters should be transformed to cypher types") {
    def echo(any: Any): AnyRef = {
      executeScalarWithAllPlannersAndCompatibilityMode[AnyRef]("RETURN {p} AS p", "p" -> any)
    }

    echo(42.asInstanceOf[Byte]) shouldBe a[java.lang.Long]
    echo(42.asInstanceOf[Short]) shouldBe a[java.lang.Long]
    echo(42) shouldBe a[java.lang.Long]
    echo(42L) shouldBe a[java.lang.Long]
    echo(42.0f) shouldBe a[java.lang.Double]
  }

  test("array parameters should be transformed to cypher types") {
    def echo(any: Any): Array[_] = {
      executeWithAllPlannersAndRuntimesAndCompatibilityMode("RETURN {p} AS p", "p" -> any)
        .columnAs[Array[_]]("p").next()
    }

    def echoNoCompatibility(any: Any): Array[_] = {
      executeWithAllPlanners("RETURN {p} AS p", "p" -> any)
        .columnAs[Array[_]]("p").next()
    }

    // Arrays of primitives
    echo(Array[Byte](42.asInstanceOf[Byte])).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[Short](42.asInstanceOf[Short])).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[Int](42)).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[Long](42L)).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[Float](42.0F)).foreach(_ shouldBe a[java.lang.Double])
    echo(Array[Double](42.0D)).foreach(_ shouldBe a[java.lang.Double])
    echoNoCompatibility(Array[Char]('a')).foreach(_ shouldBe a[java.lang.String])

    // Arrays of boxed values
    echo(Array[java.lang.Byte](42.asInstanceOf[Byte])).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[java.lang.Short](42.asInstanceOf[Short])).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[java.lang.Integer](42)).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[java.lang.Long](42L)).foreach(_ shouldBe a[java.lang.Long])
    echo(Array[java.lang.Float](42.0F)).foreach(_ shouldBe a[java.lang.Double])
    echo(Array[java.lang.Double](42.0D)).foreach(_ shouldBe a[java.lang.Double])
    echo(Array[java.lang.String]("a")).foreach(_ shouldBe a[java.lang.String])
    echoNoCompatibility(Array[java.lang.Character]('a')).foreach(_ shouldBe a[java.lang.String])

    // Nested array
    echo(Array[Array[Byte]](Array(42.asInstanceOf[Byte]))).foreach { x =>
      x shouldBe a[Array[AnyRef]]
      x.asInstanceOf[Array[_]].foreach(_ shouldBe a[java.lang.Long])
    }
  }

  test("list parameters should be transformed to cypher types") {
    def echo(any: Any): Iterable[_] = {
      executeWithAllPlannersAndRuntimesAndCompatibilityMode("RETURN {p} AS p", "p" -> any)
        .columnAs[Iterable[_]]("p").next()
    }

    def echoNoCompatibility(any: Any): Iterable[_] = {
      executeWithAllPlanners("RETURN {p} AS p", "p" -> any)
        .columnAs[Iterable[_]]("p").next()
    }

    // Lists
    echo(java.util.Arrays.asList[java.lang.Byte](42.asInstanceOf[Byte])).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Arrays.asList[java.lang.Short](42.asInstanceOf[Short])).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Arrays.asList[java.lang.Integer](42)).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Arrays.asList[java.lang.Long](42L)).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Arrays.asList[java.lang.Float](42.0F)).foreach(_ shouldBe a[java.lang.Double])
    echo(java.util.Arrays.asList[java.lang.Double](42.0D)).foreach(_ shouldBe a[java.lang.Double])
    echo(java.util.Arrays.asList[java.lang.String]("a")).foreach(_ shouldBe a[java.lang.String])
    echoNoCompatibility(java.util.Arrays.asList[java.lang.Character]('a')).foreach(_ shouldBe a[java.lang.String])

    // Nested list
    echo(java.util.Arrays.asList[java.util.List[java.lang.Byte]](
      java.util.Arrays.asList[java.lang.Byte](42.asInstanceOf[Byte])))
      .foreach { x =>
        x shouldBe a[Iterable[AnyRef]]
        x.asInstanceOf[Iterable[_]].foreach(_ shouldBe a[java.lang.Long])
      }
  }

  test("map parameters should be transformed to cypher types") {
    def echo(any: Any): Iterable[_] = {
      executeWithAllPlannersAndRuntimesAndCompatibilityMode("RETURN {p} AS p", "p" -> any)
        .columnAs[Map[_, _]]("p").next().values
    }

    def echoNoCompatibility(any: Any): Iterable[_] = {
      executeWithAllPlanners("RETURN {p} AS p", "p" -> any)
        .columnAs[Map[_, _]]("p").next().values
    }

    // Maps
    echo(java.util.Collections.singletonMap("k", 42.asInstanceOf[Byte])).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Collections.singletonMap("k", 42.asInstanceOf[Short])).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Collections.singletonMap("k", 42)).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Collections.singletonMap("k", 42L)).foreach(_ shouldBe a[java.lang.Long])
    echo(java.util.Collections.singletonMap("k", 42.0F)).foreach(_ shouldBe a[java.lang.Double])
    echo(java.util.Collections.singletonMap("k", 42.0D)).foreach(_ shouldBe a[java.lang.Double])
    echo(java.util.Collections.singletonMap("k", "a")).foreach(_ shouldBe a[java.lang.String])
    echoNoCompatibility(java.util.Collections.singletonMap("k", 'a')).foreach(_ shouldBe a[java.lang.String])

    // Nested map
    echo(java.util.Collections.singletonMap("k",
      java.util.Collections.singletonMap("k", 42.asInstanceOf[Byte])))
      .foreach { x =>
        x shouldBe a[Map[_, _]]
        x.asInstanceOf[Map[_, _]].values.foreach(_ shouldBe a[java.lang.Long])
      }
  }
}
