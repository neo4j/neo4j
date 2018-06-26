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
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.lang.Math.PI
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.logical.plans.CoerceToPredicate
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian
import org.neo4j.values.storable.LocalTimeValue.localTime
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable.{DoubleValue, PointValue, Values}
import org.neo4j.values.virtual.VirtualValues._
import org.neo4j.values.virtual.{NodeValue, RelationshipValue, VirtualValues}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{CypherTypeException, symbols}

class CodeGenerationTest extends CypherFunSuite with AstConstructionTestSupport {

  private val ctx = mock[ExecutionContext]
  private val db = mock[DbAccess]
  when(db.relationshipGetStartNode(any[RelationshipValue])).thenAnswer(new Answer[NodeValue] {
    override def answer(in: InvocationOnMock): NodeValue = in.getArgument[RelationshipValue](0).startNode()
  })
  when(db.relationshipGetEndNode(any[RelationshipValue])).thenAnswer(new Answer[NodeValue] {
    override def answer(in: InvocationOnMock): NodeValue = in.getArgument[RelationshipValue](0).endNode()
  })

  private val random = ThreadLocalRandom.current()

  test("round function") {
    compile(function("round", literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(3.0))
    compile(function("round", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("rand function") {
    // Given
    val expression = function("rand")

    // When
    val compiled = compile(expression)

    // Then
    val value = compiled.evaluate(ctx, db, EMPTY_MAP).asInstanceOf[DoubleValue].doubleValue()
    value should (be >= 0.0 and be <1.0)
  }

  test("sin function") {
    val arg = random.nextDouble()
    compile(function("sin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.sin(arg)))
    compile(function("sin", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("asin function") {
    val arg = random.nextDouble()
    compile(function("asin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.asin(arg)))
    compile(function("asin", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("haversin function") {
    val arg = random.nextDouble()
    compile(function("haversin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue((1.0 - Math.cos(arg)) / 2))
    compile(function("haversin", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("acos function") {
    val arg = random.nextDouble()
    compile(function("acos", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.acos(arg)))
    compile(function("acos", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("cos function") {
    val arg = random.nextDouble()
    compile(function("cos", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.cos(arg)))
    compile(function("cos", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("cot function") {
    val arg = random.nextDouble()
    compile(function("cot", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(1 / Math.tan(arg)))
    compile(function("cot", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("atan function") {
    val arg = random.nextDouble()
    compile(function("atan", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.atan(arg)))
    compile(function("atan", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("atan2 function") {
    val arg1 = random.nextDouble()
    val arg2 = random.nextDouble()
    compile(function("atan2", literalFloat(arg1), literalFloat(arg2))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.atan2(arg1, arg2)))
    compile(function("atan2", noValue,literalFloat(arg1))).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
    compile(function("atan2", literalFloat(arg1), noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
    compile(function("atan2", noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("tan function") {
    val arg = random.nextDouble()
    compile(function("tan", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.tan(arg)))
    compile(function("tan", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("ceil function") {
    val arg = random.nextDouble()
    compile(function("ceil", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.ceil(arg)))
    compile(function("ceil", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("floor function") {
    val arg = random.nextDouble()
    compile(function("floor", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.floor(arg)))
    compile(function("floor", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("abs function") {
    compile(function("abs", literalFloat(3.2))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(3.2))
    compile(function("abs", literalFloat(-3.2))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(3.2))
    compile(function("abs", literalInt(3))).evaluate(ctx, db, EMPTY_MAP) should equal(longValue(3))
    compile(function("abs", literalInt(-3))).evaluate(ctx, db, EMPTY_MAP) should equal(longValue(3))
    compile(function("abs", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("radians function") {
    val arg = random.nextDouble()
    compile(function("radians", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.toRadians(arg)))
    compile(function("radians", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("degrees function") {
    val arg = random.nextDouble()
    compile(function("degrees", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.toDegrees(arg)))
    compile(function("degrees", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("exp function") {
    val arg = random.nextDouble()
    compile(function("exp", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.exp(arg)))
    compile(function("exp", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("log function") {
    val arg = random.nextDouble()
    compile(function("log", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.log(arg)))
    compile(function("log", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("log10 function") {
    val arg = random.nextDouble()
    compile(function("log10", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.log10(arg)))
    compile(function("log10", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("sign function") {
    val arg = random.nextInt()
    compile(function("sign", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.signum(arg)))
    compile(function("sign", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("sqrt function") {
    val arg = random.nextDouble()
    compile(function("sqrt", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP) should equal(doubleValue(Math.sqrt(arg)))
    compile(function("sqrt", noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("pi function") {
    compile(function("pi")).evaluate(ctx, db, EMPTY_MAP) should equal(Values.PI)
  }

  test("e function") {
    compile(function("e")).evaluate(ctx, db, EMPTY_MAP) should equal(Values.E)
  }

  test("range function") {
    val range = function("range", literalInt(5), literalInt(9), literalInt(2))
    compile(range).evaluate(ctx, db, EMPTY_MAP) should equal(list(longValue(5), longValue(7), longValue(9)))
  }

  test("coalesce function") {
    compile(function("coalesce", noValue, noValue, literalInt(2), noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(longValue(2))
    compile(function("coalesce", noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("coalesce function with parameters") {
    val compiled = compile(function("coalesce", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(NO_VALUE, longValue(2), NO_VALUE))) should equal(longValue(2))
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(NO_VALUE, NO_VALUE, NO_VALUE))) should equal(NO_VALUE)
  }

  test("distance function") {
    val compiled = compile(function("distance", parameter("p1"), parameter("p2")))
    val keys = Array("p1", "p2")
    compiled.evaluate(ctx, db, map(keys,
                                   Array(pointValue(Cartesian, 0.0, 0.0),
                                         pointValue(Cartesian, 1.0, 1.0)))) should equal(doubleValue(Math.sqrt(2)))
    compiled.evaluate(ctx, db, map(keys,
                                   Array(pointValue(Cartesian, 0.0, 0.0),
                                         NO_VALUE))) should equal(NO_VALUE)
  }

  test("startNode") {
    val compiled = compile(function("startNode", parameter("a")))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(rel.startNode())
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("endNode") {
    val compiled = compile(function("endNode", parameter("a")))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(rel.endNode())
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)

  }

  test("exists on node") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, map(Array("prop"), Array(stringValue("hello"))))
    when(db.nodeHasProperty(1, "prop")).thenReturn(true)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(Values.TRUE)
  }

  test("exists on relationship") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), map(Array("prop"), Array(stringValue("hello"))))
    when(db.relationshipHasProperty(43, "prop")).thenReturn(true)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(Values.TRUE)
  }

  test("exists on map") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue))) should equal(Values.TRUE)
  }

  test("head function") {
    val compiled = compile(function("head", parameter("a")))
    val listValue = list(stringValue("hello"), intValue(42))

    compiled.evaluate(ctx, db, map(Array("a"), Array(listValue))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("last function") {
    val compiled = compile(function("last", parameter("a")))
    val listValue = list(intValue(42), stringValue("hello"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(listValue))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("left function") {
    val compiled = compile(function("left", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4)))) should
      equal(stringValue("HELL"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4)))) should equal(NO_VALUE)
  }

  test("ltrim function") {
    val compiled = compile(function("ltrim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  ")))) should
      equal(stringValue("HELLO  "))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("rtrim function") {
    val compiled = compile(function("rtrim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  ")))) should
      equal(stringValue("  HELLO"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("trim function") {
    val compiled = compile(function("trim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  ")))) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("replace function") {
    val compiled = compile(function("replace", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                   Array(stringValue("HELLO"),
                                         stringValue("LL"),
                                         stringValue("R")))) should equal(stringValue("HERO"))
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                   Array(NO_VALUE,
                                         stringValue("LL"),
                                         stringValue("R")))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                   Array(stringValue("HELLO"),
                                         NO_VALUE,
                                         stringValue("R")))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                   Array(stringValue("HELLO"),
                                         stringValue("LL"),
                                         NO_VALUE))) should equal(NO_VALUE)
  }

  test("reverse function") {
    val compiled = compile(function("reverse", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("PARIS")))) should equal(stringValue("SIRAP"))
    val original = list(intValue(1), intValue(2), intValue(3))
    val reversed = list(intValue(3), intValue(2), intValue(1))
    compiled.evaluate(ctx, db, map(Array("a"), Array(original))) should equal(reversed)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("right function") {
    val compiled = compile(function("right", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4)))) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4)))) should equal(NO_VALUE)
  }

  test("split function") {
    val compiled = compile(function("split", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), stringValue("LL")))) should
      equal(list(stringValue("HE"), stringValue("O")))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, stringValue("LL")))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), NO_VALUE))) should equal(NO_VALUE)
  }

  test("substring function no length") {
    val compiled = compile(function("substring", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(1)))) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1)))) should equal(NO_VALUE)
  }

  test("substring function with length") {
    val compiled = compile(function("substring", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(stringValue("HELLO"), intValue(1), intValue(2)))) should
      equal(stringValue("EL"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1)))) should equal(NO_VALUE)
  }

  test("nodes function") {
    val compiled = compile(function("nodes", parameter("a")))

    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
          nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
          nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(path))) should equal(VirtualValues.list(nodes:_*))
  }

  test("relationships function") {
    val compiled = compile(function("relationships", parameter("a")))

    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(path))) should equal(VirtualValues.list(rels:_*))
  }

  test("id on node") {
    val compiled = compile(function("id", parameter("a")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(longValue(1))
  }

  test("id on relationship") {
    val compiled = compile(function("id", parameter("a")))

    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"),EMPTY_MAP)


    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(longValue(43))
  }

  test("labels function") {
    val compiled = compile(function("labels", parameter("a")))

    val labels = Values.stringArray("A", "B", "C")
    val node = nodeValue(1, labels, EMPTY_MAP)
    when(db.getLabelsForNode(node.id())).thenReturn(VirtualValues.fromArray(labels))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(labels)
  }

  test("points from node") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val node = nodeValue(1, EMPTY_TEXT_ARRAY, pointMap)
    when(db.nodeProperty(any[Long], any[String])).thenAnswer(new Answer[AnyValue] {
      override def answer(in: InvocationOnMock): AnyValue = pointMap.get(in.getArgument[String](1))
    })

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(PointValue.fromMap(pointMap))
  }

  test("points from relationship") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val rel = relationshipValue(43,
                      nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      stringValue("R"),pointMap)

    when(db.relationshipProperty(any[Long], any[String])).thenAnswer(new Answer[AnyValue] {
      override def answer(in: InvocationOnMock): AnyValue = pointMap.get(in.getArgument[String](1))
    })

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(PointValue.fromMap(pointMap))
  }

  test("points from map") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(pointMap))) should equal(PointValue.fromMap(pointMap))
  }

  test("keys on node") {
    val compiled = compile(function("keys", parameter("a")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP)
    when(db.nodePropertyIds(1)).thenReturn(Array(1,2,3))
    when(db.getPropertyKeyName(1)).thenReturn("A")
    when(db.getPropertyKeyName(2)).thenReturn("B")
    when(db.getPropertyKeyName(3)).thenReturn("C")

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on relationship") {
    val compiled = compile(function("keys", parameter("a")))


    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    when(db.relationshipPropertyIds(43)).thenReturn(Array(1,2,3))
    when(db.getPropertyKeyName(1)).thenReturn("A")
    when(db.getPropertyKeyName(2)).thenReturn("B")
    when(db.getPropertyKeyName(3)).thenReturn("C")


    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on map") {
    val compiled = compile(function("keys", parameter("a")))

    val mapValue = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue))) should equal(mapValue.keys())
  }

  test("size function") {
    val compiled = compile(function("size", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("HELLO")))) should equal(intValue(5))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4)))) should equal(NO_VALUE)
  }

  test("length function") {
    val compiled = compile(function("length", parameter("a")))
    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(path))) should equal(intValue(2))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4)))) should equal(NO_VALUE)
  }

  test("tail function") {
    val compiled = compile(function("tail", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(list(intValue(1), intValue(2), intValue(3))))) should equal(list(intValue(2), intValue(3)))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("add numbers") {
    // Given
    val expression = add(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(longValue(52))
  }

  test("add temporals") {
    val compiled = compile(add(parameter("a"), parameter("b")))

    // temporal + duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(temporalValue(localTime(0)),
                                   durationValue(Duration.ofHours(10))))) should
      equal(localTime(10, 0, 0, 0))

    // duration + temporal
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                         temporalValue(localTime(0))))) should
      equal(localTime(10, 0, 0, 0))

    //duration + duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                          durationValue(Duration.ofHours(10))))) should
      equal(durationValue(Duration.ofHours(20)))
  }

  test("add with NO_VALUE") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42)))) should equal(NO_VALUE)
  }

  test("add strings") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // string1 + string2
    compiled.evaluate(ctx, db,
                      map(Array("a", "b"), Array(stringValue("hello "), stringValue("world")))) should
      equal(stringValue("hello world"))
    //string + other
    compiled.evaluate(ctx, db,
                      map(Array("a", "b"),
                          Array(stringValue("hello "), longValue(1337)))) should
      equal(stringValue("hello 1337"))
    //other + string
    compiled.evaluate(ctx, db,
                      map(Array("a", "b"),
                          Array(longValue(1337), stringValue(" hello")))) should
      equal(stringValue("1337 hello"))

  }

  test("add arrays") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                   Array(longArray(Array(42, 43)),
                                        longArray(Array(44, 45))))) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))
  }

  test("list addition") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // [a1,a2 ..] + [b1,b2 ..]
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                   Array(list(longValue(42), longValue(43)),
                                         list(longValue(44), longValue(45))))) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))

    // [a1,a2 ..] + b
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                   Array(list(longValue(42), longValue(43)), longValue(44)))) should
      equal(list(longValue(42), longValue(43), longValue(44)))

    // a + [b1,b2 ..]
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                   Array(longValue(43),
                                         list(longValue(44), longValue(45))))) should
      equal(list(longValue(43), longValue(44), longValue(45)))
  }

  test("subtract numbers") {
    // Given
    val expression = subtract(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(longValue(32))
  }

  test("subtract with NO_VALUE") {
    // Given
    val expression = subtract(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42)))) should equal(NO_VALUE)
  }

  test("subtract temporals") {
    val compiled = compile(subtract(parameter("a"), parameter("b")))

    // temporal - duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(temporalValue(localTime(20, 0, 0, 0)),
                                                          durationValue(Duration.ofHours(10))))) should
      equal(localTime(10, 0, 0, 0))

    //duration - duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                          durationValue(Duration.ofHours(10))))) should
      equal(durationValue(Duration.ofHours(0)))
  }

  test("multiply function") {
    // Given
    val expression = multiply(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(longValue(420))
  }

  test("multiply with NO_VALUE") {
    // Given
    val expression = multiply(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE))) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42)))) should equal(NO_VALUE)
  }

  test("extract parameter") {
    // Given
    val expression = parameter("prop")

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("prop"), Array(stringValue("foo")))) should equal(stringValue("foo"))
  }

  test("NULL") {
    // Given
    val expression = noValue

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(NO_VALUE)
  }

  test("TRUE") {
    // Given
    val expression = t

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
  }

  test("FALSE") {
    // Given
    val expression = f

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
  }

  test("OR") {
    compile(or(t, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(f, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(t, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)

    compile(or(noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(or(noValue, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(t, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(noValue, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(or(f, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("XOR") {
    compile(xor(t, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(xor(f, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(xor(t, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(xor(f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)

    compile(xor(noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(xor(noValue, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(xor(t, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(xor(noValue, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(xor(f, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("OR should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(or(literalInt(42), f)).evaluate(ctx, db, EMPTY_MAP)
    a [CypherTypeException] should be thrownBy compile(or(f, literalInt(42))).evaluate(ctx, db, EMPTY_MAP)
    compile(or(t, literalInt(42))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(or(literalInt(42), t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
  }

  test("OR should handle coercion") {
    val expression =  compile(or(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST))) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello"))))) should equal(Values.TRUE)
  }

  test("ORS") {
    compile(ors(f, f, f, f, f, f, t, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(ors(f, f, f, f, f, f, f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(ors(f, f, f, f, noValue, f, f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(ors(f, f, f, t, noValue, t, f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
  }

  test("ORS should throw on non-boolean input") {
    val compiled = compile(ors(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, db,
                      map(keys, Array(Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE))) should equal(Values.FALSE)

    compiled.evaluate(ctx, db,
                      map(keys, Array(Values.FALSE, Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE))) should equal(Values.TRUE)

    compiled.evaluate(ctx, db,
                      map(keys, Array(intValue(42), Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE))) should equal(Values.TRUE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, db,
                                                                 map(keys, Array(intValue(42), Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)))
  }

  test("ORS should handle coercion") {
    val expression =  compile(ors(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST))) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello"))))) should equal(Values.TRUE)
  }

  test("AND") {
    compile(and(t, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(and(f, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(and(t, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(and(f, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)

    compile(and(noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(and(noValue, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(and(t, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(and(noValue, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(and(f, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
  }

  test("AND should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(and(literalInt(42), t)).evaluate(ctx, db, EMPTY_MAP)
    a [CypherTypeException] should be thrownBy compile(and(t, literalInt(42))).evaluate(ctx, db, EMPTY_MAP)
    compile(and(f, literalInt(42))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(and(literalInt(42), f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
  }

  test("AND should handle coercion") {
    val expression =  compile(and(parameter("a"), parameter("b")))
   expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST))) should equal(Values.FALSE)
   expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello"))))) should equal(Values.TRUE)
  }

  test("ANDS") {
    compile(ands(t, t, t, t, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(ands(t, t, t, t, t, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(ands(t, t, t, t, noValue, t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(ands(t, t, t, f, noValue, f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
  }

  test("ANDS should throw on non-boolean input") {
    val compiled = compile(ands(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, db,
                      map(keys, Array(Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE))) should equal(Values.TRUE)

    compiled.evaluate(ctx, db,
                      map(keys, Array(Values.TRUE, Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE))) should equal(Values.FALSE)

    compiled.evaluate(ctx, db,
                      map(keys, Array(intValue(42), Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE))) should equal(Values.FALSE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, db,
                                    map(keys, Array(intValue(42), Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)))
  }

  test("ANDS should handle coercion") {
    val expression =  compile(ands(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST))) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello"))))) should equal(Values.TRUE)
  }

  test("NOT") {
    compile(not(f)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(not(t)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(not(noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("NOT should handle coercion") {
    val expression =  compile(not(parameter("a")))
    expression.evaluate(ctx, db, map(Array("a"), Array(EMPTY_LIST))) should equal(Values.TRUE)
    expression.evaluate(ctx, db, map(Array("a"), Array(list(stringValue("hello"))))) should equal(Values.FALSE)
  }

  test("EQUALS") {
    compile(equals(literalInt(42), literalInt(42))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(equals(literalInt(42), literalInt(43))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(equals(noValue, literalInt(43))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(equals(literalInt(42), noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(equals(noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("NOT EQUALS") {
    compile(notEquals(literalInt(42), literalInt(42))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
    compile(notEquals(literalInt(42), literalInt(43))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(notEquals(noValue, literalInt(43))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(notEquals(literalInt(42), noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(notEquals(noValue, noValue)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
  }

  test("CoerceToPredicate") {
    val coerced = CoerceToPredicate(parameter("a"))

    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(Values.FALSE))) should equal(Values.FALSE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(Values.TRUE))) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(list(stringValue("A"))))) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(list(EMPTY_LIST)))) should equal(Values.TRUE)
  }

  test("ReferenceFromSlot") {
    // Given
    val offset = 1337
    val expression = ReferenceFromSlot(offset, "foo")
    when(ctx.getRefAt(offset)).thenReturn(stringValue("hello"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(stringValue("hello"))
  }

  test("IdFromSlot") {
    // Given
    val offset = 1337
    val expression = IdFromSlot(offset)
    when(ctx.getLongAt(offset)).thenReturn(42L)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP) should equal(longValue(42))
  }

  test("PrimitiveEquals") {
    val compiled = compile(PrimitiveEquals(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), longValue(42)))) should
      equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), longValue(1337)))) should
      equal(Values.FALSE)
  }

  test("NullCheck") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(42L)

    compile(NullCheck(nullOffset, literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.NO_VALUE)
    compile(NullCheck(offset, literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP) should equal(Values.PI)
  }

  test("NullCheckVariable") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(42L)
    when(ctx.getRefAt(nullOffset)).thenReturn(NO_VALUE)
    when(ctx.getRefAt(offset)).thenReturn(stringValue("hello"))

    compile(NullCheckVariable(nullOffset, ReferenceFromSlot(offset, "a"))).evaluate(ctx, db, EMPTY_MAP) should
      equal(Values.NO_VALUE)
    compile(NullCheckVariable(offset, ReferenceFromSlot(offset, "a"))).evaluate(ctx, db, EMPTY_MAP) should
      equal(stringValue("hello"))
  }

  test("IsPrimitiveNull") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(77L)

    compile(IsPrimitiveNull(nullOffset)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.TRUE)
    compile(IsPrimitiveNull(offset)).evaluate(ctx, db, EMPTY_MAP) should equal(Values.FALSE)
  }

  test("containerIndex on node") {
    val node =  nodeValue(1, EMPTY_TEXT_ARRAY, map(Array("prop"), Array(stringValue("hello"))))
    when(db.nodeProperty(1, "prop")).thenReturn(stringValue("hello"))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(node))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("containerIndex on relationship") {
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), map(Array("prop"), Array(stringValue("hello"))))
    when(db.relationshipProperty(43, "prop")).thenReturn(stringValue("hello"))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(rel))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("containerIndex on map") {
    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  test("containerIndex on list") {
    val listValue = list(longValue(42), stringValue("hello"), intValue(42))
    val compiled = compile(containerIndex(parameter("a"), literalInt(1)))

    compiled.evaluate(ctx, db, map(Array("a"), Array(listValue))) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE))) should equal(NO_VALUE)
  }

  private def compile(e: Expression) =
    CodeGeneration.compile(new IntermediateCodeGeneration(SlotConfiguration.empty).compile(e).map(_.ir).getOrElse(fail()))

  private def function(name: String, es: Expression*) =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, es.toIndexedSeq)(pos)


  private def function(name: String) =
    FunctionInvocation(Namespace()(pos), FunctionName(name)(pos), distinct = false, IndexedSeq.empty)(pos)

  private def add(l: Expression, r: Expression) = Add(l, r)(pos)

  private def subtract(l: Expression, r: Expression) = Subtract(l, r)(pos)

  private def multiply(l: Expression, r: Expression) = Multiply(l, r)(pos)

  private def parameter(key: String) = Parameter(key, symbols.CTAny)(pos)

  private def noValue = Null()(pos)

  private def t = True()(pos)

  private def f = False()(pos)

  private def or(l: Expression, r: Expression) = Or(l, r)(pos)

  private def xor(l: Expression, r: Expression) = Xor(l, r)(pos)

  private def ors(es: Expression*) = Ors(es.toSet)(pos)

  private def and(l: Expression, r: Expression) = And(l, r)(pos)

  private def ands(es: Expression*) = Ands(es.toSet)(pos)

  private def not(e: Expression) = Not(e)(pos)

  private def equals(lhs: Expression, rhs: Expression) = Equals(lhs, rhs)(pos)

  private def notEquals(lhs: Expression, rhs: Expression) = NotEquals(lhs, rhs)(pos)

  private def property(map: Expression, key: String) = Property(map, PropertyKeyName(key)(pos))(pos)

  private def containerIndex(container: Expression, index: Expression) = ContainerIndex(container, index)(pos)

 private def literalString(s: String) = expressions.StringLiteral(s)(pos)

}
