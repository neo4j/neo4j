/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.codegen

import java.util

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.{ListValue, VirtualValues}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertions, Matchers, PropSpec}

import scala.collection.JavaConverters._

class CompiledMathHelperTest extends PropSpec with TableDrivenPropertyChecks with Matchers with Assertions {

  val javaValues: Seq[AnyRef] =
  // The exclamation mark casts the expression to a AnyRef instead of a primitive
  // To be as exhaustive as possible, the strategy is to do a cartesian product of all test values,
  // and ensure that either we have defined behaviour, or that a runtime type exception is thrown
    Seq(
      1 !,
      6789 !,
      3.14 !,
      null,
      "a",
      List(1,2,3).asJava,
      true !,
      false !
    )

  val neoValues: Seq[AnyRef] = javaValues.map(ValueUtils.of)

  val neoOnlyValues: Seq[AnyRef] =
    Seq(
      // Temporal and spatial types should only ever reach the compiled runtime as neo values
      DateValue.date(2018, 5, 3).asInstanceOf[AnyRef],
      DurationValue.duration(1, 1, 0, 0).asInstanceOf[AnyRef],
      PointValue.parse("{x: 2, y: 3}").asInstanceOf[AnyRef]
    )

  val values: Seq[AnyRef] = javaValues ++ neoValues ++ neoOnlyValues

  property("+") {
    forAll(getTable(CompiledMathHelper.add)) {
      // Nulls
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (Values.NO_VALUE,      _,                    Right(result)) => result should equal(null)
      case (_,                    Values.NO_VALUE,      Right(result)) => result should equal(null)

      // Java values
      case (l: java.lang.Double,  r: Number,            Right(result)) => result should equal(l + r.doubleValue())
      case (l: Number,            r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() + r)
      case (l: java.lang.Float,   r: Number,            Right(result)) => result should equal(l + r.doubleValue())
      case (l: Number,            r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() + r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() + r.longValue())

      case (l: Number,            r: String,            Right(result)) => result should equal(l.toString + r)
      case (l: String,            r: Number,            Right(result)) => result should equal(l + r.toString)
      case (l: String,            r: String,            Right(result)) => result should equal(l + r)
      case (l: String,            r: java.lang.Boolean, Right(result)) => result should equal(l + r)
      case (l: java.lang.Boolean, r: String,            Right(result)) => result should equal(l + r)

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      // Neo values
      case (l: DoubleValue,       r: NumberValue,       Right(result)) =>
        java.lang.Double.compare(result.asInstanceOf[NumberValue].doubleValue(), l.doubleValue() + r.doubleValue()) should equal(0)
      case (l: NumberValue,       r: DoubleValue,       Right(result)) =>
        java.lang.Double.doubleToLongBits(result.asInstanceOf[NumberValue].doubleValue()) should equal(
          java.lang.Double.doubleToLongBits(l.doubleValue() + r.doubleValue()))
      case (l: FloatValue,        r: NumberValue,       Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: NumberValue,       r: FloatValue,        Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: NumberValue,       r: NumberValue,       Right(result)) => result should equal(Values.longValue(l.longValue() + r.longValue()))

      case (l: NumberValue,       r: TextValue,         Right(result)) => result should equal(l.prettyPrint() + r.stringValue())
      case (l: TextValue,         r: NumberValue,       Right(result)) => result should equal(l.stringValue() + r.prettyPrint())
      case (l: TextValue,         r: TextValue,         Right(result)) => result should equal(l.stringValue() + r.stringValue())
      case (l: TextValue,         r: BooleanValue,      Right(result)) => result should equal(l.stringValue() + r.booleanValue())
      case (l: BooleanValue,      r: TextValue,         Right(result)) => result should equal(l.booleanValue() + r.stringValue())

      case (_: NumberValue,       _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]

      case (l: TemporalValue[_, _], r: DurationValue,       Right(result)) => result should equal(l.plus(r))
      case (l: DurationValue,       r: TemporalValue[_, _], Right(result)) => result should equal(r.plus(l))
      case (l: DurationValue,       r: DurationValue,       Right(result)) => result should equal(l.add(r))

      case (l: TemporalValue[_, _], _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: TemporalValue[_, _], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: DurationValue,       _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: DurationValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: PointValue,          _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: PointValue,          Left(exception)) => exception shouldBe a [CypherTypeException]

      // Lists
      case (l1: util.List[_], l2: util.List[_], Right(result)) => result should equal(concat(l1, l2))
      case (l1: ListValue,    l2: ListValue,    Right(result)) => result should equal(
        VirtualValues.fromList(concat(l1.asArray().toList.asJava, l1.asArray().toList.asJava).asInstanceOf[util.List[AnyValue]])
      )
      case (l1: ListValue, l2: util.List[_], Right(result)) => result should equal(
        VirtualValues.fromList(concat(l1.asArray().toList.asJava,
                                      ValueUtils.of(l2).asInstanceOf[ListValue].asArray.toList.asJava).asInstanceOf[util.List[AnyValue]])
      )
      case (l1: util.List[_], l2: ListValue, Right(result)) => result should equal(
        VirtualValues.fromList(concat(ValueUtils.of(l1).asInstanceOf[ListValue].asArray.toList.asJava,
                                      l2.asArray().toList.asJava).asInstanceOf[util.List[AnyValue]])
      )
      case (x, l: ListValue, Right(result)) => result should equal(
        VirtualValues.fromList(prepend(ValueUtils.asAnyValue(x), l.asArray().toList.asJava).asInstanceOf[util.List[AnyValue]])
      )
      case (l: ListValue, x, Right(result)) => result should equal(
        VirtualValues.fromList(append(ValueUtils.asAnyValue(x), l.asArray().toList.asJava).asInstanceOf[util.List[AnyValue]])
      )
      case (x, l: util.List[_], Right(result)) => result should equal(prepend(x, l))
      case (l: util.List[_], x, Right(result)) => result should equal(append(x, l))

      // Mix of Neo values and Java values
      case (l: DoubleValue,       r: Number,            Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: Number,            r: DoubleValue,       Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: NumberValue,       r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: java.lang.Double,  r: NumberValue,       Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: FloatValue,        r: Number,            Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: Number,            r: FloatValue,        Right(result)) => result should equal(l.doubleValue() + r.doubleValue())
      case (l: java.lang.Float,   r: NumberValue,       Right(result)) => result should equal(l + r.doubleValue())
      case (l: NumberValue,       r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() + r)
      case (l: NumberValue,       r: Number,            Right(result)) => result should equal(l.longValue() + r.longValue())
      case (l: Number,            r: NumberValue,       Right(result)) => result should equal(l.longValue() + r.longValue())

      case (l: NumberValue,       r: String,            Right(result)) => result should equal(l.prettyPrint() + r)
      case (l: String,            r: NumberValue,       Right(result)) => result should equal(l + r.prettyPrint())
      case (l: TextValue,         r: Number,            Right(result)) => result should equal(l.stringValue() + r.toString)
      case (l: Number,            r: TextValue,         Right(result)) => result should equal(l.toString() + r.stringValue())
      case (l: TextValue,         r: String,            Right(result)) => result should equal(l.stringValue() + r)
      case (l: String,            r: TextValue,         Right(result)) => result should equal(l + r.stringValue())
      case (l: TextValue,         r: java.lang.Boolean, Right(result)) => result should equal(l.stringValue() + r)
      case (l: java.lang.Boolean, r: TextValue,         Right(result)) => result should equal(l + r.stringValue())
      case (l: BooleanValue,      r: String,            Right(result)) =>
        result should equal(l.booleanValue() + r)
      case (l: String,            r: BooleanValue,      Right(result)) =>
        result should equal(l + r.booleanValue())

      case (_: NumberValue,       _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: Number,            _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) =>
        fail(s"Unspecified behaviour: $v1 + $v2 => $v3")
    }
  }

  property("-") {
    forAll(getTable(CompiledMathHelper.subtract)) {
      // Nulls
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (Values.NO_VALUE,      _,                    Right(result)) => result should equal(null)
      case (_,                    Values.NO_VALUE,      Right(result)) => result should equal(null)

      // Java values
      case (l: java.lang.Double,  r: Number,            Right(result)) => result should equal(l - r.doubleValue())
      case (l: Number,            r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() - r)
      case (l: java.lang.Float,   r: Number,            Right(result)) => result should equal(l - r.doubleValue())
      case (l: Number,            r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() - r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() - r.longValue())

      case (l: Number,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      // Neo values
      case (l: DoubleValue,       r: NumberValue,       Right(result)) =>
        java.lang.Double.compare(result.asInstanceOf[NumberValue].doubleValue(), l.doubleValue() - r.doubleValue()) should equal(0)
      case (l: NumberValue,       r: DoubleValue,       Right(result)) =>
        java.lang.Double.doubleToLongBits(result.asInstanceOf[NumberValue].doubleValue()) should equal(
          java.lang.Double.doubleToLongBits(l.doubleValue() - r.doubleValue()))
      case (l: FloatValue,        r: NumberValue,       Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: NumberValue,       r: FloatValue,        Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: NumberValue,       r: NumberValue,       Right(result)) => result should equal(Values.longValue(l.longValue() - r.longValue()))

      case (l: NumberValue,       r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]

      case (l: TemporalValue[_, _], r: DurationValue,       Right(result)) => result should equal(l.minus(r))
      case (l: DurationValue,       r: DurationValue,       Right(result)) => result should equal(l.sub(r))
      case (l: TemporalValue[_, _], _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: TemporalValue[_, _], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: DurationValue,       _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: DurationValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: PointValue,          _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: PointValue,          Left(exception)) => exception shouldBe a [CypherTypeException]

      // Lists
      case (l1: util.List[_], l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue,    l2: ListValue,    Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue, l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: util.List[_], l2: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: ListValue, x, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: util.List[_], x, Left(exception)) => exception shouldBe a [CypherTypeException]

      // Mix of Neo values and Java values
      case (l: DoubleValue,       r: Number,            Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: Number,            r: DoubleValue,       Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: NumberValue,       r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: java.lang.Double,  r: NumberValue,       Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: FloatValue,        r: Number,            Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: Number,            r: FloatValue,        Right(result)) => result should equal(l.doubleValue() - r.doubleValue())
      case (l: java.lang.Float,   r: NumberValue,       Right(result)) => result should equal(l - r.doubleValue())
      case (l: NumberValue,       r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() - r)
      case (l: NumberValue,       r: Number,            Right(result)) => result should equal(l.longValue() - r.longValue())
      case (l: Number,            r: NumberValue,       Right(result)) => result should equal(l.longValue() - r.longValue())

      case (l: NumberValue,       r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: Number,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: Number,            _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) => fail(s"Unspecified behaviour: $v1 - $v2 => $v3")
    }
  }

  property("*") {
    forAll(getTable(CompiledMathHelper.multiply)) {
      // Nulls
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (Values.NO_VALUE,      _,                    Right(result)) => result should equal(null)
      case (_,                    Values.NO_VALUE,      Right(result)) => result should equal(null)

      // Java values
      case (l: java.lang.Double,  r: Number,            Right(result)) => result should equal(l * r.doubleValue())
      case (l: Number,            r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() * r)
      case (l: java.lang.Float,   r: Number,            Right(result)) => result should equal(l * r.doubleValue())
      case (l: Number,            r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() * r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() * r.longValue())

      case (l: Number,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      // Neo values
      case (l: DoubleValue,       r: NumberValue,       Right(result)) =>
        java.lang.Double.compare(result.asInstanceOf[NumberValue].doubleValue(), l.doubleValue() * r.doubleValue()) should equal(0)
      case (l: NumberValue,       r: DoubleValue,       Right(result)) =>
        java.lang.Double.doubleToLongBits(result.asInstanceOf[NumberValue].doubleValue()) should equal(
          java.lang.Double.doubleToLongBits(l.doubleValue() * r.doubleValue()))
      case (l: FloatValue,        r: NumberValue,       Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: NumberValue,       r: FloatValue,        Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: NumberValue,       r: NumberValue,       Right(result)) => result should equal(Values.longValue(l.longValue() * r.longValue()))

      case (l: NumberValue,       r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]

      case (l: DurationValue,       r: NumberValue,     Right(result)) => result should equal(l.mul(r))
      case (l: DurationValue,       r: Number,          Right(result)) => result should equal(l.mul(Values.numberValue(r)))
      case (l: NumberValue,         r: DurationValue,   Right(result)) => result should equal(r.mul(l))
      case (l: Number,              r: DurationValue,   Right(result)) => result should equal(r.mul(Values.numberValue(l)))
      case (l: TemporalValue[_, _], _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: TemporalValue[_, _], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: DurationValue,       _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: DurationValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: PointValue,          _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: PointValue,          Left(exception)) => exception shouldBe a [CypherTypeException]

      // Lists
      case (l1: util.List[_], l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue,    l2: ListValue,    Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue, l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: util.List[_], l2: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: ListValue, x, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: util.List[_], x, Left(exception)) => exception shouldBe a [CypherTypeException]

      // Mix of Neo values and Java values
      case (l: DoubleValue,       r: Number,            Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: Number,            r: DoubleValue,       Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: NumberValue,       r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: java.lang.Double,  r: NumberValue,       Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: FloatValue,        r: Number,            Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: Number,            r: FloatValue,        Right(result)) => result should equal(l.doubleValue() * r.doubleValue())
      case (l: java.lang.Float,   r: NumberValue,       Right(result)) => result should equal(l * r.doubleValue())
      case (l: NumberValue,       r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() * r)
      case (l: NumberValue,       r: Number,            Right(result)) => result should equal(l.longValue() * r.longValue())
      case (l: Number,            r: NumberValue,       Right(result)) => result should equal(l.longValue() * r.longValue())

      case (l: NumberValue,       r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: Number,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: Number,            _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) => fail(s"Unspecified behaviour: $v1 * $v2 => $v3")
    }
  }

  property("/") {
    forAll(getTable(CompiledMathHelper.divide)) {
      // Nulls
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (Values.NO_VALUE,      _,                    Right(result)) => result should equal(null)
      case (_,                    Values.NO_VALUE,      Right(result)) => result should equal(null)

      // Java values
      case (l: java.lang.Double,  r: Number,            Right(result)) => result should equal(l / r.doubleValue())
      case (l: Number,            r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() / r)
      case (l: java.lang.Float,   r: Number,            Right(result)) => result should equal(l / r.doubleValue())
      case (l: Number,            r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() / r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() / r.longValue())

      case (l: Number,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      // Neo values
      case (l: DoubleValue,       r: NumberValue,       Right(result)) =>
        java.lang.Double.compare(result.asInstanceOf[NumberValue].doubleValue(), l.doubleValue() / r.doubleValue()) should equal(0)
      case (l: NumberValue,       r: DoubleValue,       Right(result)) =>
        java.lang.Double.doubleToLongBits(result.asInstanceOf[NumberValue].doubleValue()) should equal(
          java.lang.Double.doubleToLongBits(l.doubleValue() / r.doubleValue()))
      case (l: FloatValue,        r: NumberValue,       Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: NumberValue,       r: FloatValue,        Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: NumberValue,       r: NumberValue,       Right(result)) => result should equal(Values.longValue(l.longValue() / r.longValue()))

      case (l: NumberValue,       r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]

      case (l: DurationValue,       r: NumberValue,     Right(result)) => result should equal(l.div(r))
      case (l: DurationValue,       r: Number,          Right(result)) => result should equal(l.div(Values.numberValue(r)))
      case (l: TemporalValue[_, _], _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: TemporalValue[_, _], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: DurationValue,       _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: DurationValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: PointValue,          _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: PointValue,          Left(exception)) => exception shouldBe a [CypherTypeException]

      // Lists
      case (l1: util.List[_], l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue,    l2: ListValue,    Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue, l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: util.List[_], l2: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: ListValue, x, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: util.List[_], x, Left(exception)) => exception shouldBe a [CypherTypeException]

      // Mix of Neo values and Java values
      case (l: DoubleValue,       r: Number,            Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: Number,            r: DoubleValue,       Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: NumberValue,       r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: java.lang.Double,  r: NumberValue,       Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: FloatValue,        r: Number,            Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: Number,            r: FloatValue,        Right(result)) => result should equal(l.doubleValue() / r.doubleValue())
      case (l: java.lang.Float,   r: NumberValue,       Right(result)) => result should equal(l / r.doubleValue())
      case (l: NumberValue,       r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() / r)
      case (l: NumberValue,       r: Number,            Right(result)) => result should equal(l.longValue() / r.longValue())
      case (l: Number,            r: NumberValue,       Right(result)) => result should equal(l.longValue() / r.longValue())

      case (l: NumberValue,       r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: Number,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: Number,            _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) => fail(s"Unspecified behaviour: $v1 / $v2 => $v3")
    }
  }

  property("%") {
    forAll(getTable(CompiledMathHelper.modulo)) {
      // Nulls
      case (null,                 _,                    Right(result)) => result should equal(null)
      case (_,                    null,                 Right(result)) => result should equal(null)
      case (Values.NO_VALUE,      _,                    Right(result)) => result should equal(null)
      case (_,                    Values.NO_VALUE,      Right(result)) => result should equal(null)

      // Java values
      case (l: java.lang.Double,  r: Number,            Right(result)) => result should equal(l % r.doubleValue())
      case (l: Number,            r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() % r)
      case (l: java.lang.Float,   r: Number,            Right(result)) => result should equal(l % r.doubleValue())
      case (l: Number,            r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() % r)
      case (l: Number,            r: Number,            Right(result)) => result should equal(l.longValue() % r.longValue())

      case (l: Number,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: Number,            _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]

      // Neo values
      // NOTE: We do not produce any Values here as with the other numerical operators
      case (l: DoubleValue,       r: NumberValue,       Right(result)) =>
        java.lang.Double.compare(result.asInstanceOf[Double], l.doubleValue() % r.doubleValue()) should equal(0)
      case (l: NumberValue,       r: DoubleValue,       Right(result)) =>
        java.lang.Double.doubleToLongBits(result.asInstanceOf[Number].doubleValue()) should equal(
          java.lang.Double.doubleToLongBits(l.doubleValue() % r.doubleValue()))
      case (l: FloatValue,        r: NumberValue,       Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: NumberValue,       r: FloatValue,        Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: NumberValue,       r: NumberValue,       Right(result)) =>
        result should equal(l.longValue() % r.longValue())

      case (l: NumberValue,       r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]

      case (l: TemporalValue[_, _], _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: TemporalValue[_, _], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: DurationValue,       _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: DurationValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: PointValue,          _,                      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_,                      _: PointValue,          Left(exception)) => exception shouldBe a [CypherTypeException]

      // Lists
      case (l1: util.List[_], l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue,    l2: ListValue,    Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: ListValue, l2: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l1: util.List[_], l2: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: ListValue, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: ListValue, x, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (x, l: util.List[_], Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: util.List[_], x, Left(exception)) => exception shouldBe a [CypherTypeException]

      // Mix of Neo values and Java values
      case (l: DoubleValue,       r: Number,            Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: Number,            r: DoubleValue,       Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: NumberValue,       r: java.lang.Double,  Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: java.lang.Double,  r: NumberValue,       Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: FloatValue,        r: Number,            Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: Number,            r: FloatValue,        Right(result)) => result should equal(l.doubleValue() % r.doubleValue())
      case (l: java.lang.Float,   r: NumberValue,       Right(result)) => result should equal(l % r.doubleValue())
      case (l: NumberValue,       r: java.lang.Float,   Right(result)) => result should equal(l.doubleValue() % r)
      case (l: NumberValue,       r: Number,            Right(result)) => result should equal(l.longValue() % r.longValue())
      case (l: Number,            r: NumberValue,       Right(result)) => result should equal(l.longValue() % r.longValue())

      case (l: NumberValue,       r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: Number,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: TextValue,         r: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: java.lang.Boolean, r: TextValue,         Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: BooleanValue,      r: String,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (l: String,            r: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (_: NumberValue,       _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: NumberValue,       Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: java.lang.Boolean, Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: java.lang.Boolean, _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: BooleanValue,      _: Number,            Left(exception)) => exception shouldBe a [CypherTypeException]
      case (_: Number,            _: BooleanValue,      Left(exception)) => exception shouldBe a [CypherTypeException]

      case (v1, v2, v3) => fail(s"Unspecified behaviour: $v1 / $v2 => $v3")
    }
  }

  implicit class I(i: Int) { def ! = i: java.lang.Integer }
  implicit class D(i: Double) { def ! = i: java.lang.Double }
  implicit class Z(i: Boolean) { def ! = i: java.lang.Boolean }

  private def getTable(f: (AnyRef, AnyRef) => AnyRef) = {

    val cartesianProduct = (for (x <- values; y <- values) yield {
      (x, y)
    }).distinct

    val valuesWithResults = cartesianProduct.map {
      case (l, r) => (l, r, throwableToLeft(f(l, r)))
    }

    Table(("lhs", "rhs", "result"), valuesWithResults: _*)
  }

  def throwableToLeft[T](block: => T): Either[java.lang.Throwable, T] =
    try {
      Right(block)
    } catch {
      case ex: Throwable => Left(ex)
    }

  val inputs = Table[Any]("Number", 42, 42.1, 42L, 42.3F)
  property("transformToInt") {
    forAll(inputs)(x => CompiledMathHelper.transformToInt(x) should equal(42))
  }

  private def concat(l1: util.List[_], l2: util.List[_]): util.List[Any] = {
    val newList = new util.ArrayList[Any](l1.size() + l2.size())
    newList.addAll(l1)
    newList.addAll(l2)
    newList
  }

  private def prepend(x: Any, l: util.List[_]): util.List[Any] = {
    val newList = new util.ArrayList[Any](l.size() + 1)
    newList.add(x)
    newList.addAll(l)
    newList
  }

  private def append(x: Any, l: util.List[_]): util.List[Any] = {
    val newList = new util.ArrayList[Any](l.size() + 1)
    newList.addAll(l)
    newList.add(x)
    newList
  }

}
