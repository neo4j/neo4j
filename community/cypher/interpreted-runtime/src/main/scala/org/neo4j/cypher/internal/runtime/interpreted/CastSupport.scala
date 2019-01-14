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
package org.neo4j.cypher.internal.runtime.interpreted

import java.time._
import java.time.temporal.TemporalAmount

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.storable.{ArrayValue, _}
import org.neo4j.values.virtual._
import org.neo4j.values.{AnyValue, AnyValueWriter}

import org.neo4j.kernel.internal.Version

import scala.reflect.ClassTag

object CastSupport {

  // TODO Switch to using ClassTag once we decide to depend on the reflection api

  /**
    * Filter sequence by type
    */
  def sift[A: ClassTag](seq: Seq[Any]): Seq[A] = seq.collect(erasureCast)

  /**
    * Casts input to A if possible according to type erasure, discards input otherwise
    */
  def erasureCast[A: ClassTag]: PartialFunction[Any, A] = {
    case value: A => value
  }

  def castOrFail[A](value: Any)(implicit ev: ClassTag[A]): A = value match {
    case v: A => v
    case _ => throw new CypherTypeException(
      s"Expected $value to be a ${ev.runtimeClass.getName}, but it was a ${value.getClass.getName}")
  }

  def castOrFail[A <: AnyValue](value: AnyValue)(implicit ev: ClassTag[A]): A = value match {
    case v: A => v
    case _ => throw typeError[A](value)
  }

  def typeError[A <: AnyValue](value: AnyValue)(implicit ev: ClassTag[A]): CypherTypeException =
    throw new CypherTypeException(
      s"Expected $value to be a ${ev.runtimeClass.getName}, but it was a ${value.getClass.getName}")

  /*
  This method takes two values and finds the type both values could be represented in.

  The produced value is strictly meant to be used as a type carrier
   */
  def merge(a: AnyValue, b: AnyValue): AnyValue = {
    (a, b) match {
      case (_: TextValue, _: TextValue) => a
      case (_: BooleanValue, _: BooleanValue) => a

      case (_: ByteValue, _: ByteValue) => a
      case (_: ByteValue, _: ShortValue) => b
      case (_: ByteValue, _: IntValue) => b
      case (_: ByteValue, _: LongValue) => b
      case (_: ByteValue, _: FloatValue) => b
      case (_: ByteValue, _: DoubleValue) => b

      case (_: ShortValue, _: IntValue) => b
      case (_: ShortValue, _: LongValue) => b
      case (_: ShortValue, _: FloatValue) => b
      case (_: ShortValue, _: DoubleValue) => b
      case (_: ShortValue, _: NumberValue) => a

      case (_: IntValue, _: LongValue) => b
      case (_: IntValue, _: FloatValue) => b
      case (_: IntValue, _: DoubleValue) => b
      case (_: IntValue, _: NumberValue) => a

      case (_: LongValue, _: FloatValue) => b
      case (_: LongValue, _: DoubleValue) => b
      case (_: LongValue, _: NumberValue) => a

      case (_: FloatValue, _: DoubleValue) => b
      case (_: FloatValue, _: NumberValue) => a
      case (_: DoubleValue, _: NumberValue) => a

      case (p1: PointValue, p2: PointValue) =>
        if (p1.getCoordinateReferenceSystem != p2.getCoordinateReferenceSystem) {
          throw new CypherTypeException("Collections containing point values with different CRS can not be stored in properties.");
        } else if(p1.coordinate().length != p2.coordinate().length) {
          throw new CypherTypeException("Collections containing point values with different dimensions can not be stored in properties.");
        } else {
          p1
        }

      case (_:DateValue, _:DateValue) => a
      case (_:DateTimeValue, _:DateTimeValue) => a
      case (_:LocalDateTimeValue, _:LocalDateTimeValue) => a
      case (_:TimeValue, _:TimeValue) => a
      case (_:LocalTimeValue, _:LocalTimeValue) => a
      case (_:DurationValue, _:DurationValue) => a

      case (a, b) if a == Values.NO_VALUE || b == Values.NO_VALUE => throw new CypherTypeException(
        "Collections containing null values can not be stored in properties.")

      case (a, b) if a.isInstanceOf[ListValue] || b.isInstanceOf[ListValue] => throw new CypherTypeException(
        "Collections containing collections can not be stored in properties.")

      case _ => throw new CypherTypeException("Neo4j only supports a subset of Cypher types for storage as singleton or array properties. " +
        "Please refer to section cypher/syntax/values of the manual for more details.")

    }
  }

  /*Instances of this class can be gotten from @CastSupport.getConverter*/
  sealed case class Converter(arrayConverter: ListValue => ArrayValue)

  /*Returns a converter given a type value*/
  def getConverter(x: AnyValue): Converter = x match {
    case _: CharValue => Converter(
      transform(new ArrayConverterWriter(classOf[Char], a => Values.charArray(a.asInstanceOf[Array[Char]]))))
    case _: TextValue => Converter(
      transform(new ArrayConverterWriter(classOf[String], a => Values.stringArray(a.asInstanceOf[Array[String]]: _*))))
    case _: BooleanValue => Converter(
      transform(new ArrayConverterWriter(classOf[Boolean], a => Values.booleanArray(a.asInstanceOf[Array[Boolean]]))))
    case _: ByteValue => Converter(
      transform(new ArrayConverterWriter(classOf[Byte], a => Values.byteArray(a.asInstanceOf[Array[Byte]]))))
    case _: ShortValue => Converter(
      transform(new ArrayConverterWriter(classOf[Short], a => Values.shortArray(a.asInstanceOf[Array[Short]]))))
    case _: IntValue => Converter(
      transform(new ArrayConverterWriter(classOf[Int], a => Values.intArray(a.asInstanceOf[Array[Int]]))))
    case _: LongValue => Converter(
      transform(new ArrayConverterWriter(classOf[Long], a => Values.longArray(a.asInstanceOf[Array[Long]]))))
    case _: FloatValue => Converter(
      transform(new ArrayConverterWriter(classOf[Float], a => Values.floatArray(a.asInstanceOf[Array[Float]]))))
    case _: DoubleValue => Converter(
      transform(new ArrayConverterWriter(classOf[Double], a => Values.doubleArray(a.asInstanceOf[Array[Double]]))))
    case _: PointValue => Converter(
      transform(new ArrayConverterWriter(classOf[PointValue], a => Values.pointArray(a.asInstanceOf[Array[PointValue]]))))
    case _: Point => Converter(
      transform(new ArrayConverterWriter(classOf[Point], a => Values.pointArray(a.asInstanceOf[Array[Point]]))))
    case _: DurationValue => Converter(
      transform(new ArrayConverterWriter(classOf[TemporalAmount], a => Values.durationArray(a.asInstanceOf[Array[TemporalAmount]]))))
    case _: DateTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[ZonedDateTime], a => Values.dateTimeArray(a.asInstanceOf[Array[ZonedDateTime]]))))
    case _: DateValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalDate], a => Values.dateArray(a.asInstanceOf[Array[LocalDate]]))))
    case _: LocalTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalTime], a => Values.localTimeArray(a.asInstanceOf[Array[LocalTime]]))))
    case _: TimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[OffsetTime], a => Values.timeArray(a.asInstanceOf[Array[OffsetTime]]))))
    case _: LocalDateTimeValue => Converter(
      transform(new ArrayConverterWriter(classOf[LocalDateTime], a => Values.localDateTimeArray(a.asInstanceOf[Array[LocalDateTime]]))))
    case _ => throw new CypherTypeException("Property values can only be of primitive types or arrays thereof")
  }

  private def transform(writer: ArrayConverterWriter)(value: ListValue): ArrayValue = {
    value.writeTo(writer)
    writer.array
  }

  private class ArrayConverterWriter(typ: Class[_], transformer: (AnyRef) => ArrayValue)
    extends AnyValueWriter[RuntimeException] {

    private var _array: AnyRef = null
    private var index = 0

    private def fail() = throw new CypherTypeException(
      "Property values can only be of primitive types or arrays thereof")

    private def write(value: Any) = {
      java.lang.reflect.Array.set(_array, index, value)
      index += 1
    }

    def array: ArrayValue = {
      assert(_array != null)
      transformer(_array)
    }

    override def writeNodeReference(nodeId: Long): Unit = fail()

    override def writeNode(nodeId: Long, labels: TextArray,
                           properties: MapValue): Unit = fail()

    override def writeRelationshipReference(relId: Long): Unit = fail()

    override def writeRelationship(relId: Long, startNodeId: Long, endNodeId: Long, `type`: TextValue,
                                   properties: MapValue): Unit = fail()

    override def beginMap(size: Int): Unit = fail()

    override def endMap(): Unit = fail()

    override def beginList(size: Int): Unit = _array = java.lang.reflect.Array.newInstance(typ, size)

    override def endList(): Unit = {}

    override def writePath(nodes: Array[NodeValue],
                           relationships: Array[RelationshipValue]): Unit = fail()

    override def writeNull(): Unit = fail()

    override def writeBoolean(value: Boolean): Unit = write(value)

    override def writeInteger(value: Byte): Unit = write(value)

    override def writeInteger(value: Short): Unit = write(value)

    override def writeInteger(value: Int): Unit = write(value)

    override def writeInteger(value: Long): Unit = write(value)

    override def writeFloatingPoint(value: Float): Unit = write(value)

    override def writeFloatingPoint(value: Double): Unit = write(value)

    override def writeString(value: String): Unit = write(value)

    override def writeString(value: Char): Unit = write(value)

    override def beginArray(size: Int, arrayType: ValueWriter.ArrayType): Unit = fail()

    override def endArray(): Unit = fail()

    override def writeByteArray(value: Array[Byte]): Unit = {
      _array = value
    }

    override def writePoint(crs: CoordinateReferenceSystem, coordinate: Array[Double]): Unit =
      write(Values.pointValue(crs, coordinate: _*))

    override def writeDuration(months: Long, days: Long, seconds: Long, nanos: Int): Unit =
      write(DurationValue.duration(months, days, seconds, nanos))

    override def writeDate(localDate: LocalDate): Unit =
      write(localDate)

    override def writeLocalTime(localTime: LocalTime): Unit =
      write(localTime)

    override def writeTime(offsetTime: OffsetTime): Unit =
      write(offsetTime)

    override def writeLocalDateTime(localDateTime: LocalDateTime): Unit =
      write(localDateTime)

    override def writeDateTime(zonedDateTime: ZonedDateTime): Unit =
      write(zonedDateTime)
  }

}
