/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.value

import org.assertj.core.util.DoubleComparator
import org.eclipse.collections.impl.factory.Bags
import org.eclipse.collections.impl.factory.Maps
import org.neo4j.cypher.cucumber.value.ValueRepresentation.Connection
import org.neo4j.cypher.cucumber.value.ValueRepresentation.NoIdNode
import org.neo4j.cypher.cucumber.value.ValueRepresentation.NoIdPath
import org.neo4j.cypher.cucumber.value.ValueRepresentation.NoIdRel
import org.neo4j.cypher.testing.api.ValueMapper
import org.neo4j.driver
import org.neo4j.driver.Value
import org.neo4j.driver.internal.value.DurationValue
import org.neo4j.driver.internal.value.FloatValue
import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.internal.value.NodeValue
import org.neo4j.driver.internal.value.PathValue
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.internal.value.VectorValue
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.values.storable.DurationValue.duration
import org.neo4j.values.storable.Values

import java.lang
import java.time.temporal.Temporal
import java.time.temporal.TemporalAmount
import java.util
import java.util.function
import java.util.stream.StreamSupport

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

/** Maps result values to work with Cucumber test assertions. */
final object ResultValueMapper extends ValueMapper {

  override def driverRecordsMapper: function.Function[driver.Record, util.List[AnyRef]] = DriverRecordsMapper
  override def mapJavaValue(value: AnyRef): AnyRef = EmbeddedValueMapper.convertEmbeddedValue(value)

  /** Converts driver records. */
  final object DriverRecordsMapper extends function.Function[driver.Record, util.List[AnyRef]] {

    override def apply(record: driver.Record): util.List[AnyRef] = record.values[AnyRef](DriverValueMapper) match {
      case list: java.util.List[_] => list
      case iterable                => StreamSupport.stream(iterable.spliterator(), false).toList
    }
  }

  /** Converts driver values. */
  final object DriverValueMapper extends function.Function[driver.Value, AnyRef] {

    override def apply(value: Value): AnyRef = value match {
      case list: ListValue             => list.asList(this)
      case map: MapValue               => map.asMap(this)
      case nodeValue: NodeValue        => convertNode(nodeValue.asNode())
      case relValue: RelationshipValue => convertRel(relValue.asRelationship())
      case path: PathValue             => convertDriverPath(path.asPath())
      case float: FloatValue           => java.lang.Double.valueOf(float.asDouble() + 0.0)
      case vector: VectorValue         => convertVector(vector)
      case point: PointValue           => convertPoint(point)
      case durationValue: DurationValue => // Yes, durations are treated as strings here
        val d = durationValue.asIsoDuration()
        duration(d.months(), d.days(), d.seconds(), d.nanoseconds()).toString
      case _ => value.asObject() match {
          case uuid: java.util.UUID => Values.uuidValue(uuid)
          case temporal: Temporal   => temporal.toString // Yes, temporals are treated as strings here
          case other                => other
        }
    }

    private def convertNode(n: org.neo4j.driver.types.Node): NoIdNode = {
      val labels = java.util.HashSet.newHashSet[String](4)
      n.labels().forEach(l => labels.add(l))
      NoIdNode(labels, n.asMap(DriverValueMapper))
    }

    private def convertRel(r: org.neo4j.driver.types.Relationship): NoIdRel = {
      NoIdRel(r.`type`(), r.asMap(DriverValueMapper))
    }

    private def convertDriverPath(p: org.neo4j.driver.types.Path): NoIdPath = {
      val connections = p.iterator().asScala
        .map { s =>
          val isOutgoing = s.relationship().endNodeElementId() == s.end().elementId()
          ValueRepresentation.Connection(convertRel(s.relationship()), convertNode(s.end()), isOutgoing)
        }
      NoIdPath(convertNode(p.start()), connections.toSeq)
    }

    private def convertVector(vector: VectorValue): org.neo4j.values.storable.Value = {
      val vec = vector.asBoltVector()
      vec.elementType().toString match {
        case "byte" =>
          val coordinateArray: Array[Byte] = vec.elements().asInstanceOf[Array[Byte]]
          Values.int8Vector(coordinateArray: _*)
        case "short" =>
          val coordinateArray: Array[Short] = vec.elements().asInstanceOf[Array[Short]]
          Values.int16Vector(coordinateArray: _*)
        case "int" =>
          val coordinateArray: Array[Int] = vec.elements().asInstanceOf[Array[Int]]
          Values.int32Vector(coordinateArray: _*)
        case "long" =>
          val coordinateArray: Array[Long] = vec.elements().asInstanceOf[Array[Long]]
          Values.int64Vector(coordinateArray: _*)
        case "float" =>
          val coordinateArray: Array[Float] = vec.elements().asInstanceOf[Array[Float]]
          Values.float32Vector(coordinateArray: _*)
        case "double" =>
          val coordinateArray: Array[Double] = vec.elements().asInstanceOf[Array[Double]]
          Values.float64Vector(coordinateArray: _*)
      }
    }

    private def convertPoint(point: PointValue): org.neo4j.values.storable.Value = {
      val p = point.asPoint()
      val crs = org.neo4j.values.storable.CoordinateReferenceSystem.get(p.srid())
      val coordinates =
        if (java.lang.Double.isNaN(p.z())) Array(p.x(), p.y())
        else Array(p.x(), p.y(), p.z())
      Values.pointValue(crs, coordinates: _*)
    }
  }

  /** Converts embedded API values. */
  final object EmbeddedValueMapper {

    @tailrec
    def convertEmbeddedValue(value: AnyRef): AnyRef = value match {
      case string: java.lang.String => string
      case v: java.lang.Short       => java.lang.Long.valueOf(v.shortValue())
      case v: java.lang.Integer     => java.lang.Long.valueOf(v.longValue())
      case v: java.lang.Byte        => java.lang.Long.valueOf(v.longValue())
      case v: java.lang.Long        => v
      case double: java.lang.Double => java.lang.Double.valueOf(double.doubleValue() + 0.0) // + 0.0 to avoid -0.0
      case float: java.lang.Float   => java.lang.Double.valueOf(float.doubleValue() + 0.0)
      case list: util.List[_]       => convertList(list)
      case map: util.Map[_, _]      => convertMap(map)
      case n: Node                  => convertNode(n)
      case r: Relationship          => convertRel(r)
      case p: Path                  => convertPath(p)
      case array: Array[AnyRef]     => convertList(java.util.Arrays.asList(array: _*))
      case array: Array[_]          => convertEmbeddedValue(array.map(_.asInstanceOf[AnyRef])) // force boxed values
      case temporal: Temporal       => temporal.toString // Yes, temporals are treated as strings here
      case temporalAmount: TemporalAmount => temporalAmount.toString // Yes, also temporal amounts
      case uuid: java.util.UUID           => Values.uuidValue(uuid)
      case value                          => value
    }

    private def convertMap(map: java.util.Map[_, _]): java.util.Map[String, AnyRef] = {
      if (needsConversion(map)) {
        val result = java.util.HashMap.newHashMap[String, AnyRef](map.size())
        map.forEach((k, v) => result.put(k.asInstanceOf[String], convertEmbeddedValue(v.asInstanceOf[AnyRef])))
        result
      } else {
        map.asInstanceOf[java.util.Map[String, AnyRef]]
      }
    }

    private def convertList(list: java.util.List[_]): java.util.List[_] = {
      if (needsConversion(list)) {
        val result = new java.util.ArrayList[AnyRef](list.size())
        list.forEach(v => result.add(convertEmbeddedValue(v.asInstanceOf[AnyRef])))
        result
      } else {
        list
      }
    }

    private def convertNode(n: Node): NoIdNode = {
      val labels = java.util.HashSet.newHashSet[String](4)
      n.getLabels.forEach(l => labels.add(l.name()))
      NoIdNode(labels, convertMap(n.getAllProperties))
    }

    private def convertRel(r: Relationship): NoIdRel = {
      NoIdRel(r.getType.name(), convertMap(r.getAllProperties))
    }

    private def convertPath(p: Path): NoIdPath = {
      val connections = p.iterator().asScala.drop(1).grouped(2).map {
        case Seq(r: Relationship, n: Node) =>
          Connection(convertRel(r), convertNode(n), r.getEndNode.getElementId == n.getElementId)
        case other =>
          throw new IllegalArgumentException(s"Unexpected path: $other:\n$p")
      }
      NoIdPath(convertNode(p.startNode()), connections.toSeq)
    }

    private def needsConversion(value: AnyRef): Boolean = value match {
      case map: util.Map[_, _] => map.values().stream().anyMatch(v => needsConversion(v.asInstanceOf[AnyRef]))
      case list: util.List[_]  => list.stream().anyMatch(v => needsConversion(v.asInstanceOf[AnyRef]))
      case _: Entity | _: Path | _: java.lang.Integer | _: java.lang.Float | _: java.lang.Short | _: java.lang.Byte |
        _: Array[
          _
        ] | _: Temporal | _: TemporalAmount | _: java.util.UUID => true
      case _ => false
    }
  }

  object RowMapper {

    def mapRows(
      rows: util.List[util.List[AnyRef]],
      mapper: java.util.function.Function[_ >: AnyRef, _ <: AnyRef]
    ): util.List[util.List[AnyRef]] = {
      rows.stream().map[java.util.List[AnyRef]](row => row.stream().map[AnyRef](mapper).toList).toList
    }
  }

  object CloseEnoughNumbersList {

    def rowsWithCloseEnoughNumbers(epsilon: Double)(
      rows: util.List[util.List[AnyRef]]
    ): util.List[util.List[AnyRef]] = {
      RowMapper.mapRows(rows, withCloseEnoughNumbers(_, epsilon))
    }

    private def withCloseEnoughNumbers(value: AnyRef, epsilon: Double): AnyRef = value match {
      case double: lang.Double =>
        new CloseEnoughDouble(double, epsilon)
      case float: lang.Float =>
        new CloseEnoughDouble(float.doubleValue(), epsilon)
      case list: util.List[_] =>
        list.stream().map(v => withCloseEnoughNumbers(v.asInstanceOf[AnyRef], epsilon)).toList
      case map: util.Map[_, _] =>
        val result = Maps.mutable.ofInitialCapacity[String, AnyRef](map.size())
        map.entrySet().forEach { e =>
          val newValue = withCloseEnoughNumbers(e.getValue.asInstanceOf[AnyRef], epsilon)
          result.put(e.getKey.asInstanceOf[String], newValue)
        }
        result
      case node: NoIdNode => nodeWithCloseEnoughNumbers(node, epsilon)
      case rel: NoIdRel   => relWithCloseEnoughNumbers(rel, epsilon)
      case path: NoIdPath =>
        val newStart = nodeWithCloseEnoughNumbers(path.start, epsilon)
        val newConnections = path.connections
          .map(c =>
            Connection(
              relWithCloseEnoughNumbers(c.rel, epsilon),
              nodeWithCloseEnoughNumbers(c.node, epsilon),
              c.outgoing
            )
          )
        NoIdPath(newStart, newConnections)
      case _ => value
    }

    private def nodeWithCloseEnoughNumbers(node: NoIdNode, epsilon: Double): NoIdNode = {
      val newProps = withCloseEnoughNumbers(node.properties, epsilon).asInstanceOf[java.util.Map[String, AnyRef]]
      node.copy(properties = newProps)
    }

    private def relWithCloseEnoughNumbers(rel: NoIdRel, epsilon: Double): NoIdRel = {
      val newProps = withCloseEnoughNumbers(rel.properties, epsilon).asInstanceOf[java.util.Map[String, AnyRef]]
      rel.copy(properties = newProps)
    }

    private class CloseEnoughDouble(val value: lang.Double, epsilon: Double) {
      private val comparator = new DoubleComparator(epsilon)

      override def equals(obj: Any): Boolean = obj match {
        case otherNumber: CloseEnoughDouble =>
          comparator.compare(value, otherNumber.value) == 0
        case _ => false
      }

      override def hashCode(): Int = value.hashCode()
      override def toString: String = value.toString
    }
  }

  object UnorderedList {

    def rowsWithUnorderedLists(rows: util.List[util.List[AnyRef]]): util.List[util.List[AnyRef]] = {
      RowMapper.mapRows(rows, withUnorderedLists)
    }

    private def withUnorderedLists(value: AnyRef): AnyRef = value match {
      case list: util.List[_] =>
        new UnorderedList(list.stream().map(v => withUnorderedLists(v.asInstanceOf[AnyRef])).toList)
      case map: util.Map[_, _] =>
        val result = Maps.mutable.ofInitialCapacity[String, AnyRef](map.size())
        map.entrySet().forEach { e =>
          result.put(e.getKey.asInstanceOf[String], withUnorderedLists(e.getValue.asInstanceOf[AnyRef]))
        }
        result
      case node: NoIdNode =>
        node.copy(properties = withUnorderedLists(node.properties).asInstanceOf[java.util.Map[String, AnyRef]])
      case rel: NoIdRel =>
        rel.copy(properties = withUnorderedLists(rel.properties).asInstanceOf[java.util.Map[String, AnyRef]])
      case _ => value
    }

  }

  class UnorderedList[T](private val inner: util.List[T]) {
    private lazy val itemBag = Bags.immutable.ofAll(inner)

    override def equals(obj: Any): Boolean = obj match {
      case other: UnorderedList[_] => inner == other.inner || itemBag == other.itemBag
      case _                       => false
    }

    override def hashCode(): Int = itemBag.hashCode()
    override def toString: String = inner.toString
  }
}
