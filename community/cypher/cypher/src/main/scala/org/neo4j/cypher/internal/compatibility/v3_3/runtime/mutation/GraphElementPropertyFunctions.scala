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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation

import java.util.function.BiConsumer

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ListSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InvalidArgumentException}
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import scala.collection.Map

trait GraphElementPropertyFunctions extends ListSupport {

  implicit class RichMap(m: Map[String, Expression]) {

    def rewrite(f: (Expression) => Expression): Map[String, Expression] = m.map {
      case (k, v) => k -> v.rewrite(f)
    }

    def symboltableDependencies: Set[String] = m.values.flatMap(_.symbolTableDependencies).toSet
  }

  def setProperties(pc: PropertyContainer, props: Map[String, Expression], context: ExecutionContext,
                    state: QueryState) {
    props.foreach {
      case ("*", expression) => setAllMapKeyValues(expression, context, pc, state)
      case (key, expression) => setSingleValue(expression, context, pc, key, state)
    }
  }

  def toString(m: Map[String, Expression]): String = m.map {
    case (k, e) => "%s: %s".format(k, e.toString)
  }.mkString("{", ", ", "}")

  def getMapFromExpression(v: AnyValue): MapValue = {
    v match {
      case null => throw new InvalidArgumentException("Property map expression is null")
      case x if x == Values.NO_VALUE => throw new InvalidArgumentException("Property map expression is null")
      case m: MapValue => m
      case _ => throw new CypherTypeException(
        s"Don't know how to extract parameters from this type: ${v.getClass.getName}")
    }
  }

  private def setAllMapKeyValues(expression: Expression, context: ExecutionContext, pc: PropertyContainer,
                                 state: QueryState) {
    val map = getMapFromExpression(expression(context, state))

    pc match {
      case n: Node => map.foreach(new BiConsumer[String, AnyValue] {
        override def accept(key: String, value: AnyValue): Unit =
          state.query.nodeOps.setProperty(n.getId, state.query.getOrCreatePropertyKeyId(key), makeValueNeoSafe(value))
      })

      case r: Relationship => map.foreach(new BiConsumer[String, AnyValue] {
        override def accept(key: String, value: AnyValue): Unit =
          state.query.relationshipOps.setProperty(r.getId, state.query.getOrCreatePropertyKeyId(key), makeValueNeoSafe(value))
      })
    }
  }

  private def setSingleValue(expression: Expression, context: ExecutionContext, pc: PropertyContainer, key: String,
                             state: QueryState) {
    val unsafeValue: AnyValue = expression(context, state)
    if (unsafeValue != Values.NO_VALUE) {
      val value = makeValueNeoSafe(unsafeValue)
      pc match {
        case n: Node =>
          state.query.nodeOps.setProperty(n.getId, state.query.getOrCreatePropertyKeyId(key), value)

        case r: Relationship =>
          state.query.relationshipOps.setProperty(r.getId, state.query.getOrCreatePropertyKeyId(key), value)
      }
    }
  }
}


