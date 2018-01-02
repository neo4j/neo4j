/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.JavaConverters._
import scala.collection.Map

trait GraphElementPropertyFunctions extends CollectionSupport {

  implicit class RichMap(m: Map[String, Expression]) {
    def rewrite(f: (Expression) => Expression): Map[String, Expression] = m.map {
      case (k, v) => k -> v.rewrite(f)
    }

    def symboltableDependencies: Set[String] = m.values.flatMap(_.symbolTableDependencies).toSet
  }

  def setProperties(pc: PropertyContainer, props: Map[String, Expression], context: ExecutionContext, state: QueryState) {
    props.foreach {
      case ("*", expression) => setAllMapKeyValues(expression, context, pc, state)
      case (key, expression) => setSingleValue(expression, context, pc, key, state)
    }
  }

  def toString(m:Map[String,Expression]):String = m.map {
    case (k, e) => "%s: %s".format(k, e.toString)
  }.mkString("{", ", ", "}")

  def getMapFromExpression(v: Any): Map[String, Any] = {
    v match {
      case _: collection.Map[_, _] => v.asInstanceOf[collection.Map[String, Any]].toMap
      case _: JavaMap[_, _]        => v.asInstanceOf[JavaMap[String, Any]].asScala.toMap
      case _                       => throw new CypherTypeException(s"Don't know how to extract parameters from this type: ${v.getClass.getName}")
    }
  }

  private def setAllMapKeyValues(expression: Expression, context: ExecutionContext, pc: PropertyContainer, state: QueryState) {
    val map = getMapFromExpression(expression(context)(state))

    pc match {
      case n: Node => map.foreach {
        case (key, value) =>
          state.query.nodeOps.setProperty(n.getId, state.query.getOrCreatePropertyKeyId(key), makeValueNeoSafe(value))
      }

      case r: Relationship => map.foreach {
        case (key, value) =>
          state.query.relationshipOps.setProperty(r.getId, state.query.getOrCreatePropertyKeyId(key), makeValueNeoSafe(value))
      }
    }
  }

  private def setSingleValue(expression: Expression, context: ExecutionContext, pc: PropertyContainer, key: String, state: QueryState) {
    val unsafeValue: Any = expression(context)(state)
    if (unsafeValue != null) {
      val value = makeValueNeoSafe(unsafeValue)
      pc match {
        case n: Node  =>
          state.query.nodeOps.setProperty(n.getId, state.query.getOrCreatePropertyKeyId(key), value)

        case r: Relationship =>
          state.query.relationshipOps.setProperty(r.getId, state.query.getOrCreatePropertyKeyId(key), value)
      }
    }
  }

  def makeValueNeoSafe(a: Any): Any = if (isCollection(a)) {
    transformTraversableToArray(makeTraversable(a))
  } else {
    a
  }


  private def transformTraversableToArray(a: Any): Any = {
    val seq: Seq[Any] = a.asInstanceOf[Traversable[_]].toSeq

    if (seq.size == 0) {
      Array[String]()
    } else {
      val typeValue = seq.reduce(CastSupport.merge)
      val converter = CastSupport.getConverter(typeValue)

      converter.arrayConverter(seq.map(converter.valueConverter))
    }
  }
}
