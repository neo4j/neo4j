/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.symbols._
import org.neo4j.graphdb.{RelationshipType => KernelRelType, _}
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}

import java.util.{Map => JavaMap}
import scala.collection.JavaConverters._
import collection.Map
import org.neo4j.cypher.internal.commands._

trait UpdateAction {
  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext]
  def dependencies:Seq[Identifier]
  def identifier:Seq[Identifier]
  def rewrite(f: Expression => Expression):UpdateAction
  def filter(f: Expression => Boolean): Seq[Expression]
}

trait GraphElementPropertyFunctions {
  def setProperties(pc: PropertyContainer, props: Map[String, Expression], context: ExecutionContext, state: QueryState) {
    props.foreach {
      case ("*", expression) => setAllMapKeyValues(expression, context, pc, state)
      case (key, expression) => setSingleValue(expression, context, pc, key, state)
    }
  }

  def propDependencies(props: Map[String, Expression]) = props.values.flatMap(_.dependencies(AnyType())).toSeq.distinct

  def rewrite(props: Map[String, Expression], f: (Expression) => Expression): Map[String, Expression] = props.map{ case (k,v) => k->v.rewrite(f) }

  private def getMapFromExpression(v: Any): Map[String, Any] = v match {
    case m: collection.Map[String, Any] => m.toMap
    case m: JavaMap[String, Any] => m.asScala.toMap
    case x => throw new CypherTypeException("Don't know how to extract parameters from this type: " + x.getClass.getName)
  }

  private def setAllMapKeyValues(expression: Expression, context: ExecutionContext, pc: PropertyContainer, state: QueryState) {
    val map = getMapFromExpression(expression(context))
    map.foreach {
      case (key, value) => {
        pc.setProperty(key, value)
        state.propertySet.increase()
      }
    }
  }

  private def setSingleValue(expression: Expression, context: ExecutionContext, pc: PropertyContainer, key: String, state: QueryState) {
    val value = makeValueNeoSafe(expression(context))
    pc.setProperty(key, value)
    state.propertySet.increase()
  }

  def makeValueNeoSafe(a: Any): Any = if (a.isInstanceOf[Traversable[_]]) {
    transformTraversableToArray(a)
  } else {
    a
  }

  private def transformTraversableToArray(a: Any): Any = {
    val seq = a.asInstanceOf[Traversable[_]].toSeq

    if (seq.size == 0) {
      Array[String]();
    } else try {
      seq.head match {
        case c: String => seq.map(_.asInstanceOf[String]).toArray[String]
        case b: Boolean => seq.map(_.asInstanceOf[Boolean]).toArray[Boolean]
        case b: Byte => seq.map(_.asInstanceOf[Byte]).toArray[Byte]
        case s: Short => seq.map(_.asInstanceOf[Short]).toArray[Short]
        case i: Int => seq.map(_.asInstanceOf[Int]).toArray[Int]
        case l: Long => seq.map(_.asInstanceOf[Long]).toArray[Long]
        case f: Float => seq.map(_.asInstanceOf[Float]).toArray[Float]
        case d: Double => seq.map(_.asInstanceOf[Double]).toArray[Double]
        case c: Char => seq.map(_.asInstanceOf[Char]).toArray[Char]
        case _ => throw new CypherTypeException("Tried to set a property to a collection of mixed types. " + a.toString)
      }
    } catch {
      case e: ClassCastException => throw new CypherTypeException("Collections containing mixed types can not be stored in properties.", e)
    }
  }
}



