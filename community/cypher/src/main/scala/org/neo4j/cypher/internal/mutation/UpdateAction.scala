/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cypher.internal.pipes.{QueryState}

import java.util.{Map => JavaMap}
import scala.collection.JavaConverters._
import collection.Map
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.graphdb.{Node, Relationship, PropertyContainer}
import org.neo4j.cypher.internal.commands.AstNode
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.values.LabelValue

trait UpdateAction extends TypeSafe with AstNode[UpdateAction] {
  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext]

  def throwIfSymbolsMissing(symbols: SymbolTable)

  def identifiers: Seq[(String, CypherType)]

  def rewrite(f: Expression => Expression): UpdateAction
}

trait GraphElementPropertyFunctions extends CollectionSupport {

  implicit class RichMap(m:Map[String, Expression]) {
    def rewrite(f: (Expression) => Expression): Map[String, Expression] = m.map {
      case (k, v) => k -> v.rewrite(f)
    }

    def throwIfSymbolsMissing(symbols: SymbolTable) {
      m.values.foreach(_.throwIfSymbolsMissing(symbols))
    }

    def symboltableDependencies: Set[String] = m.values.flatMap(_.symbolTableDependencies).toSet
  }

  def setProperties(pc: PropertyContainer, props: Map[String, Expression], context: ExecutionContext, state: QueryState) {
    props.foreach {
      case ("*", expression) => setAllMapKeyValues(expression, context, pc, state)
      case (key, expression) => setSingleValue(expression, context, pc, key, state)
    }
  }

  def getMapFromExpression(v: Any): Map[String, Any] = {
    if (v.isInstanceOf[collection.Map[_, _]])
      v.asInstanceOf[collection.Map[String, Any]].toMap
    else if (v.isInstanceOf[JavaMap[_, _]])
      v.asInstanceOf[JavaMap[String, Any]].asScala.toMap
    else
      throw new CypherTypeException(s"Don't know how to extract parameters from this type: ${v.getClass.getName}")
  }

  private def setAllMapKeyValues(expression: Expression, context: ExecutionContext, pc: PropertyContainer, state: QueryState) {
    val map = getMapFromExpression(expression(context)(state))

    pc match {
      case n: Node => map.foreach {
        case (key, value) =>
          state.query.nodeOps.setProperty(n, key, value)
      }

      case r: Relationship => map.foreach {
        case (key, value) =>
          state.query.relationshipOps.setProperty(r, key, value)
      }
    }
  }

  private def setSingleValue(expression: Expression, context: ExecutionContext, pc: PropertyContainer, key: String, state: QueryState) {
    val value = makeValueNeoSafe(expression(context)(state))
    pc match {
      case n: Node =>
        state.query.nodeOps.setProperty(n, key, value)

      case r: Relationship =>
        state.query.relationshipOps.setProperty(r, key, value)
    }
  }

  def makeValueNeoSafe(a: Any): Any = if (isCollection(a)) {
    transformTraversableToArray(makeTraversable(a))
  } else {
    a
  }

  private def transformTraversableToArray(a: Any): Any = {
    val seq = a.asInstanceOf[Traversable[_]].toSeq

    if (seq.size == 0) {
      Array[String]()
    } else try {
      seq.head match {
        case c: String  => seq.map(_.asInstanceOf[String]).toArray[String]
        case b: Boolean => seq.map(_.asInstanceOf[Boolean]).toArray[Boolean]
        case b: Byte    => seq.map(_.asInstanceOf[Byte]).toArray[Byte]
        case s: Short   => seq.map(_.asInstanceOf[Short]).toArray[Short]
        case i: Int     => seq.map(_.asInstanceOf[Int]).toArray[Int]
        case l: Long    => seq.map(_.asInstanceOf[Long]).toArray[Long]
        case f: Float   => seq.map(_.asInstanceOf[Float]).toArray[Float]
        case d: Double  => seq.map(_.asInstanceOf[Double]).toArray[Double]
        case c: Char    => seq.map(_.asInstanceOf[Char]).toArray[Char]
        case _          => throw new CypherTypeException("Tried to set a property to a collection of mixed types. " + a.toString)
      }
    } catch {
      case e: ClassCastException => throw new CypherTypeException("Collections containing mixed types can not be stored in properties.", e)
    }
  }
}



