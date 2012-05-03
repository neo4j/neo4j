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
import org.neo4j.cypher.internal.commands.{Literal, IterableSupport, Property, Expression}

abstract class UpdateAction {
  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext]

  def dependencies: Seq[Identifier]

  def influenceSymbolTable(symbols: SymbolTable): SymbolTable

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

trait GraphElementPropertyFunctions extends UpdateAction {
  def setProps(pc: PropertyContainer, props: Map[String, Expression], context: ExecutionContext, state: QueryState) {
    props.foreach {
      case ("*", expression) => setAllMapKeyValues(expression, context, pc, state)
      case (key, expression) => setSingleValue(expression, context, pc, key, state)
    }
  }

  def propDependencies(props: Map[String, Expression]) = props.values.flatMap(_.dependencies(AnyType())).toSeq.distinct

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
}

case class CreateNodeAction(key: String, props: Map[String, Expression], db: GraphDatabaseService)
  extends UpdateAction
  with GraphElementPropertyFunctions
  with IterableSupport {
  def exec(context: ExecutionContext, state: QueryState) =
    if (props.size == 1 && props.head._1 == "*") {
      makeTraversable(props.head._2(context)).map(x => {
        val m: Map[String, Expression] = x.asInstanceOf[Map[String, Any]].map {
          case (k, v) => (k -> Literal(v))
        }
        val node = db.createNode()
        state.createdNodes.increase()
        setProps(node, m, context, state)
        context.copy(m = context.m ++ Map(key -> node))
      })
    } else {
      val node = db.createNode()
      state.createdNodes.increase()
      setProps(node, props, context, state)
      context.put(key, node)

      Stream(context)
    }

  def dependencies = propDependencies(props)

  def influenceSymbolTable(symbols: SymbolTable) = symbols.add(Identifier(key, NodeType()))
}

case class CreateRelationshipAction(key: String, from: Expression, to: Expression, typ: String, props: Map[String, Expression])
  extends UpdateAction with GraphElementPropertyFunctions {

  private lazy val relType = DynamicRelationshipType.withName(typ)

  def exec(context: ExecutionContext, state: QueryState) = {
    val f = from(context).asInstanceOf[Node]
    val t = to(context).asInstanceOf[Node]
    val relationship = f.createRelationshipTo(t, relType)
    state.createdRelationships.increase()
    setProps(relationship, props, context, state)
    context.put(key, relationship)
    Stream(context)
  }

  def dependencies = from.dependencies(NodeType()) ++ to.dependencies(NodeType()) ++ propDependencies(props)

  def influenceSymbolTable(symbols: SymbolTable) = symbols.add(Identifier(key, RelationshipType()))

}

case class DeleteEntityAction(elementToDelete: Expression)
  extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState) = {
    elementToDelete(context) match {
      case n: Node => {
        state.deletedNodes.increase()
        n.delete()
      }
      case r: Relationship => {
        state.deletedRelationships.increase()
        r.delete()
      }
      case x => throw new CypherTypeException("Expression `" + elementToDelete.toString() + "` yielded `" + x.toString + "`. Don't know how to delete that.")
    }

    Stream(context)
  }

  def dependencies = elementToDelete.dependencies(MapType())

  def influenceSymbolTable(symbols: SymbolTable) = symbols
}

case class DeletePropertyAction(element: Expression, property: String)
  extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState) = {
    val entity = element(context).asInstanceOf[PropertyContainer]
    if (entity.hasProperty(property)) {
      entity.removeProperty(property)
      state.propertySet.increase()
    }

    Stream(context)
  }

  def dependencies = element.dependencies(MapType())

  def influenceSymbolTable(symbols: SymbolTable) = symbols
}

case class PropertySetAction(prop: Property, e: Expression)
  extends UpdateAction {
  val Property(entityKey, propertyKey) = prop

  def dependencies = e.dependencies(AnyType())

  def exec(context: ExecutionContext, state: QueryState) = {
    val value = makeValueNeoSafe(e(context))
    val entity = context(entityKey).asInstanceOf[PropertyContainer]

    value match {
      case null => entity.removeProperty(propertyKey)
      case _ => entity.setProperty(propertyKey, value)
    }

    state.propertySet.increase()

    Stream(context)
  }

  def influenceSymbolTable(symbols: SymbolTable) = symbols
}

case class ForeachAction(iterable: Expression, symbol: String, actions: Seq[UpdateAction])
  extends UpdateAction
  with IterableSupport {
  def dependencies = iterable.dependencies(AnyIterableType()) ++ actions.flatMap(_.dependencies).filterNot(_.name == symbol)

  def exec(context: ExecutionContext, state: QueryState) = {
    val before = context.get(symbol)

    val seq = makeTraversable(iterable(context))
    seq.foreach(element => {
      context.put(symbol, element)
      actions.foreach(action => action.exec(context, state))
    })

    before match {
      case None => context.remove(symbol)
      case Some(old) => context.put(symbol, old)
    }

    Stream(context)
  }

  def influenceSymbolTable(symbols: SymbolTable) = symbols
}
