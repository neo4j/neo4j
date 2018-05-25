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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast.{NodeProperty, RelationshipProperty}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{doubleValue, longValue}
import org.neo4j.values.storable.{BooleanValue, DoubleValue, Value, Values}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions._

/**
  * Produces IntermediateRepresentation from a Cypher Expression
  */
object IntermediateCodeGeneration {

  import IntermediateRepresentation._

  def compile(expression: Expression): Option[IntermediateRepresentation] = expression match {

    //functions
    case c: FunctionInvocation if c.function == functions.Round =>
      compile(c.args.head) match {
        case Some(arg) =>
          Some(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), arg))
        case _ => None
      }

    case c: FunctionInvocation if c.function == functions.Sin =>
      compile(c.args.head) match {
        case Some(arg) =>
          Some(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), arg))
        case _ => None
      }

    case c: FunctionInvocation if c.function == functions.Rand =>
      Some(invokeStatic(method[CypherFunctions, DoubleValue]("rand")))

    //math
    case Multiply(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l, r))

        case _ => None
      }

    case Add(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l, r))
        case _ => None
      }

    case Subtract(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l, r))
        case _ => None
      }


    //literals
    case d: DoubleLiteral => Some(float(doubleValue(d.value)))
    case i: IntegerLiteral => Some(integer(longValue(i.value)))
    case s: expressions.StringLiteral => Some(string(Values.stringValue(s.value)))
    case _: Null => Some(noValue)
    case _: True => Some(truthy)
    case _: False => Some(falsy)

    //boolean operators
    case Or(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[CypherBoolean, Value, Array[AnyValue]]("or"), arrayOf(l, r)))
        case _ => None
      }

    //data access
    case Parameter(name, _) =>
      Some(invoke(load("params"), method[MapValue, AnyValue, String]("get"), constantJavaValue(name)))

    case NodeProperty(offset, token, _) =>
      Some(invokeStatic(method[CypherDbAccess, Value, Transaction, Long, Int]("nodeProperty"),
                        load("tx"),
                        invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                               constantJavaValue(offset)), constantJavaValue(token)))

    case RelationshipProperty(offset, token, _) =>
      Some(invokeStatic(method[CypherDbAccess, Value, Transaction, Long, Int]("relationshipProperty"),
                        load("tx"),
                        invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                               constantJavaValue(offset)), constantJavaValue(token)))

    case _ => None
  }

}
