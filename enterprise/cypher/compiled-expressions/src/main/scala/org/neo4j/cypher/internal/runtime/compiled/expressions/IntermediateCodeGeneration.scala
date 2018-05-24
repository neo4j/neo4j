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

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{doubleValue, longValue}
import org.neo4j.values.storable.{DoubleValue, Values}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.expressions._


object IntermediateCodeGeneration {

  import IntermediateRepresentation._

  def compile(expression: Expression): Option[IntermediateRepresentation] = expression match {

    case c: FunctionInvocation if c.function == functions.Round =>
      compile(c.args.head) match {
        case Some(arg) =>
          Some(invokeStatic(method[AnyValueMath, DoubleValue, AnyValue]("round"), arg))
        case _ => None
      }

    case c: FunctionInvocation if c.function == functions.Sin =>
      compile(c.args.head) match {
        case Some(arg) =>
          Some(invokeStatic(method[AnyValueMath, DoubleValue, AnyValue]("sin"), arg))
        case _ => None
      }

    case c: FunctionInvocation if c.function == functions.Rand =>
      Some(invokeStatic(method[AnyValueMath, DoubleValue]("rand")))

    case Multiply(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[AnyValueMath, AnyValue, AnyValue, AnyValue]("multiply"), l, r))

        case _ => None
      }

    case Add(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[AnyValueMath, AnyValue, AnyValue, AnyValue]("add"), l, r))
        case _ => None
      }

    case Subtract(lhs, rhs) =>
      (compile(lhs), compile(rhs)) match {
        case (Some(l), Some(r)) =>
          Some(invokeStatic(method[AnyValueMath, AnyValue, AnyValue, AnyValue]("subtract"), l, r))
        case _ => None
      }

    case Parameter(name, _) =>
      Some(invoke(load("params"), method[MapValue, AnyValue, String]("get"), constant(name)))

    case d: DoubleLiteral => Some(float(doubleValue(d.value)))

    case i: IntegerLiteral => Some(integer(longValue(i.value)))

    case _ => None
  }

}
