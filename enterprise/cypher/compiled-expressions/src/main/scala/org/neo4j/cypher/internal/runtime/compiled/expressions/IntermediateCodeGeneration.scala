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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.operations.{CypherBoolean, CypherDbAccess, CypherFunctions, CypherMath}
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
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
    case c: FunctionInvocation => c.function match {
      case functions.Acos =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("acos"), _))
      case functions.Cos =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("cos"), _))
      case functions.Cot =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("cot"), _))
      case functions.Asin =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("asin"), _))
      case functions.Haversin =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("haversin"), _))
      case functions.Sin =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("sin"), _))
      case functions.Atan =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("atan"), _))
      case functions.Atan2 =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("atan2"), _))
      case functions.Tan =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("tan"), _))
      case functions.Round =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("round"), _))
      case functions.Rand =>
        Some(invokeStatic(method[CypherFunctions, DoubleValue]("rand")))
      case functions.Abs =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("abs"), _))
      case functions.Ceil =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("ceil"), _))
      case functions.Floor =>
        compile(c.args.head).map(invokeStatic(method[CypherFunctions, Value, AnyValue]("floor"), _))

      case _ => None
    }

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
    case d: DoubleLiteral => Some(invokeStatic(method[Values, DoubleValue, Double]("doubleValue"), constant(d.value)))
    case i: IntegerLiteral => Some(invokeStatic(method[Values, LongValue, Long]("longValue"), constant(i.value)))
    case s: expressions.StringLiteral => Some(invokeStatic(method[Values, TextValue, String]("stringValue"), constant(s.value)))
    case _: Null => Some(noValue)
    case _: True => Some(truthy)
    case _: False => Some(falsy)

    //boolean operators
    case Or(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield invokeStatic(method[CypherBoolean, Value, Array[AnyValue]]("or"), arrayOf(l, r))

    case Ors(exprs) =>
      val compiled = exprs.flatMap(compile).toIndexedSeq
      //we bail if some of the expressions weren't compiled
      if (compiled.size < exprs.size) None
      else Some(invokeStatic(method[CypherBoolean, Value, Array[AnyValue]]("or"), arrayOf(compiled: _*)))

    case Xor(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), l, r)

    case And(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield invokeStatic(method[CypherBoolean, Value, Array[AnyValue]]("and"), arrayOf(l, r))

    case Ands(expressions) =>
      val compiled = expressions.flatMap(compile).toIndexedSeq
      //we bail if some of the expressions weren't compiled
      if (compiled.size < expressions.size) None
      else Some(invokeStatic(method[CypherBoolean, Value, Array[AnyValue]]("and"), arrayOf(compiled: _*)))

    case Not(arg) =>
      compile(arg).map(invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), _))

    case Equals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("equals"), l, r)

    case NotEquals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l, r)

    //data access
    case Parameter(name, _) =>
      Some(invoke(load("params"), method[MapValue, AnyValue, String]("get"), constant(name)))

    case NodeProperty(offset, token, _) =>
      Some(invokeStatic(method[CypherDbAccess, Value, Transaction, Long, Int]("nodeProperty"),
                        load("tx"),
                        invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                               constant(offset)), constant(token)))

    case NodePropertyLate(offset, key, _) =>
      Some(invokeStatic(method[CypherDbAccess, Value, Transaction, Long, String]("nodeProperty"),
                        load("tx"),
                        invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                               constant(offset)), constant(key)))

    case RelationshipProperty(offset, token, _) =>
      Some(invokeStatic(method[CypherDbAccess, Value, Transaction, Long, Int]("relationshipProperty"),
                        load("tx"),
                        invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                               constant(offset)), constant(token)))

      //slotted operations
    case ReferenceFromSlot(offset, _) =>
      Some(invoke(load("context"), method[ExecutionContext, AnyValue, Int]("getRefAt"),
                  constant(offset)))
    case IdFromSlot(offset) =>
      Some(
        invokeStatic(method[Values, LongValue, Long]("longValue"),
                     invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
                            constant(offset))))
    case _ => None
  }

}
