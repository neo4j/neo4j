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
package org.neo4j.codegen.api

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.IntermediateRepresentation.booleanValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueBooleanLogic

object CodeOptimization {

  val NO_VALUE_TYPE: TypeReference = typeRefOf[NoValue]
  val LONG_VALUE_TYPE: TypeReference = typeRefOf[LongValue]
  val INT_VALUE_TYPE: TypeReference = typeRefOf[IntValue]
  val BOOLEAN_VALUE_TYPE: TypeReference = typeRefOf[BooleanValue]
  val VALUE_TYPE: TypeReference = typeRefOf[Value]
  val ANY_VALUE_TYPE: TypeReference = typeRefOf[AnyValue]
  val VALUES_TYPE: TypeReference = typeRefOf[Values]
  val VALUE_BOOLEAN_LOGIC_TYPE: TypeReference = typeRefOf[ValueBooleanLogic]

  object LongValueFcn {

    def unapply(arg: IntermediateRepresentation): Option[IntermediateRepresentation] = arg match {
      case InvokeStatic(Method(owner, returnType, "longValue", Seq(inType)), Seq(in))
        if owner == VALUES_TYPE && returnType == LONG_VALUE_TYPE && inType == TypeReference.LONG =>
        Some(in)
      case _ => None
    }
  }

  object ReferenceEqualsFcn {

    def unapply(arg: IntermediateRepresentation): Option[(IntermediateRepresentation, IntermediateRepresentation)] =
      arg match {
        case Invoke(target, Method(_, returnType, "equals", Seq(inType)), Seq(obj))
          if returnType == TypeReference.BOOLEAN && inType == TypeReference.OBJECT =>
          Some((target, obj))
        case _ => None
      }
  }

  object IntValueFcn {

    def unapply(arg: IntermediateRepresentation): Option[IntermediateRepresentation] = arg match {
      case InvokeStatic(Method(owner, returnType, "intValue", Seq(inType)), Seq(in))
        if owner == VALUES_TYPE && returnType == INT_VALUE_TYPE && inType == TypeReference.INT =>
        Some(in)
      case _ => None
    }
  }

  object BooleanValueFcn {

    def unapply(arg: IntermediateRepresentation): Option[IntermediateRepresentation] = arg match {
      case InvokeStatic(Method(owner, returnType, "booleanValue", Seq(inType)), Seq(in))
        if owner == VALUES_TYPE && returnType == BOOLEAN_VALUE_TYPE && inType == TypeReference.BOOLEAN =>
        Some(in)
      case _ => None
    }
  }

  object IsTrueValue {

    def unapply(arg: IntermediateRepresentation): Boolean = arg match {
      case GetStatic(Some(owner), t, "TRUE") => owner == VALUES_TYPE && t == BOOLEAN_VALUE_TYPE
      case _                                 => false
    }
  }

  object NotFcn {

    def unapply(arg: IntermediateRepresentation): Option[IntermediateRepresentation] = arg match {
      case InvokeStatic(Method(owner, returnType, "not", Seq(inType)), Seq(in))
        if owner == VALUE_BOOLEAN_LOGIC_TYPE && returnType == VALUE_TYPE && inType == ANY_VALUE_TYPE =>
        Some(in)
      case _ => None
    }
  }

  object InFcn {

    def unapply(arg: IntermediateRepresentation): Boolean = arg match {
      case InvokeStatic(Method(owner, returnType, "in", _), _)
        if owner == VALUE_BOOLEAN_LOGIC_TYPE && returnType == VALUE_TYPE => true
      case _ => false
    }
  }

  object CoerceToBooleanFcn {

    def unapply(arg: IntermediateRepresentation): Option[IntermediateRepresentation] = arg match {
      case InvokeStatic(Method(owner, returnType, "coerceToBoolean", Seq(inType)), Seq(in))
        if owner == VALUE_BOOLEAN_LOGIC_TYPE && returnType == VALUE_TYPE && inType == ANY_VALUE_TYPE =>
        Some(in)
      case _ => None
    }
  }

  private val stopper: RewriterStopper = {
    case _: OneTime => true
    case _          => false
  }

  /**
   * Simplifies commonly occurring predicate expressions in generated code
   */
  def simplifyPredicates(predicate: IntermediateRepresentation): IntermediateRepresentation = {
    val rewriter = bottomUp(
      Rewriter.lift {
        // booleanValue(a) == Values.TRUE -> a
        case Eq(BooleanValueFcn(in), IsTrueValue())                  => in
        case Eq(Block(others :+ BooleanValueFcn(in)), IsTrueValue()) => Block(others :+ in)
        // Values.TRUE == booleanValue(a) -> a
        case Eq(IsTrueValue(), BooleanValueFcn(in))                  => in
        case Eq(IsTrueValue(), Block(others :+ BooleanValueFcn(in))) => Block(others :+ in)
        // ValueBooleanLogic.not(booleanValue(a)) -> booleanValue(!a)
        case NotFcn(BooleanValueFcn(in)) => booleanValue(not(in))
        // !true -> false
        case Not(Constant(true)) => Constant(false)
        // !false -> true
        case Not(Constant(false)) => Constant(true)
        // !!a -> a
        case Not(Not(inner)) => inner
        // !(a==b) -> a != b
        case Not(Eq(l, r)) => NotEq(l, r)
        // longValue(a).equals(longValue(b)) -> a == b
        case ReferenceEqualsFcn(LongValueFcn(l), LongValueFcn(r)) => Eq(l, r)
        // intValue(a).equals(intValue(b)) -> a == b
        case ReferenceEqualsFcn(IntValueFcn(l), IntValueFcn(r)) => Eq(l, r)
        // booleanValue(a).equals(booleanValue(b)) -> a == b
        case ReferenceEqualsFcn(BooleanValueFcn(l), BooleanValueFcn(r)) => Eq(l, r)
        // true || a -> true
        // a || true -> true
        // false || a -> a
        // a || false -> a
        case BooleanOr(ors) =>
          val reducedBuilder = Seq.newBuilder[IntermediateRepresentation]
          var seenTrue = false
          ors.foreach {
            case Constant(true)   => seenTrue = true
            case Constant(false)  => // ignore
            case BooleanOr(inner) => reducedBuilder.addAll(inner)
            case other            => reducedBuilder += other
          }
          val reduced = reducedBuilder.result()
          if (seenTrue) Constant(true)
          else if (reduced.isEmpty) Constant(false)
          else if (reduced.size == 1) reduced.head
          else BooleanOr(reduced)

        // true && a -> a
        // a && true -> a
        // false && a -> false
        // a && false -> false
        case BooleanAnd(ands) =>
          val reducedBuilder = Seq.newBuilder[IntermediateRepresentation]
          var seenFalse = false
          ands.foreach {
            case Constant(false)   => seenFalse = true
            case Constant(true)    => // ignore
            case BooleanAnd(inner) => reducedBuilder.addAll(inner)
            case other             => reducedBuilder += other
          }
          val reduced = reducedBuilder.result()
          if (seenFalse) Constant(false)
          else if (reduced.isEmpty) Constant(true)
          else if (reduced.size == 1) reduced.head
          else BooleanAnd(reduced)

        // if (true) doStuff -> doStuff
        case Condition(Constant(true), onTrue, _) => onTrue
        // if (false) doStuff -> doNothing/doElse
        case Condition(Constant(false), _, maybeOnFalse) => maybeOnFalse.getOrElse(noop())
        // true ? a : b -> a
        case Ternary(Constant(true), onTrue, _) => onTrue
        // false ? a : b -> b
        case Ternary(Constant(false), _, onFalse) => onFalse
      },
      stopper = stopper
    )
    rewriter(predicate).asInstanceOf[IntermediateRepresentation]
  }
}
