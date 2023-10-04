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
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren

import java.lang.reflect.Modifier.isInterface

import scala.collection.mutable

object SizeEstimation {
  private val JUMP_INSTRUCTION = 3
  private val FIELD_INSTRUCTION = 3
  private val INVOKE = 3
  private val INVOKE_INTERFACE = 5
  private val LDC_INSTRUCTION = 2
  private val WIDE_LDC_INSTRUCTION = 3
  private val TYPE_INSTRUCTION = 3

  /**
   * Estimates the number of bytes the byte code that is required for the generating
   * the byte code of this method.
   * @param m The method to estimate
   * @return the estimation of the number of bytes this will use
   */
  def estimateByteCodeSize(m: MethodDeclaration): Int = {
    val fullBody =
      block(
        (m.parameters.map(p => declare(p.typ, p.name)) ++
          m.localVariables.distinct.map(v => declareAndAssign(v.typ, v.name, v.value))) :+
          m.body: _*
      )

    estimateByteCodeSize(fullBody, 0)
  }

  def estimateByteCodeSize(ir: IntermediateRepresentation, initialNumberOfVariables: Int): Int = {
    // 0 is always `this`
    var localVarCount = initialNumberOfVariables + 1
    // keeps track of the index of each local variable
    val locals = mutable.Map.empty[String, Int]
    def declare(typeReference: TypeReference, name: String): Unit = {
      locals.put(name, localVarCount)
      if (typeReference.simpleName() == "long" || typeReference.simpleName() == "double") {
        localVarCount += 2
      } else {
        localVarCount += 1
      }
    }
    def localVarInstruction(name: String) = {
      val index = locals.getOrElse(name, initialNumberOfVariables)
      if (index < 4) 1 else if (index >= 256) 4 else 2
    }
    def sizeOfIntPush(i: Integer) = {
      if (i < 6 && i >= -1) 1
      else if (i <= Byte.MaxValue && i >= Byte.MinValue) 2 // BIPUSH
      else if (i <= Short.MaxValue && i >= Short.MinValue) 3 // SIPUSH
      else LDC_INSTRUCTION // constant pool
    }
    def costOfNot(not: Not) = not.test match {
      case _: Not                                                         => 0
      case _: Constant                                                    => 0
      case _: Gt | _: Gte | _: Lt | _: Lte | _: Eq | _: NotEq | _: IsNull => 0
      case _                                                              => 2 /*true and false*/ + 2 * JUMP_INSTRUCTION
    }

    val visitedOneTimes = mutable.Set.empty[IntermediateRepresentation]
    ir.folder.treeFold(0) {

      case _: Field =>
        acc => SkipChildren(acc)

      case op: IntermediateRepresentation =>
        var visitChildren = true

        val bytesForInstruction = op match {
          // Freebies
          case oneTime: OneTime =>
            if (!visitedOneTimes.add(oneTime)) {
              visitChildren = false
            }
            0
          case _: Block | Noop => 0

          // Single byte instructions
          case _: ArraySet | _: ArrayLength | _: ArrayLoad | _: Add | _: Subtract | _: Multiply | _: Returns | _: Self | _: Throw =>
            1

          // multi byte instructions
          case i: InvokeSideEffect => if (isInterface(i.method.owner.modifiers())) INVOKE_INTERFACE else INVOKE
          case i: Invoke           => if (isInterface(i.method.owner.modifiers())) INVOKE_INTERFACE else INVOKE
          case _: InvokeStatic | _: InvokeStaticSideEffect | _: InvokeLocal | _: InvokeLocalSideEffect => INVOKE
          case Load(name, _) => localVarInstruction(name)
          case _: LoadField  => 1 + FIELD_INSTRUCTION // load this + 3 bytes for the field
          case _: SetField   => 1 + FIELD_INSTRUCTION // load this + 3 bytes for setting the field
          case _: GetStatic  => FIELD_INSTRUCTION
          case DeclareLocalVariable(typ, name) =>
            declare(typ, name)
            0
          case AssignToLocalVariable(name, _) =>
            localVarInstruction(name)
          case Constant(constant) => constant match {
              case i: Int     => sizeOfIntPush(i)
              case l: Long    => if (l == 0L || l == 1L) 1 else WIDE_LDC_INSTRUCTION // constant pool (unless 0 or 1)
              case _: Boolean => 1
              case d: Double  => if (d == 0.0 || d == 1.0) 1 else WIDE_LDC_INSTRUCTION // constant pool (unless 0 or 1)
              case null       => 1
              case _          => LDC_INSTRUCTION
            }
          case ArrayLiteral(typ, values) =>
            val numberOfElements = values.length
            val sizeOfNewArray = if (typ.isPrimitive) 2 else 3
            sizeOfIntPush(numberOfElements) + sizeOfNewArray + (0 until numberOfElements).map(i =>
              1 + sizeOfIntPush(i) + 1
            ).sum
          case NewArray(typ, size)         => sizeOfIntPush(size) + (if (typ.isPrimitive) 2 else 3)
          case NewArrayDynamicSize(typ, _) => if (typ.isPrimitive) 2 else 3

          // Conditions and loops
          case _: Ternary                              => 2 * JUMP_INSTRUCTION // two jump instructions
          case Condition(ands: BooleanAnd, _, onFalse) =>
            // Here we subtract the cost of the jump instruction + TRUE + FALSE since we can combine
            // it with the jump instruction of the conditional

            // for each or contained in the ands we also save in on two JUMP INSTRUCTIONS and TRUE, FALSE
            val numberOfOrs = ands.folder.treeCount {
              case _: BooleanOr => ()
            }
            -5 + onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0) - numberOfOrs * 8
          case Condition(_: BooleanOr, _, onFalse) =>
            // Here we subtract the cost of the jump instruction + TRUE + FALSE since we can combine
            // it with the jump instruction of the conditional
            -5 + onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0)
          case Condition(Not(_: BooleanAnd), _, onFalse) => onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0) - 8 - 5
          case Condition(Not(_: BooleanOr), _, onFalse)  => onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0) - 8 - 5
          case Condition(not: Not, _, onFalse)  => 3 - costOfNot(not) + onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0)
          case Condition(_: IsNull, _, onFalse) => 3 - 8 + onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0)
          case c: Condition =>
            3 + c.onFalse.map(_ => JUMP_INSTRUCTION).getOrElse(0) // single jump instruction takes 3 bytes

          case Loop(ands: BooleanAnd, _, _) =>
            // Here we subtract the cost of the jump instruction + TRUE + FALSE since we can combine
            // it with the jump instruction of the loop

            // for each or contained in the ands we also save in on two JUMP INSTRUCTIONS and TRUE, FALSE
            val numberOfOrs = ands.folder.treeCount {
              case _: BooleanOr => ()
            }
            -5 + JUMP_INSTRUCTION - numberOfOrs * 8
          case Loop(_: BooleanOr, _, _) =>
            // Here we subtract the cost of the jump instruction + TRUE + FALSE since we can combine
            // it with the jump instruction of the loop
            -5 + JUMP_INSTRUCTION
          case Loop(Not(_: BooleanAnd), _, _) => JUMP_INSTRUCTION - 5 - 8 // subtract the cost of OR and the cost of NOT
          case Loop(Not(_: BooleanOr), _, _)  => JUMP_INSTRUCTION - 5 - 8 // subtract the cost of OR and the cost of NOT
          case Loop(not: Not, _, _)           => 2 * JUMP_INSTRUCTION - costOfNot(not)
          case Loop(_: IsNull, _, _)          => 2 * JUMP_INSTRUCTION - 8
          case _: Loop                        => 2 * JUMP_INSTRUCTION // two jump instructions

          // Boolean operations
          case BooleanAnd(as) =>
            // For a stand-alone `and` we generate a single jump instruction (3 bytes) and a TRUE and FALSE for the different cases
            // furthermore for each argument we generate a jump instruction
            (1 + 1 + JUMP_INSTRUCTION) + as.length * 3
          // For a stand-alone `or` we generate a single jump instruction (3 bytes) and a TRUE and FALSE for the different cases
          // furthermore for each argument we generate a jump instruction
          case BooleanOr(as) => (1 + 1 + JUMP_INSTRUCTION) + as.length * 3
          case c: Gt         => if (c.lhs.typeReference == TypeReference.INT) 8 else 9
          case c: Gte        => if (c.lhs.typeReference == TypeReference.INT) 8 else 9
          case c: Lt         => if (c.lhs.typeReference == TypeReference.INT) 8 else 9
          case c: Lte        => if (c.lhs.typeReference == TypeReference.INT) 8 else 9
          case c: Eq     => if (c.lhs.typeReference == TypeReference.INT || !c.lhs.typeReference.isPrimitive) 8 else 9
          case c: NotEq  => if (c.lhs.typeReference == TypeReference.INT || !c.lhs.typeReference.isPrimitive) 8 else 9
          case _: IsNull => 2 /*true and false*/ + 2 * JUMP_INSTRUCTION
          case n: Not    => costOfNot(n)

          // Misc operations
          case _: NewInstance | _: NewInstanceInnerClass => 3 /*NEW*/ + 1 /*DUP*/ + INVOKE /*INVOKESPECIAL*/
          case _: Break                                  => JUMP_INSTRUCTION // an extra jump instruction
          case _: Box                                    => INVOKE // boils down to INVOKESTATIC, eg `Long.valueOf(x)`
          case _: Unbox                                  => INVOKE // boils down to INVOKEVIRTUAL, eg `x.longValue()`
          case Cast(to, _)                               => if (to.isPrimitive) 1 else TYPE_INSTRUCTION
          case _: InstanceOf                             => TYPE_INSTRUCTION
          case t: TryCatch =>
            declare(t.typeReference, t.name)
            JUMP_INSTRUCTION + localVarInstruction(t.name)

          case unknown => throw new IllegalStateException(s"Don't know how many bytes $unknown will use")
        }

        if (visitChildren) acc => TraverseChildren(acc + bytesForInstruction)
        else acc => SkipChildren(acc + bytesForInstruction)
    }
  }
}
