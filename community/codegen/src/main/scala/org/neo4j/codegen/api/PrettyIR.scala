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

import scala.annotation.nowarn

object PrettyIR {
  val indentSize = 2

  def pretty(ir: IntermediateRepresentation): String = {
    val pb = new PrettyBuilder()
    pb.pretty(ir)
    pb.result
  }

  class PrettyBuilder {
    val sb = new StringBuilder()
    var indent = 0

    var onNewLine = false

    def append(str: String): PrettyBuilder = {
      if (onNewLine) {
        sb.append(" " * indent)
        onNewLine = false
      }
      sb.append(str)
      this
    }

    def newLine(): PrettyBuilder = {
      sb.append("\n")
      onNewLine = true
      this
    }

    def incrIndent(): PrettyBuilder = {
      indent += indentSize
      this
    }

    def decrIndent(): PrettyBuilder = {
      indent -= indentSize
      this
    }

    def result: String = sb.result()

    // noinspection NameBooleanParameters
    @nowarn("msg=(Exhaustivity analysis)|(match may not be exhaustive)")
    def pretty(ir: IntermediateRepresentation): PrettyBuilder = {
      ir match {
        case Block(Seq(d @ DeclareLocalVariable(_, name), AssignToLocalVariable(name2, value))) if name == name2 =>
          pretty(d).append(" = ").pretty(value)

        case Block(Seq()) => append("{ }")

        case Block(ops) =>
          append("{").newLine()
          incrIndent()
          val lastIr = ops.tail.fold(ops.head) {
            /**
             * boolean v3;
             * v3 = true;
             *
             *    |
             *    v
             *
             * boolean v3 = true;
             */
            case (d @ DeclareLocalVariable(_, name), AssignToLocalVariable(name2, value)) if name == name2 =>
              pretty(d).append(" = ").pretty(value).newLine()
              Noop
            case (Noop, current) => current
            case (acc, current) =>
              pretty(acc).newLine()
              current
          }
          if (lastIr != Noop)
            pretty(lastIr).newLine()
          decrIndent()
          append("}")

        case DeclareLocalVariable(typ, name) =>
          prettyType(typ).append(s" $name")

        case AssignToLocalVariable(name, value) =>
          append(s"$name = ").pretty(value)

        case NewInstance(constructor, params) =>
          append("new ").prettyType(constructor.owner).prettyParams(params)

        case Invoke(target, method, params) =>
          pretty(target).prettyInvoke(method, params)

        case InvokeSideEffect(target, method, params) =>
          pretty(target).prettyInvoke(method, params)

        case InvokeStatic(method, params) =>
          append(method.owner.simpleName()).prettyInvoke(method, params)

        case InvokeStaticSideEffect(method, params) =>
          append(method.owner.simpleName()).prettyInvoke(method, params)

        case GetStatic(owner, _, name) =>
          if (name == "NO_VALUE") {
            append("NO_VALUE")
          } else {
            append(s"${owner.map(typ => typ.simpleName() + ".").getOrElse("")}$name")
          }

        case Constant(null) => append("null")

        case Constant(value) =>
          append(value.toString)

        case Load(name, _) =>
          append(name)

        case Eq(lhs, rhs) =>
          pretty(lhs).append(" == ").pretty(rhs)

        case NotEq(lhs, rhs) =>
          pretty(lhs).append(" != ").pretty(rhs)

        case Add(lhs, rhs) =>
          pretty(lhs).append(" + ").pretty(rhs)

        case Subtract(lhs, rhs) =>
          pretty(lhs).append(" - ").pretty(rhs)

        case BooleanOr(as) =>
          val size = as.size
          as.zipWithIndex.foreach {
            case (v, i) if i < size - 1 => pretty(v).append(" || ")
            case (v, _)                 => pretty((v))
          }

        case BooleanAnd(as) =>
          val size = as.size
          as.zipWithIndex.foreach {
            case (v, i) if i < size - 1 => pretty(v).append(" && ")
            case (v, _)                 => pretty((v))
          }

        case Condition(test, onTrue, onFalse) =>
          append("if (").pretty(test).append(") ").pretty(onTrue)
          onFalse.foreach(ir => append(" else ").pretty(ir))

        case Ternary(test, onTrue, onFalse) =>
          pretty(test).append(" ? ").pretty(onTrue).append(" : ").pretty(onFalse)

        case Loop(test, body, labelName) =>
          if (labelName != null)
            append(labelName).append(":").newLine()
          append("while (").pretty(test).append(") ").pretty(body)

        case ArrayLoad(array, offset) =>
          pretty(array).append("[").pretty(offset).append("]")

        case ArraySet(array, offset, value) =>
          pretty(array).append("[").pretty(offset).append("] = ").pretty(value)

        case OneTime(ir) =>
          append("oneTime(").pretty(ir).append(")")

        case TryCatch(ops, onError, exception, name) =>
          append(s"try {")
            .incrIndent()
            .newLine()
            .pretty(ops)
            .newLine()
            .decrIndent()
            .append(s"} catch($name: ${exception.simpleName()}) {")
            .incrIndent()
            .newLine()
            .pretty(onError)
            .newLine()
            .decrIndent()
            .append("}")

        case Throw(error) =>
          append("throw ").pretty(error)

        case Cast(to, expression) =>
          append("(").prettyType(to).append(")").pretty(expression)

        case Noop =>

        case Not(Eq(lhs, rhs)) => pretty(lhs).append(" != ").pretty(rhs)

        case Not(expr) => append("!(").pretty(expr).append(")")
      }
      this
    }

    private def prettyInvoke(method: Method, params: Seq[IntermediateRepresentation]): PrettyBuilder = {
      append(s".${method.name}")
      prettyParams(params)
    }

    private def prettyType(typeReference: TypeReference): PrettyBuilder = {
      append(typeReference.name())

      if (typeReference.isGeneric) {
        append("<")

        var first = true
        val iter = typeReference.parameters.iterator()
        while (iter.hasNext) {
          if (first) first = false else append(", ")
          prettyType(iter.next())
        }
        append(">")
      }

      if (typeReference.isArray) {
        append("[]")
      }
      this
    }

    private def prettyParams(params: Seq[IntermediateRepresentation]): PrettyBuilder = {
      append("(")
      if (params.nonEmpty) {
        pretty(params.head)
        params.tail.foreach(p => append(", ").pretty(p))
      }
      append(")")
    }
  }
}
