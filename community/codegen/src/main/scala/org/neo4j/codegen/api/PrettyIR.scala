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

object PrettyIR {
  val indentSize = 2

  def pretty(ir: IntermediateRepresentation, indent: Int = 0): String = {
    val pb = new PrettyBuilder(indent)
    pb.pretty(ir)
    pb.result
  }

  class PrettyBuilder(startIndent: Int) {
    val sb = new StringBuilder()
    var indent = startIndent

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
    def pretty(ir: IntermediateRepresentation): PrettyBuilder = {
      ir match {
        case Block(Seq(d @ DeclareLocalVariable(_, name), AssignToLocalVariable(name2, value))) if name == name2 =>
          pretty(d).append(" = ").pretty(value)

        case Block(Seq()) => append("{ }")

        case Block(ops) if ops.head.isInstanceOf[Block] =>
          pretty(Block(ops.head.asInstanceOf[Block].ops ++ ops.tail))

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

        case ph: PlaceHolder =>
          val ops = ph.ops
          if (ops.isEmpty) {
            append("{ /* EMPTY PLACEHOLDER */ }")
          } else {
            append("{ /* PLACEHOLDER */").newLine()
            incrIndent()
            ops.map(pretty(_).newLine())
            decrIndent()
            append("}")
          }

        case DeclareLocalVariable(typ, name) =>
          prettyType(typ).append(s" $name")

        case AssignToLocalVariable(name, value) =>
          append(s"$name = ").pretty(value)

        case NewInstance(constructor, params) =>
          append("new ").prettyType(constructor.owner).prettyParams(params)

        case NewInstanceInnerClass(clazz, arguments) =>
          append("new ").append(clazz.name).prettyParams(arguments)

        case NewArray(typ, size) =>
          append("new ").prettyType(typ).append("[").append(s"$size").append("]")

        case NewArrayDynamicSize(typ, size) =>
          append("new ").prettyType(typ).append("[").pretty(size).append("]")

        case Returns(value) =>
          append("return ").pretty(value)

        case Invoke(target, method, params) =>
          pretty(target).prettyInvoke(method, params)

        case InvokeSideEffect(target, method, params) =>
          pretty(target).prettyInvoke(method, params)

        case InvokeStatic(method, params) =>
          append(method.owner.simpleName()).prettyInvoke(method, params)

        case InvokeStaticSideEffect(method, params) =>
          append(method.owner.simpleName()).prettyInvoke(method, params)

        case InvokeLocal(method, params) =>
          append("this.").prettyInvoke(method, params)

        case InvokeLocalSideEffect(method, params) =>
          append("this.").prettyInvoke(method, params)

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

        case Self(_) =>
          append("this")

        case Eq(lhs, rhs) =>
          append("(").pretty(lhs).append(" == ").pretty(rhs).append(")")

        case NotEq(lhs, rhs) =>
          append("(").pretty(lhs).append(" != ").pretty(rhs).append(")")

        case Lt(lhs, rhs) =>
          append("(").pretty(lhs).append(" < ").pretty(rhs).append(")")

        case Lte(lhs, rhs) =>
          append("(").pretty(lhs).append(" <= ").pretty(rhs).append(")")

        case Gt(lhs, rhs) =>
          append("(").pretty(lhs).append(" > ").pretty(rhs).append(")")

        case Gte(lhs, rhs) =>
          append("(").pretty(lhs).append(" >= ").pretty(rhs).append(")")

        case Add(lhs, rhs) =>
          append("(").pretty(lhs).append(" + ").pretty(rhs).append(")")

        case Subtract(lhs, rhs) =>
          append("(").pretty(lhs).append(" - ").pretty(rhs).append(")")

        case Multiply(lhs, rhs) =>
          append("(").pretty(lhs).append(" * ").pretty(rhs).append(")")

        case Ternary(condition, onTrue, onFalse) =>
          append("( (").pretty(condition).append(") ? ").pretty(onTrue).append(" : ").pretty(onFalse).append(" )")

        case BooleanOr(as) =>
          val size = as.size
          append("(")
          as.zipWithIndex.foreach {
            case (v, i) if i < size - 1 => pretty(v).append(" || ")
            case (v, _)                 => pretty((v))
          }
          append(")")

        case BooleanAnd(as) =>
          val size = as.size
          append("(")
          as.zipWithIndex.foreach {
            case (v, i) if i < size - 1 => pretty(v).append(" && ")
            case (v, _)                 => pretty((v))
          }
          append(")")

        case Condition(test, onTrue, onFalse) =>
          append("if (").pretty(test).append(") ").pretty(onTrue)
          onFalse.foreach(ir => append(" else ").pretty(ir))

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

        case Break(labelName) =>
          append("break ").append(labelName)

        case Throw(error) =>
          append("throw ").pretty(error)

        case Cast(to, expression) =>
          append("((").prettyType(to).append(") ").pretty(expression).append(")")

        case InstanceOf(typ, expression) =>
          pretty(expression).append(" instanceof ").prettyType(typ)

        case Noop =>

        case IsNull(expr) =>
          append("(").pretty(expr).append(" == ").append("null").append(")")

        case Not(IsNull(expr)) =>
          append("(").pretty(expr).append(" != ").append("null").append(")")

        case Not(Eq(lhs, rhs)) =>
          append("(").pretty(lhs).append(" != ").pretty(rhs).append(")")

        case Not(expr) => append("!(").pretty(expr).append(")")

        case LoadField(None, field) =>
          append("this.").append(field.name)

        case LoadField(Some(owner), field) =>
          pretty(owner).append(".").append(field.name)

        case SetField(None, field, value) =>
          append("this.").append(field.name).append(" = ").pretty(value)

        case SetField(Some(owner), field, value) =>
          pretty(owner).append(".").append(field.name).append(" = ").pretty(value)

        case Box(expression) =>
          append("|<box>|(").pretty(expression).append(")")

        case Unbox(expression) =>
          append("|<unbox>|(").pretty(expression).append(")")

        case Comment(comment) =>
          append(s"// $comment")

        case ArrayLiteral(typ, values) =>
          append("new ").prettyType(typ).append("[] { ")
          values.zipWithIndex.foreach {
            case (value, i) =>
              pretty(value)
              if (i < values.size - 1) {
                append(", ")
              }
          }
          append(" }")

        case instruction =>
          throw new IllegalArgumentException(s"Please add the missing pretty IR string case for: $instruction")
      }
      this
    }

    private def prettyInvoke(method: Method, params: Seq[IntermediateRepresentation]): PrettyBuilder = {
      append(s".${method.name}")
      prettyParams(params)
    }

    private def prettyInvoke(method: PrivateMethod, params: Seq[IntermediateRepresentation]): PrettyBuilder = {
      append(s".${method.name}")
      prettyParams(params)
    }

    private def prettyType(typeReference: TypeReference): PrettyBuilder = {
      if (typeReference == null) {
        println("typeReference is null")
      }
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
