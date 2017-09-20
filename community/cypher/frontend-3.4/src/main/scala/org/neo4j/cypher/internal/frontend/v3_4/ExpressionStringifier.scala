/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.expressions._

case class ExpressionStringifier(extender: Expression => String = e => throw new InternalException(s"failed to pretty print $e")) {
  def apply(ast: Expression): String = {
    ast match {
      case StringLiteral(txt) =>
        var containsSingle = false
        var containsDouble = false
        txt.toCharArray foreach {
          case '"' => containsDouble = true
          case '\'' => containsSingle = true
          case _ =>
        }
        if (containsDouble && containsSingle)
          "\"" + txt.replaceAll("\"", "\\\\\"") + "\""
        else if (containsDouble)
          "'" + txt + "'"
        else
          "\"" + txt + "\""

      case l: Literal =>
        l.asCanonicalStringVal
      case e: BinaryOperatorExpression =>
        s"${parens(e, e.lhs)} ${e.canonicalOperatorSymbol} ${parens(e, e.rhs)}"
      case Variable(v) =>
        backtick(v)
      case ListLiteral(expressions) =>
        expressions.map(this.apply).mkString("[", ", ", "]")
      case FunctionInvocation(namespace, functionName, distinct, args) =>
        val ns = namespace.parts.mkString(".")
        val ds = if (distinct) "DISTINCT " else ""
        val as = args.map(this.apply).mkString(", ")
        s"$ns${functionName.name}($ds$as)"
      case p@Property(m, k) =>
        s"${parens(p, m)}.${backtick(k.name)}"
      case MapExpression(items) =>
        val is = items.map({ case (k, e) => s"${backtick(k.name)}: ${this.apply(e)}" }).mkString(", ")
        s"{$is}"
      case Parameter(name, _) =>
        s"$name"
      case _: CountStar =>
        s"count(*)"
      case e@IsNull(arg) =>
        s"${parens(e, arg)} IS NULL"
      case e@IsNotNull(arg) =>
        s"${parens(e, arg)} IS NOT NULL"
      case e@ContainerIndex(exp, idx) =>
        s"${parens(e, exp)}[${this.apply(idx)}]"
      case ListSlice(list, start, end) =>
        val l = start.map(this.apply).getOrElse("")
        val r = end.map(this.apply).getOrElse("")
        s"${this.apply(list)}[$l..$r]"
      case PatternExpression(RelationshipsPattern(relChain)) =>
        pattern(relChain)
      case FilterExpression(scope, expression) =>
        s"filter${prettyScope(scope, expression)}"
      case AnyIterablePredicate(scope, expression) =>
        s"any${prettyScope(scope, expression)}"
      case not@Not(arg) =>
        s"not ${parens(not, arg)}"
      case ListComprehension(s, expression) =>
        val v = this.apply(s.variable)
        val p = s.innerPredicate.map(e => " WHERE " + this.apply(e)).getOrElse("")
        val e = s.extractExpression.map(e => " | " + this.apply(e)).getOrElse("")
        val expr = this.apply(expression)
        s"[$v IN $expr$p$e]"
      case ExtractExpression(s, expression) =>
        val v = this.apply(s.variable)
        val p = s.innerPredicate.map(e => " WHERE " + this.apply(e)).getOrElse("")
        val e = s.extractExpression.map(e => " | " + this.apply(e)).getOrElse("")
        val expr = this.apply(expression)
        s"extract($v IN $expr$p$e)"
      case PatternComprehension(variable, RelationshipsPattern(relChain), predicate, proj, _) =>
        val v = variable.map(e => s"${this.apply(e)} = ").getOrElse("")
        val p = predicate.map(e => " WHERE " + this.apply(e)).getOrElse("")
        s"[$v${pattern(relChain)}$p | ${this.apply(proj)}]"
      case e@HasLabels(arg, labels) =>
        val l = labels.map(label => backtick(label.name)).mkString(":", ":", "")
        s"${parens(e, arg)}$l"
      case AllIterablePredicate(scope, e) =>
        s"all${prettyScope(scope, e)}"
      case NoneIterablePredicate(scope, e) =>
        s"none${prettyScope(scope, e)}"
      case MapProjection(variable, items, _) =>
        val itemsText = items.map {
          case LiteralEntry(k, e) => s"${backtick(k.name)}: ${this.apply(e)}"
          case VariableSelector(v) => this.apply(v)
          case PropertySelector(v) => s".${this.apply(v)}"
          case AllPropertiesSelector() => ".*"
        }.mkString(", ")
        s"${this.apply(variable)}{$itemsText}"
      case CaseExpression(expression, alternatives, default) =>
        val e = expression.map(e => s" ${this.apply(e)}").getOrElse("")
        val d = default.map(e => s" else ${this.apply(e)} ").getOrElse("")
        val items = (alternatives map {
          case (e1, e2) => s"when ${this.apply(e1)} then ${this.apply(e2)}"
        }).mkString(" ", " ", "")
        s"case$e$items${d}end"
      case e@Ands(expressions) =>
        expressions.map(x => parens(e, x)).mkString(" AND ")
      case e@Ors(expressions) =>
        expressions.map(x => parens(e, x)).mkString(" OR ")
      case ShortestPathExpression(s@ShortestPaths(r:RelationshipChain, _)) =>
        s"${s.name}(${pattern(r)})"
      case _: ExtractScope | _: FilterScope | _: ReduceScope =>
        // These are not really expressions, they are part of expressions
        ""
      case _ =>
        println(s"$ast failed on default, trying extender")
        extender(ast)
    }
  }

  private def parens(caller: Expression, argument: Expression) = {
    val thisPrecedence = precedenceLevel(caller)
    val argumentPrecedence = precedenceLevel(argument)
    if (argumentPrecedence >= thisPrecedence)
      s"(${this.apply(argument)})"
    else
      this.apply(argument)
  }

  private def prettyScope(s: FilterScope, expression: Expression) = {
    val v = this.apply(s.variable)
    val e = this.apply(expression)
    val p = s.innerPredicate.map(this.apply).getOrElse("")
    s"($v IN $e WHERE $p)"
  }

  private def backtick(txt: String) = {
    val c = txt.toCharArray
    val needsBackticks = !(Character.isJavaIdentifierStart(c(0)) && c.tail.forall(Character.isJavaIdentifierPart))
    if (needsBackticks)
      s"`$txt`"
    else
      txt
  }

  private def props(prepend: String, e: Option[Expression]): String = {
    val separator = if(prepend=="") "" else " "
    e.map(e => s"$separator${this.apply(e)}").getOrElse(prepend)
  }

  private def node(nodePattern: NodePattern): String = {
    val name = nodePattern.variable.map(this.apply).getOrElse("")
    val labels = if (nodePattern.labels.isEmpty) "" else
      nodePattern.labels.map(l => backtick(l.name)).mkString(":", ":", "")
    val e = props(s"$name$labels", nodePattern.properties)
    s"($e)"
  }

  private def edge(relationship: RelationshipPattern) = {
    val lArrow = if (relationship.direction == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (relationship.direction == SemanticDirection.OUTGOING) ">" else ""
    val types = if (relationship.types.isEmpty)
      ""
    else
      relationship.types.map(l => backtick(l.name)).mkString(":", ":", "")
    val name = relationship.variable.map(this.apply).getOrElse("")
    val length = relationship.length match {
      case None => ""
      case Some(None) => "*"
      case Some(Some(Range(lower, upper))) =>
        s"*${lower.map(_.stringVal).getOrElse("")}..${upper.map(_.stringVal).getOrElse("")}"
    }
    val info = props(s"$name$types$length", relationship.properties)
    if (info == "")
      s"$lArrow--$rArrow"
    else
      s"$lArrow-[$info]-$rArrow"
  }

  private def pattern(relationshipChain: RelationshipChain): String = {
    val r = node(relationshipChain.rightNode)
    val middle = edge(relationshipChain.relationship)
    val l = relationshipChain.element match {
      case r: RelationshipChain => pattern(r)
      case n: NodePattern => node(n)
    }

    s"$l$middle$r"
  }

  private def precedenceLevel(in: Expression): Int = in match {
    case _: Or |
         _: Ors =>
      12
    case _: Xor =>
      11
    case _: And |
         _: Ands =>
      10
    case _: Not =>
      9
    case _: Equals |
         _: NotEquals |
         _: InvalidNotEquals |
         _: GreaterThan |
         _: GreaterThanOrEqual |
         _: LessThan |
         _: LessThanOrEqual =>
      8
    case _: Add |
         _: Subtract =>
      7
    case _: Multiply |
         _: Divide |
         _: Modulo =>
      6
    case _: Pow =>
      5
    case _: UnaryAdd |
         _: UnarySubtract =>
      4
    case _: RegexMatch |
         _: In |
         _: StartsWith |
         _: EndsWith |
         _: Contains |
         _: IsNull |
         _: IsNotNull =>
      3
    case _: Property |
         _: HasLabels |
         _: ContainerIndex |
         _: ListSlice =>
      2
    case _ =>
      1

  }
}
