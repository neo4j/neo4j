/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.BinaryOperatorExpression
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ChainableBinaryOperatorExpression
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.ElementIdToLongId
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.UserDefinedFunctionInvocation
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UnicodeHelper
import org.neo4j.internal.helpers.Strings

trait ExpressionStringifier {
  def apply(ast: Expression): String
  def apply(s: SymbolicName): String
  def apply(ns: Namespace): String
  def patterns: PatternStringifier
  def pathSteps: PathStepStringifier
  def backtick(in: String): String
  def quote(txt: String): String
  def escapePassword(password: Expression): String
  def stringifyLabelExpression(le: LabelExpression): String
}

private class DefaultExpressionStringifier(
  extensionStringifier: ExpressionStringifier.Extension,
  alwaysParens: Boolean,
  alwaysBacktick: Boolean,
  preferSingleQuotes: Boolean,
  sensitiveParamsAsParams: Boolean
) extends ExpressionStringifier {

  override val patterns: PatternStringifier = PatternStringifier(this)

  override val pathSteps: PathStepStringifier = PathStepStringifier(this)

  private val prettifier: Prettifier = Prettifier(this)

  override def apply(ast: Expression): String =
    stringify(ast)

  override def apply(s: SymbolicName): String =
    backtick(s.name)

  override def apply(ns: Namespace): String =
    ns.parts.map(backtick).mkString(".")

  private def inner(outer: Expression)(inner: Expression): String = {
    val str = stringify(inner)

    def parens = (binding(outer), binding(inner)) match {
      case (_, Syntactic)                 => false
      case (Syntactic, _)                 => false
      case (Precedence(o), Precedence(i)) => i >= o
    }

    if (alwaysParens || parens) "(" + str + ")"
    else str
  }

  private def stringify(ast: Expression): String = {
    ast match {

      case StringLiteral(txt) =>
        quote(txt)

      case l: Literal =>
        l.asCanonicalStringVal

      case e: BinaryOperatorExpression =>
        s"${inner(ast)(e.lhs)} ${e.canonicalOperatorSymbol} ${inner(ast)(e.rhs)}"

      case Variable(v) =>
        backtick(v)

      case ListLiteral(expressions) =>
        expressions.map(apply).mkString("[", ", ", "]")

      case FunctionInvocation(namespace, functionName, distinct, args) =>
        val ns = apply(namespace)
        val np = if (namespace.parts.isEmpty) "" else "."
        val ds = if (distinct) "DISTINCT " else ""
        val as = args.map(inner(ast)).mkString(", ")
        s"$ns$np${apply(functionName)}($ds$as)"

      case functionInvocation: UserDefinedFunctionInvocation =>
        apply(functionInvocation.asUnresolvedFunction)

      case Property(m, k) =>
        s"${inner(ast)(m)}.${apply(k)}"

      case MapExpression(items) =>
        items.map({
          case (k, i) => s"${apply(k)}: ${apply(i)}"
        }).mkString("{", ", ", "}")

      case Parameter(name, _, _) =>
        s"$$${backtick(name)}"

      case _: CountStar =>
        s"count(*)"

      case IsNull(arg) =>
        s"${inner(ast)(arg)} IS NULL"

      case IsNotNull(arg) =>
        s"${inner(ast)(arg)} IS NOT NULL"

      case e @ IsTyped(arg, predicateType) =>
        s"${inner(ast)(arg)} ${e.canonicalOperatorSymbol} ${predicateType.description}"

      case e @ IsNotTyped(arg, predicateType) =>
        s"${inner(ast)(arg)} ${e.canonicalOperatorSymbol} ${predicateType.description}"

      case ContainerIndex(exp, idx) =>
        s"${inner(ast)(exp)}[${inner(ast)(idx)}]"

      case ListSlice(list, start, end) =>
        val l = start.map(inner(ast)).getOrElse("")
        val r = end.map(inner(ast)).getOrElse("")
        s"${inner(ast)(list)}[$l..$r]"

      case PatternExpression(RelationshipsPattern(relChain)) =>
        patterns.apply(relChain)

      case AnyIterablePredicate(scope, expression) =>
        s"any${prettyScope(scope, expression)}"

      case Not(arg) =>
        s"NOT ${inner(ast)(arg)}"

      case ListComprehension(s, expression) =>
        val v = apply(s.variable)
        val p = s.innerPredicate.map(pr => " WHERE " + inner(ast)(pr)).getOrElse("")
        val e = s.extractExpression.map(ex => " | " + inner(ast)(ex)).getOrElse("")
        val expr = inner(ast)(expression)
        s"[$v IN $expr$p$e]"

      case PatternComprehension(variable, RelationshipsPattern(relChain), predicate, proj) =>
        val v = variable.map(apply).map(_ + " = ").getOrElse("")
        val p = patterns.apply(relChain)
        val w = predicate.map(inner(ast)).map(" WHERE " + _).getOrElse("")
        val b = inner(ast)(proj)
        s"[$v$p$w | $b]"

      case HasLabelsOrTypes(arg, labels) =>
        val l = labels.map(apply).mkString(":", ":", "")
        s"${inner(ast)(arg)}$l"

      case HasLabels(arg, labels) =>
        val l = labels.map(apply).mkString(":", ":", "")
        s"${inner(ast)(arg)}$l"

      case HasAnyLabel(arg, labels) =>
        val l = labels.map(apply).mkString(":", "|", "")
        s"${inner(ast)(arg)}$l"

      case HasTypes(arg, types) =>
        val l = types.map(apply).mkString(":", ":", "")
        s"${inner(ast)(arg)}$l"

      case lep: LabelExpressionPredicate =>
        s"${inner(ast)(lep.entity)}:${stringifyLabelExpression(lep.labelExpression)}"

      case AllIterablePredicate(scope, e) =>
        s"all${prettyScope(scope, e)}"

      case NoneIterablePredicate(scope, e) =>
        s"none${prettyScope(scope, e)}"

      case SingleIterablePredicate(scope, e) =>
        s"single${prettyScope(scope, e)}"

      case MapProjection(variable, items) =>
        val itemsText = items.map(apply).mkString(", ")
        s"${apply(variable)}{$itemsText}"

      case DesugaredMapProjection(variable, items, includeAllProps) =>
        val itemsText = {
          val allItems = if (!includeAllProps) items else items :+ AllPropertiesSelector()(InputPosition.NONE)
          allItems.map(apply).mkString(", ")
        }
        s"${apply(variable)}{$itemsText}"

      case LiteralEntry(k, e) =>
        s"${apply(k)}: ${inner(ast)(e)}"

      case VariableSelector(v) =>
        apply(v)

      case PropertySelector(v) =>
        s".${apply(v)}"

      case AllPropertiesSelector() => ".*"

      case CaseExpression(expression, alternatives, default) =>
        Seq(
          Seq("CASE"),
          for { e <- expression.toSeq; i <- Seq(inner(ast)(e)) } yield i,
          for { (e1, e2) <- alternatives; i <- Seq("WHEN", inner(ast)(e1), "THEN", inner(ast)(e2)) } yield i,
          for { e <- default.toSeq; i <- Seq("ELSE", inner(ast)(e)) } yield i,
          Seq("END")
        ).flatten.mkString(" ")

      case Ands(expressions) =>
        type ChainOp = Expression with ChainableBinaryOperatorExpression

        def findChain: Option[List[ChainOp]] = {
          val chainable = expressions.collect { case e: ChainableBinaryOperatorExpression => e }
          def allChainable = chainable.size == expressions.size
          def formsChain = chainable.sliding(2).forall(p => p.head.rhs == p.last.lhs)
          if (allChainable && formsChain) Some(chainable.toList) else None
        }

        findChain match {
          case Some(chain) =>
            val head = apply(chain.head)
            val tail = chain.tail.flatMap(o => List(o.canonicalOperatorSymbol, inner(ast)(o.rhs)))
            (head :: tail).mkString(" ")
          case None =>
            expressions.map(x => inner(ast)(x)).mkString(" AND ")
        }

      case AndsReorderable(expressions) =>
        val ands = Ands(expressions)(InputPosition.NONE)
        s"(${apply(ands)})"

      case AndedPropertyInequalities(_, _, exprs) =>
        exprs.map(apply).toIndexedSeq.mkString(" AND ")

      case Ors(expressions) =>
        expressions.map(x => inner(ast)(x)).mkString(" OR ")

      case ShortestPathExpression(pattern) =>
        patterns.apply(pattern)

      case PathExpression(pathStep) =>
        pathSteps(pathStep)

      case ReduceExpression(ReduceScope(Variable(acc), Variable(identifier), expression), init, list) =>
        val a = backtick(acc)
        val v = backtick(identifier)
        val i = inner(ast)(init)
        val l = inner(ast)(list)
        val e = inner(ast)(expression)
        s"reduce($a = $i, $v IN $l | $e)"

      case _: ExtractScope | _: FilterScope | _: ReduceScope =>
        // These are not really expressions, they are part of expressions
        ""

      case ExistsExpression(q) =>
        val p = prettifier.asString(q)
        s"EXISTS { $p }"

      case CollectExpression(q) =>
        val p = prettifier.asString(q)
        s"COLLECT { $p }"

      case CountExpression(q) =>
        val p = prettifier.asString(q)
        s"COUNT { $p }"

      case UnaryAdd(r) =>
        val i = inner(ast)(r)
        s"+$i"

      case UnarySubtract(r) =>
        val i = inner(ast)(r)
        s"-$i"

      case CoerceTo(expr, typ) =>
        apply(expr)

      case CoerceToPredicate(expr) =>
        val inner = apply(expr)
        s"CoerceToPredicate($inner)"

      case AssertIsNode(argument) =>
        s"assertIsNode(${apply(argument)})"

      case e @ ElementIdToLongId(_, _, elementIdExpr) =>
        val prefix = e match {
          case ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Single, _) =>
            "elementIdToNodeId"
          case ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Many, _) =>
            "elementIdListToNodeIdList"
          case ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Single, _) =>
            "elementIdToRelationshipId"
          case ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Many, _) =>
            "elementIdListToRelationshipIdList"
        }
        s"$prefix(${apply(elementIdExpr)})"

      case NoneOfRelationships(rel, relList) => s"NOT ${apply(rel)} IN ${apply(relList)}"

      case DifferentRelationships(rel1, rel2) => s"NOT ${apply(rel1)} = ${apply(rel2)}"

      case Disjoint(rel1, rel2) => s"disjoint(${apply(rel1)}, ${apply(rel2)})"

      case Unique(rel) => s"unique(${apply(rel)})"

      case VarLengthLowerBound(relName, bound) => s"size(${apply(relName)}) >= $bound"
      case VarLengthUpperBound(relName, bound) => s"size(${apply(relName)}) <= $bound"

      case IsRepeatTrailUnique(argument) =>
        s"isRepeatTrailUnique(${apply(argument)})"

      case _ =>
        extensionStringifier(this)(ast)
    }
  }

  private def prettyScope(s: FilterScope, expression: Expression) = {
    Seq(
      for { i <- Seq(apply(s.variable), "IN", inner(s)(expression)) } yield i,
      for { p <- s.innerPredicate.toSeq; i <- Seq("WHERE", inner(s)(p)) } yield i
    ).flatten.mkString("(", " ", ")")
  }

  sealed trait Binding
  case object Syntactic extends Binding
  case class Precedence(level: Int) extends Binding

  private def binding(in: Expression): Binding = in match {
    case _: Or |
      _: Ors =>
      Precedence(12)

    case _: Xor =>
      Precedence(11)

    case _: And |
      _: Ands =>
      Precedence(10)

    case _: Not =>
      Precedence(9)

    case _: Equals |
      _: NotEquals |
      _: InvalidNotEquals |
      _: GreaterThan |
      _: GreaterThanOrEqual |
      _: LessThan |
      _: LessThanOrEqual =>
      Precedence(8)

    case _: Add |
      _: Subtract =>
      Precedence(7)

    case _: Multiply |
      _: Divide |
      _: Modulo =>
      Precedence(6)

    case _: Pow =>
      Precedence(5)

    case _: UnaryAdd |
      _: UnarySubtract =>
      Precedence(4)

    case _: RegexMatch |
      _: In |
      _: StartsWith |
      _: EndsWith |
      _: Contains |
      _: IsNull |
      _: IsNotNull |
      _: IsTyped |
      _: IsNotTyped =>
      Precedence(3)

    case _: Property |
      _: HasLabels |
      _: ContainerIndex |
      _: ListSlice =>
      Precedence(2)

    case _ =>
      Syntactic

  }

  override def backtick(txt: String): String = {
    ExpressionStringifier.backtick(txt, alwaysBacktick)
  }

  override def quote(txt: String): String = {
    val str = txt.replaceAll("\\\\", "\\\\\\\\")
    val containsSingle = str.contains('\'')
    val containsDouble = str.contains('"')
    if (containsDouble && containsSingle)
      "\"" + str.replaceAll("\"", "\\\\\"") + "\""
    else if (containsDouble || preferSingleQuotes)
      "'" + str + "'"
    else
      "\"" + str + "\""
  }

  override def escapePassword(password: Expression): String = password match {
    case _: SensitiveAutoParameter if !sensitiveParamsAsParams => "'******'"
    case _: SensitiveLiteral                                   => "'******'"
    case param: Parameter                                      => s"$$${ExpressionStringifier.backtick(param.name)}"
    case _                                                     => throw new InternalError("illegal password expression")
  }

  override def stringifyLabelExpression(labelExpression: LabelExpression): String = labelExpression match {
    case le: Disjunctions =>
      le.children.map(stringifyLabelExpressionHalfAtom).mkString("|")
    case le: ColonDisjunction =>
      s"${stringifyLabelExpressionInColonDisjunction(le.lhs)}|:${stringifyLabelExpressionHalfAtom(le.rhs)}"
    case le: Conjunctions =>
      le.children.map(stringifyLabelExpressionHalfAtom).mkString("&")
    case le: ColonConjunction =>
      s"${stringifyLabelExpressionInColonConjunction(le.lhs)}:${stringifyLabelExpressionHalfAtom(le.rhs)}"
    case le => s"${stringifyLabelExpressionHalfAtom(le)}"
  }

  private def stringifyLabelExpressionInColonDisjunction(labelExpression: LabelExpression): String =
    labelExpression match {
      case le: ColonDisjunction =>
        s"${stringifyLabelExpressionInColonDisjunction(le.lhs)}|:${stringifyLabelExpressionHalfAtom(le.rhs)}"
      case le => s"${stringifyLabelExpressionHalfAtom(le)}"
    }

  private def stringifyLabelExpressionInColonConjunction(labelExpression: LabelExpression): String =
    labelExpression match {
      case le: ColonConjunction =>
        s"${stringifyLabelExpressionInColonConjunction(le.lhs)}:${stringifyLabelExpressionHalfAtom(le.rhs)}"
      case le => s"${stringifyLabelExpressionHalfAtom(le)}"
    }

  private def stringifyLabelExpressionHalfAtom(labelExpression: LabelExpression): String = labelExpression match {
    case le: Negation => s"!${stringifyLabelExpressionHalfAtom(le.e)}"
    case le           => s"${stringifyLabelExpressionAtom(le)}"
  }

  private def stringifyLabelExpressionAtom(labelExpression: LabelExpression): String = labelExpression match {
    case Leaf(name, _) => apply(name)
    case _: Wildcard   => s"%"
    case le            => s"(${stringifyLabelExpression(le)})"
  }
}

object ExpressionStringifier {

  def apply(
    extensionStringifier: ExpressionStringifier.Extension,
    alwaysParens: Boolean,
    alwaysBacktick: Boolean,
    preferSingleQuotes: Boolean,
    sensitiveParamsAsParams: Boolean
  ): ExpressionStringifier = new DefaultExpressionStringifier(
    extensionStringifier,
    alwaysParens,
    alwaysBacktick,
    preferSingleQuotes,
    sensitiveParamsAsParams
  )

  def apply(
    extender: Expression => String = failingExtender,
    alwaysParens: Boolean = false,
    alwaysBacktick: Boolean = false,
    preferSingleQuotes: Boolean = false,
    sensitiveParamsAsParams: Boolean = false
  ): ExpressionStringifier = new DefaultExpressionStringifier(
    Extension.simple(extender),
    alwaysParens,
    alwaysBacktick,
    preferSingleQuotes,
    sensitiveParamsAsParams
  )

  /**
   * Generates pretty strings from expressions.
   */
  def pretty(onFailure: Expression => String): ExpressionStringifier = {
    new PrettyExpressionStringifier(ExpressionStringifier(onFailure))
  }

  trait Extension {
    def apply(ctx: ExpressionStringifier)(expression: Expression): String
  }

  object Extension {

    def simple(func: Expression => String): Extension = new Extension {
      def apply(ctx: ExpressionStringifier)(expression: Expression): String = func(expression)
    }
  }

  /*
   * Some strings (identifiers) were escaped with back-ticks to allow non-identifier characters
   * When printing these again, the knowledge of the back-ticks is lost, but the same test for
   * non-identifier characters can be used to recover that knowledge.
   */
  def backtick(txt: String, alwaysBacktick: Boolean = false, globbing: Boolean = false): String = {
    def escaped = txt.replaceAll("`", "``")
    def orGlobbedCharacter(p: Int) = globbing && (p == '*'.asInstanceOf[Int] || p == '?'.asInstanceOf[Int])

    if (alwaysBacktick)
      s"`$escaped`"
    else {
      val isJavaIdentifier =
        Strings.codePoints(txt).limit(1).allMatch(p =>
          UnicodeHelper.isIdentifierStart(p) || orGlobbedCharacter(p)
        ) &&
          Strings.codePoints(txt).skip(1).allMatch(p => UnicodeHelper.isIdentifierPart(p) || orGlobbedCharacter(p))
      if (!isJavaIdentifier)
        s"`$escaped`"
      else
        txt
    }
  }

  val failingExtender: Expression => String =
    e => throw new IllegalStateException(s"failed to pretty print $e")
}
