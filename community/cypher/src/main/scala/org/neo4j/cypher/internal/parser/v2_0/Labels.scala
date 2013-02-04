/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands.expressions.{Expression, Literal}
import org.neo4j.cypher.internal.commands.values.{LabelValue, LabelName}
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.commands.AstNode
import org.neo4j.cypher.SyntaxException

trait Labels extends Base {
  def labelLit: Parser[Literal] = ":" ~> identity ^^ { x => Literal(LabelName(x)) }

  def labelShortForm: Parser[LabelSet] = rep1(labelLit) ^^ {
    case litList =>
      LabelSet(Literal(litList.map(_.v)))
  }

  private def labelExpr: Parser[LabelSet] = expression ^^ (expr => LabelSet(expr))

  private def labelKeywordForm: Parser[LabelSet] = ignoreCase("label") ~> (labelShortForm | labelExpr)

  private def labelGroupsForm: Parser[LabelSpec] = rep1sep(labelShortForm, "|") ^^ {
    case labelSets => LabelChoice(labelSets: _*).simplify
  }

  def labelChoiceForm: Parser[LabelSpec] = labelGroupsForm | labelKeywordForm

  def optLabelChoiceForm: Parser[LabelSpec] = opt(labelChoiceForm) ^^ {
    case optSpec => optSpec.getOrElse(NoLabels)
  }

  def labelLongForm: Parser[LabelSet] = labelShortForm | labelKeywordForm

  def optLabelLongForm: Parser[OptLabelSet] = opt(labelLongForm) ^^ {
    case optSpec => optSpec.getOrElse(NoLabels)
  }

  def expression: Parser[Expression]
}

sealed abstract class LabelSpec extends AstNode[LabelSpec] {
  def allSets: Seq[LabelSet]
  def bare: Boolean

  def asOptLabelSet: OptLabelSet = throw new SyntaxException("Required single label set but found many")
  def asLabelSet: LabelSet = throw new SyntaxException("Required single label set but found none or too many")
  def simplify: LabelSpec = this

  def rewrite(f: Expression => Expression): LabelSpec
  def children: Seq[Expression]
  def symbolTableDependencies: Set[String]
  def throwIfSymbolsMissing(symbols: SymbolTable)
}

sealed abstract class OptLabelSet extends LabelSpec {
  final override def asOptLabelSet: OptLabelSet = this
  override def rewrite(f: Expression => Expression): OptLabelSet
}

case object NoLabels extends OptLabelSet {
  def allSets = Seq.empty
  def bare = true


  override def rewrite(f: Expression => Expression): OptLabelSet = NoLabels
  def children: Seq[Expression] = Seq.empty
  def symbolTableDependencies: Set[String] = Set.empty

  def throwIfSymbolsMissing(symbols: SymbolTable) { }
}

object LabelSet {
  def fromStrings(elems: String*) = LabelSet(Literal(Seq(elems.map(LabelName(_)): _*)))
}

final case class LabelSet(expr: Expression) extends OptLabelSet {

  override def allSets = Seq(this)
  def bare = false

  override def asLabelSet: LabelSet = this

  override def rewrite(f: Expression => Expression): LabelSet= LabelSet(expr.rewrite(f))
  override val children: Seq[Expression] = Seq(expr)
  def symbolTableDependencies: Set[String] = expr.symbolTableDependencies

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    expr.throwIfSymbolsMissing(symbols)
  }

}

final case class LabelChoice(override val allSets: LabelSet*) extends LabelSpec {
  def bare = allSets.isEmpty

  override def rewrite(f: Expression => Expression): LabelChoice = LabelChoice(allSets.map(_.rewrite(f)): _*)
  override def children: Seq[Expression] = allSets.map(_.expr)
  def symbolTableDependencies: Set[String] = allSets.map(_.symbolTableDependencies).fold(Set.empty)(_ ++ _)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    allSets.foreach(_.throwIfSymbolsMissing(symbols))
  }

  override def simplify: LabelSpec =
    if (allSets.isEmpty)
      NoLabels
    else
      if (allSets.tail.isEmpty) allSets.head else this
}
