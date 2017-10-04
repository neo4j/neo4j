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
package org.neo4j.cypher.internal.frontend.v3_4.rewriters

import org.neo4j.cypher.internal.util.v3_4.{ASTNode, Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTList, CTString}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.IdentityMap

object literalReplacement {

  case class LiteralReplacement(parameter: Parameter, value: AnyRef)
  type LiteralReplacements = IdentityMap[Expression, LiteralReplacement]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = rewriter.apply(that)

    private val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case l: Expression if replaceableLiterals.contains(l) => replaceableLiterals(l).parameter
    })
  }

  private val literalMatcher: PartialFunction[Any, LiteralReplacements => (LiteralReplacements, Option[LiteralReplacements => LiteralReplacements])] = {
    case _: Match |
         _: Create |
         _: CreateUnique |
         _: Merge |
         _: SetClause |
         _: Return |
         _: With |
         _: CallClause =>
      acc => (acc, Some(identity))
    case _: Clause |
         _: PeriodicCommitHint |
         _: Limit =>
      acc => (acc, None)
    case n: NodePattern =>
      acc => (n.properties.treeFold(acc)(literalMatcher), None)
    case r: RelationshipPattern =>
      acc => (r.properties.treeFold(acc)(literalMatcher), None)
    case ContainerIndex(_, _: StringLiteral) =>
      acc => (acc, None)
    case _: GraphUrl =>
      acc => (acc, None)
    case l: StringLiteral =>
      acc =>
        if (acc.contains(l)) (acc, None) else {
          val parameter = Parameter(s"  AUTOSTRING${acc.size}", CTString)(l.position)
          (acc + (l -> LiteralReplacement(parameter, l.value)), None)
        }
    case l: IntegerLiteral =>
      acc =>
        if (acc.contains(l)) (acc, None) else {
          val parameter = Parameter(s"  AUTOINT${acc.size}", CTInteger)(l.position)
          (acc + (l -> LiteralReplacement(parameter, l.value)), None)
        }
    case l: DoubleLiteral =>
      acc =>
        if (acc.contains(l)) (acc, None) else {
          val parameter = Parameter(s"  AUTODOUBLE${acc.size}", CTFloat)(l.position)
          (acc + (l -> LiteralReplacement(parameter, l.value)), None)
        }
    case l: BooleanLiteral =>
      acc =>
        if (acc.contains(l)) (acc, None) else {
          val parameter = Parameter(s"  AUTOBOOL${acc.size}", CTBoolean)(l.position)
          (acc + (l -> LiteralReplacement(parameter, l.value)), None)
        }
    case l: ListLiteral if l.expressions.forall(_.isInstanceOf[Literal]) =>
      acc =>
        if (acc.contains(l)) (acc, None) else {
          val parameter = Parameter(s"  AUTOLIST${acc.size}", CTList(CTAny))(l.position)
          val values: Seq[AnyRef] = l.expressions.map(_.asInstanceOf[Literal].value).toIndexedSeq
          (acc + (l -> LiteralReplacement(parameter, values)), None)
        }
  }

  private def doIt(term: ASTNode) = {
    val replaceableLiterals = term.treeFold(IdentityMap.empty: LiteralReplacements)(literalMatcher)

    val extractedParams: Map[String, AnyRef] = replaceableLiterals.map {
      case (_, LiteralReplacement(parameter, value)) => (parameter.name, value)
    }

    (ExtractParameterRewriter(replaceableLiterals), extractedParams)
  }

  def apply(term: ASTNode, paramExtraction: LiteralExtraction): (Rewriter, Map[String, Any]) = paramExtraction match {
    case Never =>
      Rewriter.noop -> Map.empty
    case Forced =>
      doIt(term)
    case IfNoParameter =>
      val containsParameter: Boolean = term.treeExists {
        case _: Parameter => true
      }

      if (containsParameter) Rewriter.noop -> Map.empty
      else doIt(term)
  }
}

sealed trait LiteralExtraction
case object Forced extends LiteralExtraction
case object IfNoParameter extends LiteralExtraction
case object Never extends LiteralExtraction
