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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternPart.CountedSelector
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.BucketSize
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.SizeBucket
import org.neo4j.cypher.internal.util.UnknownSize
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType

object literalReplacement {

  type LiteralReplacements = IdentityMap[Expression, AutoExtractedParameter]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = rewriter.apply(that)

    private val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case l: Expression if replaceableLiterals.contains(l) => replaceableLiterals(l)
    })
  }

  private val literalMatcher
    : PartialFunction[Any, LiteralReplacements => Foldable.FoldingBehavior[LiteralReplacements]] = {
    case _: Match |
      _: CreateOrInsert |
      _: Merge |
      _: SetClause |
      _: Return |
      _: With |
      _: SubqueryCall |
      _: Unwind |
      _: CallClause =>
      acc => TraverseChildren(acc)
    case _: Clause |
      _: Limit |
      _: Skip |
      _: GraphPatternQuantifier |
      _: CountedSelector =>
      acc => SkipChildren(acc)
    case n: NodePattern =>
      acc => SkipChildren(n.properties.folder.treeFold(acc)(literalMatcher))
    case r: RelationshipPattern =>
      acc => SkipChildren(r.properties.folder.treeFold(acc)(literalMatcher))
    case ContainerIndex(_, _: StringLiteral) =>
      acc => SkipChildren(acc)
    case l: StringLiteral =>
      acc =>
        if (acc.contains(l)) SkipChildren(acc)
        else {
          SkipChildren(acc + (l -> createParameter(
            l,
            s"  AUTOSTRING${acc.size}",
            CTString,
            SizeBucket.computeBucket(l.value.length)
          )))
        }
    case l: IntegerLiteral =>
      acc =>
        if (acc.contains(l)) SkipChildren(acc)
        else {
          SkipChildren(acc + (l -> createParameter(l, s"  AUTOINT${acc.size}", CTInteger)))
        }
    case l: DoubleLiteral =>
      acc =>
        if (acc.contains(l)) SkipChildren(acc)
        else {
          SkipChildren(acc + (l -> createParameter(l, s"  AUTODOUBLE${acc.size}", CTFloat)))
        }
    case l: ListLiteral if l.expressions.forall(_.isInstanceOf[Literal]) =>
      acc =>
        if (acc.contains(l)) SkipChildren(acc)
        else {
          // NOTE: we need to preserve inner type for Strings since that allows us to use the text index, for other types
          //       we would end up with the same plan anyway so there is no need to keep the inner type.
          val cypherType = {
            if (l.expressions.nonEmpty && l.expressions.forall(_.isInstanceOf[StringLiteral])) CTList(CTString)
            else CTList(CTAny)
          }
          SkipChildren(acc + (l -> createParameter(
            l,
            s"  AUTOLIST${acc.size}",
            cypherType,
            SizeBucket.computeBucket(l.expressions.size)
          )))
        }
  }

  private def createParameter(
    l: Expression,
    name: String,
    typ: CypherType,
    sizeHint: BucketSize = UnknownSize
  ): AutoExtractedParameter =
    AutoExtractedParameter(name, typ, sizeHint)(l.position)

  private def doIt(term: ASTNode) = {
    val replaceableLiterals: LiteralReplacements =
      term.folder.treeFold(IdentityMap.empty: LiteralReplacements)(literalMatcher)

    val extractedParams = replaceableLiterals.map {
      case (e, parameter) => parameter -> e
    }

    (ExtractParameterRewriter(replaceableLiterals), extractedParams)
  }

  def apply(
    term: ASTNode,
    paramExtraction: LiteralExtractionStrategy
  ): (Rewriter, Map[AutoExtractedParameter, Expression]) =
    paramExtraction match {
      case Never =>
        Rewriter.noop -> Map.empty
      case Forced =>
        doIt(term)
      case IfNoParameter =>
        val containsParameter: Boolean = term.folder.treeExists {
          case _: Parameter => true
        }

        if (containsParameter) Rewriter.noop -> Map.empty
        else doIt(term)
    }
}

sealed trait LiteralExtractionStrategy
case object Forced extends LiteralExtractionStrategy
case object IfNoParameter extends LiteralExtractionStrategy
case object Never extends LiteralExtractionStrategy
