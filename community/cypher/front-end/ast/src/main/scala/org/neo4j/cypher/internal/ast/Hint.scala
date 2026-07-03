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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintSpec
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintType
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ExpandHints
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.liftSemanticEitherFunc
import org.neo4j.cypher.internal.ast.semantics.optionSemanticChecking
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

sealed trait Hint {
  def variables: NonEmptyList[Variable]

  def coveredBy(overlappingIds: Set[LogicalVariable]): Boolean =
    variables.forall(overlappingIds.contains)
}

object Hint {

  implicit val byVariable: Ordering[Hint] =
    Ordering.by { (hint: Hint) => hint.variables.head }(Variable.byName)
}

/**
 * A hint that was specified by the user in the query and can appear in `Clause.hints`.
 * The parser emits these, and semantic analysis runs over them.
 *
 * Pure IR-only hints (`UsingExpandStepHint`, `UsingStatefulShortestPath*`) are NOT `AstHint`s,
 * so the type system prevents them from being placed into `Clause.hints`.
 */
sealed trait AstHint extends Hint with ASTNode with SemanticCheckable with SemanticAnalysisTooling

/**
 * A hint that can appear in `QueryGraph.hints`.
 * AST-only hints like `UsingExpandHint` are NOT `IrHint`s, so the type system
 * prevents them from being placed into `QueryGraph.hints`.
 *
 * These are never introduced from the parser. Thus, no need for semantic checks or input positions.
 *
 * Because most hints are both [[AstHint]] as well as [[IrHint]], this trait is placed in the `front-end` module
 */
sealed trait IrHint extends Hint

sealed trait LeafPlanHint extends AstHint with IrHint {
  def variable: Variable

  override def variables: NonEmptyList[Variable] = NonEmptyList(variable)

  override def semanticCheck: SemanticCheck =
    ensureDefined(variable) chain expectType(CTNode.covariant | CTRelationship.covariant, variable)

}

case class UsingIndexHint(
  variable: Variable,
  labelOrRelType: LabelOrRelTypeName,
  properties: Seq[PropertyKeyName],
  spec: UsingIndexHintSpec = UsingIndexHint.SeekOrScan,
  indexType: UsingIndexHintType = UsingIndexHint.UsingAnyIndexType
)(val position: InputPosition) extends LeafPlanHint

object UsingIndexHint {

  sealed trait UsingIndexHintSpec {
    def fulfilledByScan: Boolean
  }

  case object SeekOnly extends UsingIndexHintSpec {
    override def fulfilledByScan: Boolean = false
  }

  case object SeekOrScan extends UsingIndexHintSpec {
    override def fulfilledByScan: Boolean = true
  }

  sealed trait UsingIndexHintType
  case object UsingAnyIndexType extends UsingIndexHintType
  case object UsingRangeIndexType extends UsingIndexHintType
  case object UsingTextIndexType extends UsingIndexHintType
  case object UsingPointIndexType extends UsingIndexHintType
}

case class UsingScanHint(variable: Variable, labelOrRelType: LabelOrRelTypeName)(val position: InputPosition)
    extends LeafPlanHint

case class UsingJoinHint(variables: NonEmptyList[Variable])(val position: InputPosition)
    extends AstHint with IrHint {

  override def semanticCheck: SemanticCheck =
    variables.foldSemanticCheck {
      variable => ensureDefined(variable) chain expectType(CTNode.covariant, variable)
    }
}

/**
 * Optional qualifier on an [[ExpandStep]] that constrains the `Expand`'s
 * `ExpansionMode` for that step: [[ExpandHintAll]] forces `ExpandAll`,
 * [[ExpandHintInto]] forces `ExpandInto`.
 */
sealed trait ExpandHintMode
case object ExpandHintAll extends ExpandHintMode
case object ExpandHintInto extends ExpandHintMode

/**
 * One comma-separated step of a `USING EXPAND ...` chain. At least one of
 *   - `from` + `to`  (direction)
 *   - `via`          (relationship-name discriminator)
 * is required. This is enforced by the parser.
 */
case class ExpandStep(
  from: Option[Variable],
  to: Option[Variable],
  via: Option[Variable],
  mode: Option[ExpandHintMode]
)(val position: InputPosition) extends ASTNode {

  def variablesIterator: Iterator[Variable] =
    from.iterator ++ to.iterator ++ via.iterator
}

object ExpandStep {

  def byEndpoints(
    from: Variable,
    to: Variable,
    via: Option[Variable] = None,
    mode: Option[ExpandHintMode] = None
  )(position: InputPosition): ExpandStep =
    ExpandStep(Some(from), Some(to), via, mode)(position)

  def byRelationship(via: Variable, mode: Option[ExpandHintMode] = None)(position: InputPosition): ExpandStep =
    ExpandStep(from = None, to = None, via = Some(via), mode = mode)(position)
}

/**
 * [[AstHint]] for a `USING EXPAND ...` clause. One object per `USING` clause, holding
 * the comma-separated steps in textual order.
 *
 * `ClauseConverters` then translates it into one or more [[UsingExpandStepHint]] as its [[IrHint]] representation.
 */
case class UsingExpandHint(
  steps: NonEmptyList[ExpandStep]
)(val position: InputPosition)
    extends AstHint {

  override def variables: NonEmptyList[Variable] =
    NonEmptyList.from(steps.iterator.flatMap(_.variablesIterator))

  override def semanticCheck: SemanticCheck = {
    requireFeatureSupport("`USING EXPAND`", ExpandHints, position) ifOkChain
      steps.foldSemanticCheck { step =>
        val endpointTypeCheck =
          step.from.foldSemanticCheck(v => ensureDefined(v) chain expectType(CTNode.covariant, v)) chain
            step.to.foldSemanticCheck(v => ensureDefined(v) chain expectType(CTNode.covariant, v))

        val viaTypeCheck =
          step.via.foldSemanticCheck { v =>
            ensureDefined(v) chain
              expectType(CTRelationship.covariant | CTList(CTRelationship).covariant, v)
          }

        endpointTypeCheck chain viaTypeCheck
      }
  }
}

/**
 * Opaque identifier for an [[ExpandStep]]. Derived from the step's source
 * position, so it is stable across AST rewrites that rename variables.
 */
case class UsingExpandStepId(position: InputPosition)

/**
 * Atomic [[IrHint]] representing a single expand step plus the set of
 * prior step IDs that must already be solved by the source plan before this
 * step's hint can itself be claimed. This set enables us to hint on expand order.
 *
 * Created by `ClauseConverters` from an AST [[UsingExpandHint]].
 *
 * @param mustFollow to enforce the order of expands, these expands need to be solved before this
 */
case class UsingExpandStepHint(
  from: Option[Variable],
  to: Option[Variable],
  via: Option[Variable],
  mode: Option[ExpandHintMode],
  stepId: UsingExpandStepId,
  mustFollow: Set[UsingExpandStepId]
) extends IrHint {

  override def variables: NonEmptyList[Variable] =
    NonEmptyList.from(from.toList ++ to.toList ++ via.toList)
}

sealed trait UsingStatefulShortestPathHint extends IrHint

case class UsingStatefulShortestPathInto(override val variables: NonEmptyList[Variable])
    extends UsingStatefulShortestPathHint

case class UsingStatefulShortestPathAll(override val variables: NonEmptyList[Variable])
    extends UsingStatefulShortestPathHint
