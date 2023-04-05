/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan.VERBOSE_TO_STRING
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.logical.plans.Trail.VariableGrouping
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.Identifiable
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.util.Preconditions

import java.lang.reflect.Method

import scala.annotation.tailrec
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

// -------
// HELPERS
// -------

case object Flattener extends LogicalPlans.Mapper[Seq[LogicalPlan]] {
  override def onLeaf(plan: LogicalPlan): Seq[LogicalPlan] = Seq(plan)

  override def onOneChildPlan(plan: LogicalPlan, source: Seq[LogicalPlan]): Seq[LogicalPlan] = plan +: source

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Seq[LogicalPlan], rhs: Seq[LogicalPlan]): Seq[LogicalPlan] =
    (plan +: lhs) ++ rhs

  def create(plan: LogicalPlan): Seq[LogicalPlan] =
    LogicalPlans.map(plan, this)
}

sealed trait IndexUsage {
  def identifier: String
}

final case class SchemaLabelIndexUsage(
  identifier: String,
  labelId: Int,
  label: String,
  propertyTokens: Seq[PropertyKeyToken]
) extends IndexUsage

final case class SchemaRelationshipIndexUsage(
  identifier: String,
  relTypeId: Int,
  relType: String,
  propertyTokens: Seq[PropertyKeyToken]
) extends IndexUsage

final case class SchemaIndexLookupUsage(identifier: String, entityType: EntityType) extends IndexUsage

trait IndexSeekNames {
  def PLAN_DESCRIPTION_INDEX_SCAN_NAME: String
  def PLAN_DESCRIPTION_INDEX_SEEK_NAME: String
  def PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME: String
  def PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME: String
  def PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME: String
  def PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME: String
}

// ---------------------------------
// LOGICAL_PLAN AND ABSTRACT CLASSES
// ---------------------------------

object LogicalPlan {
  val LOWEST_TX_LAYER = 0
  val VERBOSE_TO_STRING = false
}

/**
 * A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
 * non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
 * application of the corresponding operator on the relations generated by its children, the result of which is then sent
 * further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
 * to data in the database, to the root, which is the final operator producing the query answer.
 */
sealed abstract class LogicalPlan(idGen: IdGen)
    extends Product
    with Foldable
    with Rewritable
    with Identifiable {

  self =>

  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]
  def availableSymbols: Set[String]

  override val id: Id = idGen.id()

  override val hashCode: Int = MurmurHash3.productHash(self)

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[LogicalPlan]) false
    else {
      val otherPlan = obj.asInstanceOf[LogicalPlan]
      if (this.eq(otherPlan)) return true
      if (this.getClass != otherPlan.getClass) return false
      val stack = new mutable.ArrayStack[(Iterator[Any], Iterator[Any])]()
      var p1 = this.productIterator
      var p2 = otherPlan.productIterator
      while (p1.hasNext && p2.hasNext) {
        val continue =
          (p1.next(), p2.next()) match {
            case (lp1: LogicalPlan, lp2: LogicalPlan) =>
              if (lp1.getClass != lp2.getClass) {
                false
              } else {
                stack.push((p1, p2))
                p1 = lp1.productIterator
                p2 = lp2.productIterator
                true
              }
            case (_: LogicalPlan, _) => false
            case (_, _: LogicalPlan) => false
            case (a1, a2)            => a1 == a2
          }

        if (!continue) return false
        while (!p1.hasNext && !p2.hasNext && stack.nonEmpty) {
          val (p1New, p2New) = stack.pop()
          p1 = p1New
          p2 = p2New
        }
      }
      p1.isEmpty && p2.isEmpty
    }
  }

  def leaves: Seq[LogicalPlan] = this.folder.treeFold(Seq.empty[LogicalPlan]) {
    case plan: LogicalPlan if plan.lhs.isEmpty && plan.rhs.isEmpty => acc => TraverseChildren(acc :+ plan)
  }

  @tailrec
  final def leftmostLeaf: LogicalPlan = lhs match {
    case Some(plan) => plan.leftmostLeaf
    case None       => this
  }

  def copyPlanWithIdGen(idGen: IdGen): LogicalPlan = {
    try {
      val arguments = this.treeChildren.toList :+ idGen
      copyConstructor.invoke(this, arguments: _*).asInstanceOf[this.type]
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("wrong number of arguments") =>
        throw new InternalException(
          "Logical plans need to be case classes, and have the IdGen in a separate constructor",
          e
        )
    }
  }

  lazy val copyConstructor: Method = this.getClass.getMethods.find(_.getName == "copy").get

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toIndexedSeq
      val resultingPlan =
        if (
          params.length == args.length + 1
          && params.last.isAssignableFrom(classOf[IdGen])
        )
          constructor.invoke(this, args :+ SameId(this.id): _*).asInstanceOf[this.type]
        else if (
          (params.length == args.length + 2)
          && params(params.length - 2).isAssignableFrom(classOf[SinglePlannerQuery])
          && params(params.length - 1).isAssignableFrom(classOf[IdGen])
        )
          constructor.invoke(this, args :+ SameId(this.id): _*).asInstanceOf[this.type]
        else
          constructor.invoke(this, args: _*).asInstanceOf[this.type]
      resultingPlan
    }

  def isLeaf: Boolean = lhs.isEmpty && rhs.isEmpty

  override def toString: String = {
    if (VERBOSE_TO_STRING) {
      verboseToString
    } else {
      LogicalPlanToPlanBuilderString(this)
    }
  }

  def verboseToString: String = {
    def planRepresentation(plan: LogicalPlan): String = {
      val children = plan.lhs.toIndexedSeq ++ plan.rhs.toIndexedSeq
      val nonChildFields = plan.productIterator.filterNot(children.contains).mkString(", ")
      val prodPrefix = plan.productPrefix
      s"$prodPrefix($nonChildFields)"
    }

    LogicalPlanTreeRenderer.render(this, "| ", planRepresentation)
  }

  def satisfiesExpressionDependencies(e: Expression): Boolean =
    e.dependencies.map(_.name).forall(availableSymbols.contains)

  def debugId: String = f"0x$hashCode%08x"

  def flatten: Seq[LogicalPlan] = Flattener.create(this)

  def readOnly: Boolean = !this.folder.treeExists {
    case _: UpdatingPlan => true
  }

  def indexUsage(): Seq[IndexUsage] = {
    this.folder.fold(Seq.empty[IndexUsage]) {
      case MultiNodeIndexSeek(indexPlans) =>
        acc => acc ++ indexPlans.flatMap(_.indexUsage())
      case NodeByLabelScan(idName, _, _, _) =>
        acc => acc :+ SchemaIndexLookupUsage(idName, EntityType.NODE)
      case DirectedRelationshipTypeScan(idName, _, _, _, _, _) =>
        acc => acc :+ SchemaIndexLookupUsage(idName, EntityType.RELATIONSHIP)
      case UndirectedRelationshipTypeScan(idName, _, _, _, _, _) =>
        acc => acc :+ SchemaIndexLookupUsage(idName, EntityType.RELATIONSHIP)
      case relIndexScan: RelationshipIndexLeafPlan =>
        acc =>
          acc :+
            SchemaRelationshipIndexUsage(
              relIndexScan.idName,
              relIndexScan.typeToken.nameId.id,
              relIndexScan.typeToken.name,
              relIndexScan.properties.map(_.propertyKeyToken)
            )
      case nodeIndexPlan: NodeIndexLeafPlan =>
        acc =>
          acc :+
            SchemaLabelIndexUsage(
              nodeIndexPlan.idName,
              nodeIndexPlan.label.nameId.id,
              nodeIndexPlan.label.name,
              nodeIndexPlan.properties.map(_.propertyKeyToken)
            )
    }
  }
}

// Marker interface for all plans that aggregate inputs.
sealed trait AggregatingPlan extends LogicalPlan {
  def groupingExpressions: Map[String, Expression]
  def aggregationExpressions: Map[String, Expression]
}

// Marker interface for all plans that performs updates
sealed trait UpdatingPlan extends LogicalUnaryPlan {
  override def withLhs(source: LogicalPlan)(idGen: IdGen): UpdatingPlan
}

// Marker trait for relationship type scans
sealed trait RelationshipTypeScan {
  def idName: String
}

sealed abstract class LogicalBinaryPlan(idGen: IdGen) extends LogicalPlan(idGen) {
  final lazy val hasUpdatingRhs: Boolean = right.folder.treeExists { case _: UpdatingPlan => true }
  final def lhs: Option[LogicalPlan] = Some(left)
  final def rhs: Option[LogicalPlan] = Some(right)

  def left: LogicalPlan
  def right: LogicalPlan

  /**
   * A copy of this plan with a new LHS
   */
  def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan

  /**
   * A copy of this plan with a new LHS
   */
  def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan
}

sealed abstract class LogicalUnaryPlan(idGen: IdGen) extends LogicalPlan(idGen) {
  final def lhs: Option[LogicalPlan] = Some(source)
  final def rhs: Option[LogicalPlan] = None

  def source: LogicalPlan

  /**
   * A copy of this plan with a new LHS
   */
  def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan
}

sealed abstract class LogicalLeafPlan(idGen: IdGen) extends LogicalPlan(idGen) {
  final def lhs: Option[LogicalPlan] = None
  final def rhs: Option[LogicalPlan] = None
  def argumentIds: Set[String]

  def usedVariables: Set[String]

  def withoutArgumentIds(argsToExclude: Set[String]): LogicalLeafPlan
}

sealed abstract class NodeLogicalLeafPlan(idGen: IdGen) extends LogicalLeafPlan(idGen) {
  def idName: String
}

sealed abstract class RelationshipLogicalLeafPlan(idGen: IdGen) extends LogicalLeafPlan(idGen) {
  def idName: String
  def leftNode: String
  def rightNode: String
  def directed: Boolean
}

sealed trait MultiEntityLogicalLeafPlan extends PhysicalPlanningPlan {
  def idNames: Set[String]
}

sealed trait IndexedPropertyProvidingPlan {

  /**
   * All properties
   */
  def properties: Seq[IndexedProperty]

  /**
   * Indexed properties that will be retrieved from the index and cached in the row.
   */
  def cachedProperties: Seq[CachedProperty]

  /**
   * Create a copy of this plan, swapping out the properties
   */
  def withMappedProperties(f: IndexedProperty => IndexedProperty): IndexedPropertyProvidingPlan

  /**
   * Get a copy of this index plan where getting values is disabled
   */
  def copyWithoutGettingValues: IndexedPropertyProvidingPlan
}

sealed abstract class NodeIndexLeafPlan(idGen: IdGen) extends NodeLogicalLeafPlan(idGen)
    with IndexedPropertyProvidingPlan {
  def label: LabelToken

  override def cachedProperties: Seq[CachedProperty] = properties.flatMap(_.maybeCachedProperty(idName))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexLeafPlan

  override def copyWithoutGettingValues: NodeIndexLeafPlan

  def indexType: IndexType
}

sealed abstract class RelationshipIndexLeafPlan(idGen: IdGen) extends RelationshipLogicalLeafPlan(idGen)
    with IndexedPropertyProvidingPlan {
  def typeToken: RelationshipTypeToken

  override def cachedProperties: Seq[CachedProperty] = properties.flatMap(_.maybeCachedProperty(idName))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): RelationshipIndexLeafPlan

  override def copyWithoutGettingValues: RelationshipIndexLeafPlan

  def indexType: IndexType
}

sealed abstract class MultiNodeIndexLeafPlan(idGen: IdGen) extends LogicalLeafPlan(idGen)
    with MultiEntityLogicalLeafPlan
    with IndexedPropertyProvidingPlan {}

sealed abstract class NodeIndexSeekLeafPlan(idGen: IdGen) extends NodeIndexLeafPlan(idGen) {

  def valueExpr: QueryExpression[Expression]

  def properties: Seq[IndexedProperty]

  def indexOrder: IndexOrder

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexSeekLeafPlan
}

sealed abstract class MultiRelationshipIndexLeafPlan(idGen: IdGen) extends RelationshipLogicalLeafPlan(idGen)
    with MultiEntityLogicalLeafPlan
    with IndexedPropertyProvidingPlan {}

sealed abstract class RelationshipIndexSeekLeafPlan(idGen: IdGen) extends RelationshipIndexLeafPlan(idGen) {

  def valueExpr: QueryExpression[Expression]

  def properties: Seq[IndexedProperty]

  def indexOrder: IndexOrder

  def unique: Boolean

  def withNewLeftAndRightNodes(leftNode: String, rightNode: String): RelationshipIndexSeekLeafPlan

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): RelationshipIndexSeekLeafPlan
}

/**
 * A plan that limits selectivity on child plans.
 */
sealed trait LimitingLogicalPlan extends LogicalUnaryPlan

/**
 * A plan that eventually exhausts all input from LHS.
 */
sealed trait ExhaustiveLogicalPlan extends LogicalPlan

/**
 * A plan that exhausts all input from LHS before producing it's first output.
 */
sealed trait EagerLogicalPlan extends ExhaustiveLogicalPlan

/**
 * A plan that consumes only a single row from RHS for every row in LHS
 */
sealed trait SingleFromRightLogicalPlan extends LogicalBinaryPlan {
  final def source: LogicalPlan = left
  final def inner: LogicalPlan = right
}

/**
 * A leaf plan that is unaffected by changes to the transaction state after yielding the first row.
 */
sealed trait StableLeafPlan extends LogicalLeafPlan

sealed trait ProjectingPlan extends LogicalUnaryPlan {

  /**
   * override def withLhs(newLHS: LogicalPlan): LogicalUnaryPlan = copy(source = newLHS)
   */
  def projectExpressions: Map[String, Expression]

  def aliases: Map[String, Expression] =
    projectExpressions.filter { case (_, expr) => expr.isInstanceOf[LogicalVariable] }

  /**
   * Columns from the source plan that can be discarded after this plan.
   */
  def discardSymbols: Set[String] = Set.empty
}

sealed abstract class AbstractVarExpand(
  val from: String,
  val types: Seq[RelTypeName],
  val to: String,
  val nodePredicates: Seq[VariablePredicate],
  val relationshipPredicates: Seq[VariablePredicate],
  idGen: IdGen
) extends LogicalUnaryPlan(idGen) {

  def withNewPredicates(
    newNodePredicates: Seq[VariablePredicate],
    newRelationshipPredicates: Seq[VariablePredicate]
  )(idGen: IdGen): AbstractVarExpand
}

/**
 * Marker trait for all Apply plans. These are plans that execute their rhs
 * at least once for every lhs row, and pass the lhs as the argument to the rhs.
 */
sealed trait ApplyPlan extends LogicalBinaryPlan

object ApplyPlan {
  def unapply(applyPlan: ApplyPlan): Option[(LogicalPlan, LogicalPlan)] = Some((applyPlan.left, applyPlan.right))
}

sealed abstract class AbstractLetSelectOrSemiApply(left: LogicalPlan, idName: String)(idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan with SingleFromRightLogicalPlan {

  override val availableSymbols: Set[String] = left.availableSymbols + idName
}

sealed abstract class AbstractLetSemiApply(left: LogicalPlan, right: LogicalPlan, idName: String)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan with SingleFromRightLogicalPlan {

  override val availableSymbols: Set[String] = left.availableSymbols + idName
}

sealed abstract class AbstractSelectOrSemiApply(left: LogicalPlan)(idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan with SingleFromRightLogicalPlan {

  val availableSymbols: Set[String] = left.availableSymbols

  def expression: Expression
}

sealed abstract class AbstractSemiApply(left: LogicalPlan)(idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan with SingleFromRightLogicalPlan {

  val availableSymbols: Set[String] = left.availableSymbols
}

/**
 * Not sealed sub-hierarchy of command plans.
 */
abstract class CommandLogicalPlan(idGen: IdGen) extends LogicalLeafPlan(idGen = idGen) {

  def defaultColumns: List[ShowColumn]

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): CommandLogicalPlan = this

  // Always the first leaf plan, so arguments is always empty
  override def argumentIds: Set[String] = Set.empty

  override def availableSymbols: Set[String] = defaultColumns.map(_.variable.name).toSet
}

/**
 * Marker trait for all plans that are only introduced during physical planning.
 * (E.g. in [[pipelinedPrePhysicalPlanRewriter]])
 */
sealed trait PhysicalPlanningPlan extends LogicalPlan

/**
 * Marker trait for all plans that are only generated in tests.
 */
sealed trait TestOnlyPlan extends LogicalPlan

/**
 * Allows non-sealed sub-hierarchies of logical plans.
 */
abstract class LogicalPlanExtension(idGen: IdGen) extends LogicalPlan(idGen)

/**
 * Allows non-sealed sub-hierarchies of logical leaf plans.
 */
abstract class LogicalLeafPlanExtension(idGen: IdGen) extends LogicalLeafPlan(idGen)

// ----------------
// CONCRETE CLASSES
// ----------------

/**
 * Aggregation is a more advanced version of Distinct, where source rows are grouped by the
 * values of the groupingsExpressions. When the source is fully consumed, one row is produced
 * for every group, containing the values of the groupingExpressions for that row, as well as
 * aggregates computed on all the rows in that group.
 *
 * If there are no groupingExpressions, aggregates are computed over all source rows.
 */
case class Aggregation(
  override val source: LogicalPlan,
  override val groupingExpressions: Map[String, Expression],
  override val aggregationExpressions: Map[String, Expression]
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with EagerLogicalPlan with AggregatingPlan with ProjectingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val projectExpressions: Map[String, Expression] = groupingExpressions

  val groupingKeys: Set[String] = groupingExpressions.keySet

  val availableSymbols: Set[String] = groupingKeys ++ aggregationExpressions.keySet

}

/**
 * Produce one row for every node in the graph. Each row contains the contents of argument, and
 * a node assigned to the variable IdName.
 */
case class AllNodesScan(idName: String, argumentIds: Set[String])(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): AllNodesScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Produces exactly one row, if its source produces zero rows. Otherwise produces zero rows.
 * If a row is produced, that row will only contain values in the argument columns.
 * Anti can only be planned on the RHS of an Apply.
 */
case class Anti(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with PhysicalPlanningPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * AntiConditionalApply works like ConditionalApply, but with reversed condition.
 *
 * for ( leftRow <- left ) {
 *   if ( !condition( leftRow ) ) {
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     for ( rightRow <- right ) {
 *       produce rightRow
 *     }
 *   }
 * }
 */
case class AntiConditionalApply(override val left: LogicalPlan, override val right: LogicalPlan, items: Seq[String])(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols ++ items

}

/**
 * For every row in left, set that row as the argument, and produce all rows from right
 *
 * {{{
 * for ( leftRow <- left ) {
 *   right.setArgument( leftRow )
 *   for ( rightRow <- right ) {
 *     produce rightRow
 *   }
 * }
 * }}}
 */
case class Apply(override val left: LogicalPlan, override val right: LogicalPlan, fromSubquery: Boolean = false)(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * Produce a single row with the contents of argument
 */
case class Argument(argumentIds: Set[String] = Set.empty)(implicit idGen: IdGen) extends LogicalLeafPlan(idGen)
    with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): Argument =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Plan used to indicate that argument completion needs to be tracked
 *
 * NOTE: Only introduced by physical plan rewriter in pipelined runtime
 */
case class ArgumentTracker(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with PhysicalPlanningPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Produces one or zero rows containing the nodes with the given labels and property values.
 *
 * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
 * node is guaranteed per seek.
 */
case class AssertingMultiNodeIndexSeek(node: String, nodeIndexSeeks: Seq[NodeIndexSeekLeafPlan])(implicit idGen: IdGen)
    extends MultiNodeIndexLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] =
    nodeIndexSeeks.flatMap(_.availableSymbols).toSet

  override def usedVariables: Set[String] = nodeIndexSeeks.flatMap(_.usedVariables).toSet

  override def argumentIds: Set[String] =
    nodeIndexSeeks.flatMap(_.argumentIds).toSet

  override def cachedProperties: Seq[CachedProperty] =
    nodeIndexSeeks.flatMap(_.cachedProperties)

  override def properties: Seq[IndexedProperty] =
    nodeIndexSeeks.flatMap(_.properties)

  override def withoutArgumentIds(argsToExclude: Set[String]): MultiNodeIndexLeafPlan =
    copy(nodeIndexSeeks = nodeIndexSeeks.map(_.withoutArgumentIds(argsToExclude).asInstanceOf[NodeIndexSeekLeafPlan]))(
      SameId(this.id)
    )

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): MultiNodeIndexLeafPlan =
    AssertingMultiNodeIndexSeek(node, nodeIndexSeeks.map(_.withMappedProperties(f)))(SameId(this.id))

  override def copyWithoutGettingValues: AssertingMultiNodeIndexSeek =
    // NOTE: This is only used by a top-down rewriter (removeCachedProperties).
    // Since our generalized tree rewriters will descend into children (including Seq) we do not need to do anything
    this

  override def idNames: Set[String] =
    nodeIndexSeeks.map(_.idName).toSet
}

/**
 * Produces one or zero rows containing the relationships with the given labels and property values.
 *
 * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
 * relationship is guaranteed per seek.
 */
case class AssertingMultiRelationshipIndexSeek(
  relationship: String,
  leftNode: String,
  rightNode: String,
  directed: Boolean,
  relIndexSeeks: Seq[RelationshipIndexSeekLeafPlan]
)(
  implicit idGen: IdGen
) extends MultiRelationshipIndexLeafPlan(idGen) with StableLeafPlan with PhysicalPlanningPlan {

  override val availableSymbols: Set[String] =
    relIndexSeeks.flatMap(_.availableSymbols).toSet

  override def usedVariables: Set[String] = relIndexSeeks.flatMap(_.usedVariables).toSet

  override def argumentIds: Set[String] =
    relIndexSeeks.flatMap(_.argumentIds).toSet

  override def cachedProperties: Seq[CachedProperty] =
    relIndexSeeks.flatMap(_.cachedProperties)

  override def properties: Seq[IndexedProperty] =
    relIndexSeeks.flatMap(_.properties)

  override def withoutArgumentIds(argsToExclude: Set[String]): MultiRelationshipIndexLeafPlan =
    copy(relIndexSeeks =
      relIndexSeeks.map(_.withoutArgumentIds(argsToExclude).asInstanceOf[RelationshipIndexSeekLeafPlan])
    )(
      SameId(this.id)
    )

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): MultiRelationshipIndexLeafPlan =
    AssertingMultiRelationshipIndexSeek(
      relationship,
      leftNode,
      rightNode,
      directed,
      relIndexSeeks.map(_.withMappedProperties(f))
    )(SameId(this.id))

  override def copyWithoutGettingValues: AssertingMultiRelationshipIndexSeek =
    // NOTE: This is only used by a top-down rewriter (removeCachedProperties).
    // Since our generalized tree rewriters will descend into children (including Seq) we do not need to do anything
    this

  override def idNames: Set[String] =
    relIndexSeeks.map(_.idName).toSet

  override def idName: String = relationship
}

/**
 * For every row in left, assert that all rows in right produce the same value
 * for the variable IdName. Produce the rows from left.
 *
 * for ( leftRow <- left )
 *   for ( rightRow <- right )
 *     assert( leftRow(node) == rightRow(node) )
 *   produce leftRow
 *
 * This operator is planned for merges using unique index seeks.
 */
case class AssertSameNode(node: String, override val left: LogicalPlan, override val right: LogicalPlan)(implicit
idGen: IdGen) extends LogicalBinaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols + node
}

/**
 * For every row in left, assert that all rows in right produce the same value
 * for the variable idName. Produce the rows from left.
 *
 * {{{
 * for ( leftRow <- left )
 *   for ( rightRow <- right )
 *     assert( leftRow(idName) == rightRow(idName) )
 *   produce leftRow
 * }}}
 *
 * This operator is planned for merges using unique index seeks.
 */
case class AssertSameRelationship(
  idName: String,
  override val left: LogicalPlan,
  override val right: LogicalPlan
)(implicit idGen: IdGen) extends LogicalBinaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols + idName
}

/**
 * Used to solve queries like: `(start) [(innerStart)-->(innerEnd)]{i, j} (end)`,
 * if both `start` and `end` are bound, with a bidirectional search.
 *
 * @param left                              source plan
 * @param right                             2 options for the inner plan to repeat
 * @param repetition                        how many times to repeat the RHS on each partial result
 * @param start                             the outside node variable where the quantified pattern
 *                                          starts. Assumed to be present in the output of `left`.
 *                                          [[start]] (and for subsequent iterations [[innerEnd]]) is projected to [[innerStart]].
 * @param end                               the outside node variable where the quantified pattern
 *                                          ends. Projected in output if present.
 * @param innerStart                        the node variable where the inner pattern starts
 * @param innerEnd                          the node variable where the inner pattern ends.
 *                                          [[innerEnd]] will eventually be projected to [[end]] (if present).
 * @param nodeVariableGroupings             node variables to aggregate
 * @param relationshipVariableGroupings     relationship variables to aggregate
 * @param innerRelationships                all inner relationships, whether they get projected or not
 * @param previouslyBoundRelationships      all relationship variables of the same MATCH that are present in lhs
 * @param previouslyBoundRelationshipGroups all relationship group variables of the same MATCH that are present in lhs
 * @param reverseGroupVariableProjections   if `true` reverse the group variable lists
 */
case class BidirectionalRepeatTrail(
  override val left: LogicalPlan,
  override val right: RepeatOptions,
  repetition: Repetition,
  start: String,
  end: String,
  innerStart: String,
  innerEnd: String,
  nodeVariableGroupings: Set[VariableGrouping],
  relationshipVariableGroupings: Set[VariableGrouping],
  innerRelationships: Set[String],
  previouslyBoundRelationships: Set[String],
  previouslyBoundRelationshipGroups: Set[String],
  reverseGroupVariableProjections: Boolean
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)

  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = {
    newRHS match {
      case x: RepeatOptions => copy(right = x)(idGen)
      case _ => throw new IllegalArgumentException("BidirectionalRepeatTrail must have RepeatOptions as its RHS.")
    }
  }

  override val availableSymbols: Set[String] =
    left.availableSymbols + end + start ++ nodeVariableGroupings.map(_.groupName) ++ relationshipVariableGroupings.map(
      _.groupName
    )
}

/**
 * Two options that both solve the inner part of a QPP.
 *
 * @param left solves the inner part of the QPP by starting from the left node.
 * @param right solves the inner part of the QPP by starting from the right node.
 */
case class RepeatOptions(
  override val left: LogicalPlan,
  override val right: LogicalPlan
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override def availableSymbols: Set[String] = left.availableSymbols
}

/**
 * Reads properties of a set of nodes or relationships and caches them in the current row.
 * Later accesses to this property can then read from this cache instead of reading from the store.
 */
case class CacheProperties(override val source: LogicalPlan, properties: Set[LogicalProperty])(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Cartesian Product
 *
 * {{{
 * for ( leftRow <- left )
 *   for ( rightRow <- right )
 *     produce (leftRow merge rightRow)
 * }}}
 */
case class CartesianProduct(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  fromSubquery: Boolean = false
)(implicit idGen: IdGen) extends LogicalBinaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * This is a variation of apply, which only executes 'right' if all variables in 'items' != NO_VALUE.
 *
 * for ( leftRow <- left ) {
 *   if ( condition( leftRow ) ) {
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     for ( rightRow <- right ) {
 *       produce rightRow
 *     }
 *   }
 * }
 */
case class ConditionalApply(override val left: LogicalPlan, override val right: LogicalPlan, items: Seq[String])(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols ++ items
}

/**
 * For each input row, create new nodes and relationships.
 */
case class Create(override val source: LogicalPlan, nodes: Seq[CreateNode], relationships: Seq[CreateRelationship])(
  implicit idGen: IdGen
) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = {
    source.availableSymbols ++ nodes.map(_.idName) ++ relationships.map(_.idName)
  }
}

/**
 * For each input row, delete the entity specified by 'expression'. Entity can be a node, relationship or path.
 */
case class DeleteExpression(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the node specified by 'expression' from the graph.
 */
case class DeleteNode(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the path specified by 'expression' from the graph.
 */
case class DeletePath(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the relationship specified by 'expression' from the graph.
 */
case class DeleteRelationship(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the entity specified by 'expression' from the graph. If the entity is a
 *   node) all it's relationships are also deleted
 *   path) all nodes in the path and all their relationships are deleted.
 */
case class DetachDeleteExpression(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the node specified by 'expression' and all its relationships from the graph.
 */
case class DetachDeleteNode(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For each input row, delete the path specified by 'expression' from the graph. All nodes in the path and all their
 * relationships are deleted.
 */
case class DetachDeletePath(override val source: LogicalPlan, expression: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Scans all relationships and produces one row for each relationship it finds.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedAllRelationshipsScan(
  idName: String,
  startNode: String,
  endNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedAllRelationshipsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * For each relationship element id in 'relIds', fetch the corresponding relationship. For each relationship,
 * produce one row containing:
 *   - argument
 *   - the relationship as 'idName'
 *   - the start node as 'startNode'
 *   - the end node as 'endNode'
 */
case class DirectedRelationshipByElementIdSeek(
  idName: String,
  relIds: SeekableArgs,
  startNode: String,
  endNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) {

  val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = relIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipByElementIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * For each relationship id in 'relIds', fetch the corresponding relationship. For each relationship,
 * produce one row containing:
 *   - argument
 *   - the relationship as 'idName'
 *   - the start node as 'startNode'
 *   - the end node as 'endNode'
 */
case class DirectedRelationshipByIdSeek(
  idName: String,
  relIds: SeekableArgs,
  startNode: String,
  endNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) {

  val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = relIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipByIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * This operator does a full scan of an index, producing rows for all entries that contain a string value.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipIndexContainsScan(
  idName: String,
  startNode: String,
  endNode: String,
  override val typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipIndexContainsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipIndexContainsScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): RelationshipIndexLeafPlan =
    copy(property = f(property))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * This operator does a full scan of an index, producing rows for all entries that end with a string value
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipIndexEndsWithScan(
  idName: String,
  startNode: String,
  endNode: String,
  override val typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipIndexEndsWithScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipIndexEndsWithScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): DirectedRelationshipIndexEndsWithScan =
    copy(property = f(property))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * This operator does a full scan of an index, producing one row per entry.
 *
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipIndexScan(
  idName: String,
  startNode: String,
  endNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipIndexScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipIndexScan =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): DirectedRelationshipIndexScan =
    copy(properties = properties.map(f))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * For every relationship with the given type and property values, produces rows with that relationship.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipIndexSeek(
  idName: String,
  startNode: String,
  endNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends RelationshipIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): DirectedRelationshipIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def unique: Boolean = false

  override def directed: Boolean = true

  override def withNewLeftAndRightNodes(leftNode: String, rightNode: String): RelationshipIndexSeekLeafPlan =
    copy(startNode = leftNode, endNode = rightNode)
}

object DirectedRelationshipIndexSeek extends IndexSeekNames {
  override val PLAN_DESCRIPTION_INDEX_SCAN_NAME = "DirectedRelationshipIndexScan"
  override val PLAN_DESCRIPTION_INDEX_SEEK_NAME = "DirectedRelationshipIndexSeek"
  override val PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME = "DirectedRelationshipIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME = "DirectedRelationshipUniqueIndexSeek"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME = "DirectedRelationshipUniqueIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME = "DirectedRelationshipUniqueIndexSeek(Locking)"
}

/**
 * Scans the relationship by type and produces one row for each relationship it finds.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipTypeScan(
  idName: String,
  startNode: String,
  relType: RelTypeName,
  endNode: String,
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with RelationshipTypeScan with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipTypeScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * Produces one or zero rows containing relationship with the given type and property values.
 *
 * This operator is used on type/property combinations under uniqueness constraint.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipUniqueIndexSeek(
  idName: String,
  startNode: String,
  endNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends RelationshipIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipUniqueIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipUniqueIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): DirectedRelationshipUniqueIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def unique: Boolean = true

  override def directed: Boolean = true

  override def withNewLeftAndRightNodes(leftNode: String, rightNode: String): RelationshipIndexSeekLeafPlan =
    copy(startNode = leftNode, endNode = rightNode)

}

/**
 * Produce one row for every relationship in the graph that has at least one of the provided types. 
 * This row contains the relationship (assigned to 'idName') and the contents of argument.
 */
case class DirectedUnionRelationshipTypesScan(
  idName: String,
  startNode: String,
  types: Seq[RelTypeName],
  endNode: String,
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedUnionRelationshipTypesScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = true
}

/**
 * Distinct produces source rows without changing them, but omitting rows
 * which have been produced before. That is, the order of rows is unchanged, but each
 * unique combination of values is only produced once.
 */
case class Distinct(override val source: LogicalPlan, override val groupingExpressions: Map[String, Expression])(
  implicit idGen: IdGen
) extends LogicalUnaryPlan(idGen) with ProjectingPlan with AggregatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val projectExpressions: Map[String, Expression] = groupingExpressions
  override val availableSymbols: Set[String] = groupingExpressions.keySet

  override def aggregationExpressions: Map[String, Expression] = Map.empty
}

/**
 * Consumes and buffers all source rows, marks the transaction as stable, and then produces all rows.
 */
case class Eager(
  override val source: LogicalPlan,
  reasons: ListSet[EagernessReason.Reason] = ListSet(EagernessReason.Unknown)
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with EagerLogicalPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/*
 * Produce zero rows, regardless of source.
 */
case class EmptyResult(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with EagerLogicalPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Throws exception if evaluated.
 */
case class ErrorPlan(override val source: LogicalPlan, exception: Exception)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/*
 * Only produce the first 'count' rows from source but exhausts the source. Used for plan where the source has side effects that need to happen
 * regardless of the limit.
 */
case class ExhaustiveLimit(override val source: LogicalPlan, count: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with ExhaustiveLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

object Expand {
  sealed trait ExpansionMode

  /**
   * Expand relationships (a)-[r]-(b) for a given a, and populate r and b
   */
  case object ExpandAll extends ExpansionMode

  /**
   * Expand relationships (a)-[r]-(b) for a given a and b, and populate r
   */
  case object ExpandInto extends ExpansionMode

  case class VariablePredicate(variable: LogicalVariable, predicate: Expression)
}

/**
 * For every source row, traverse all the relationships of 'from' which fulfill the
 * provided constraints. Produce one row per traversed relationships, and add the
 * relationship and end node as values on the produced rows.
 */
case class Expand(
  override val source: LogicalPlan,
  from: String,
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  to: String,
  relName: String,
  mode: ExpansionMode = ExpandAll
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to
}

/**
 * This works exactly like Expand, but if no matching relationships are found, a single
 * row is produced instead populated by the argument, and the 'relName' and 'to' variables
 * are set to NO_VALUE.
 */
case class OptionalExpand(
  override val source: LogicalPlan,
  from: String,
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  to: String,
  relName: String,
  mode: ExpansionMode = ExpandAll,
  predicate: Option[Expression] = None
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to
}

/**
 * For every source row, explore all homogeneous paths starting in 'from', that fulfill the provided
 * criteria. Paths are homogeneous in that all relationships have to fulfill the same relationship
 * predicate, and all nodes have to fulfill the same node predicate. For each explored
 * path that is longer or equal to length.min, and shorter than length.max, a row is produced.
 *
 * The relationships and end node of the corresponding path are added to the produced row.
 */
case class VarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  projectedDir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  relName: String,
  length: VarPatternLength,
  mode: ExpansionMode = ExpandAll,
  override val nodePredicates: Seq[VariablePredicate] = Seq.empty,
  override val relationshipPredicates: Seq[VariablePredicate] = Seq.empty
)(implicit idGen: IdGen) extends AbstractVarExpand(from, types, to, nodePredicates, relationshipPredicates, idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + relName + to

  override def withNewPredicates(
    newNodePredicates: Seq[VariablePredicate],
    newRelationshipPredicates: Seq[VariablePredicate]
  )(idGen: IdGen): VarExpand =
    copy(nodePredicates = newNodePredicates, relationshipPredicates = newRelationshipPredicates)(idGen)

}

/**
 * In essence a VarExpand, where some paths are not explored if they could not produce an unseen
 * end node. Used to serve DISTINCT VarExpands where the individual paths are not of interest. This
 * operator does not guarantee unique end nodes, but it will produce less of them than the regular
 * VarExpand.
 *
 * Only the end node is added to produced rows.
 */
case class PruningVarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  minLength: Int,
  maxLength: Int,
  override val nodePredicates: Seq[VariablePredicate] = Seq.empty,
  override val relationshipPredicates: Seq[VariablePredicate] = Seq.empty
)(implicit idGen: IdGen)
    extends AbstractVarExpand(from, types, to, nodePredicates, relationshipPredicates, idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + to

  override def withNewPredicates(
    newNodePredicates: Seq[VariablePredicate],
    newRelationshipPredicates: Seq[VariablePredicate]
  )(idGen: IdGen): PruningVarExpand =
    copy(nodePredicates = newNodePredicates, relationshipPredicates = newRelationshipPredicates)(idGen)
}

/**
 * In essence a VarExpand, where some paths are not explored if they could not produce an unseen
 * end node. Used to serve DISTINCT VarExpands where the individual paths are not of interest. This
 * operator does guarantee unique end nodes for a given input, and it will produce less of them than the regular
 * VarExpand.
 *
 * Only the end node is added to produced rows.
 */
case class BFSPruningVarExpand(
  override val source: LogicalPlan,
  override val from: String,
  dir: SemanticDirection,
  override val types: Seq[RelTypeName],
  override val to: String,
  includeStartNode: Boolean,
  maxLength: Int,
  depthName: Option[String],
  override val nodePredicates: Seq[VariablePredicate] = Seq.empty,
  override val relationshipPredicates: Seq[VariablePredicate] = Seq.empty
)(implicit idGen: IdGen)
    extends AbstractVarExpand(from, types, to, nodePredicates, relationshipPredicates, idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols + to

  override def withNewPredicates(
    newNodePredicates: Seq[VariablePredicate],
    newRelationshipPredicates: Seq[VariablePredicate]
  )(idGen: IdGen): BFSPruningVarExpand =
    copy(nodePredicates = newNodePredicates, relationshipPredicates = newRelationshipPredicates)(idGen)

}

case class PathPropagatingBFS(
  left: LogicalPlan,
  right: LogicalPlan,
  from: String,
  dir: SemanticDirection,
  projectedDir: SemanticDirection,
  types: Seq[RelTypeName],
  to: String,
  relName: String,
  length: VarPatternLength,
  nodePredicates: Seq[VariablePredicate] = Seq.empty,
  relationshipPredicates: Seq[VariablePredicate] = Seq.empty
)(implicit idGen: IdGen) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)

  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols + relName + to ++ right.availableSymbols

  def withNewPredicates(
    newNodePredicates: Seq[VariablePredicate],
    newRelationshipPredicates: Seq[VariablePredicate]
  )(idGen: IdGen): PathPropagatingBFS =
    copy(nodePredicates = newNodePredicates, relationshipPredicates = newRelationshipPredicates)(idGen)
}

/**
 * Find the shortest paths between two nodes, as specified by 'shortestPath'. For each shortest path found produce a
 * row containing the source row and the found path.
 */
case class FindShortestPaths(
  override val source: LogicalPlan,
  shortestPath: ShortestPathPattern,
  perStepNodePredicates: Seq[VariablePredicate] = Seq.empty,
  perStepRelPredicates: Seq[VariablePredicate] = Seq.empty,
  pathPredicates: Seq[Expression] = Seq.empty,
  withFallBack: Boolean = false,
  disallowSameNode: Boolean = true
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols ++ shortestPath.availableSymbols
}

/**
 * Foreach is an operator that performs the provided side-effects for each item in the provided list.
 */
case class Foreach(
  source: LogicalPlan,
  variable: String,
  expression: Expression,
  mutations: collection.Seq[SimpleMutatingPattern]
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override def availableSymbols: Set[String] = source.availableSymbols // NOTE: variable is not available outside
}

/**
 * ForeachApply is a side-effect type apply, which operates on a list value. Each left row is used to compute a
 * list, and each value in this list applied as the argument to right. Left rows are produced unchanged.
 *
 * {{{
 * for ( leftRow <- left)
 *   list <- expression.evaluate( leftRow )
 *   for ( value <- list )
 *     right.setArgument( value )
 *     for ( rightRow <- right )
 *       // just consume
 *
 *   produce leftRow
 * }}}
 */
case class ForeachApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  variable: String,
  expression: Expression
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] =
    left.availableSymbols // NOTE: right.availableSymbols and variable are not available outside
}

/**
 * Produce rows from a query state input stream. Only one input operator can
 * exist per logical plan tree, and it has to be the left-most leaf.
 *
 * @param nullable if there can be null values among the nodes, relationships or variables
 */
case class Input(nodes: Seq[String], relationships: Seq[String], variables: Seq[String], nullable: Boolean)(implicit
idGen: IdGen) extends LogicalLeafPlan(idGen) with StableLeafPlan {
  val availableSymbols: Set[String] = nodes.toSet ++ relationships.toSet ++ variables.toSet
  override def argumentIds: Set[String] = Set.empty
  override def usedVariables: Set[String] = Set.empty
  override def withoutArgumentIds(argsToExclude: Set[String]): Input = this
}

object Input {

  def apply(variables: Seq[String])(implicit idGen: IdGen): Input =
    new Input(Seq.empty, Seq.empty, variables, true)(idGen)
}

/**
 * Produce one row for every node in the graph that has all of the provided labels. This row contains the node (assigned to 'idName')
 * and the contents of argument.
 */
case class IntersectionNodeByLabelsScan(
  idName: String,
  labels: Seq[LabelName],
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): IntersectionNodeByLabelsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Variant of NodeHashJoin. Also builds a hash table using 'left' and produces merged left and right rows using this
 * table. In addition, also produces left rows with missing key values, and left rows that were not matched
 * by any right row. In these additional rows, variables from the opposing stream are set to NO_VALUE.
 *
 * This is equivalent to a left outer join in relational algebra.
 */
case class LeftOuterHashJoin(nodes: Set[String], override val left: LogicalPlan, override val right: LogicalPlan)(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with EagerLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * Find the shortest paths between two nodes, as specified by 'shortestPath'. For each shortest path found produce a
 * row containing the source row and the found path.
 */
case class LegacyFindShortestPaths(
  override val source: LogicalPlan,
  shortestPath: ShortestPathPattern,
  predicates: Seq[Expression] = Seq.empty,
  withFallBack: Boolean = false,
  disallowSameNode: Boolean = true
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols ++ shortestPath.availableSymbols
}

/**
 * Like LetSemiApply, but with a precondition 'expr'. If 'expr' is true, 'idName' will be set to true without
 * executing right.
 *
 * for ( leftRow <- left ) {
 *   if ( leftRow.evaluate( expr) ) {
 *     leftRow('idName') = true
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     leftRow('idName') =  right.nonEmpty
 *     produce leftRow
 *   }
 * }
 */
case class LetSelectOrSemiApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  idName: String,
  expr: Expression
)(implicit idGen: IdGen)
    extends AbstractLetSelectOrSemiApply(left, idName)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * Like LetAntiSemiApply, but with a precondition 'expr'. If 'expr' is true, 'idName' will be set to true without
 * executing right.
 *
 * for ( leftRow <- left ) {
 *   if ( leftRow.evaluate( expr) ) {
 *     leftRow('idName') = true
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     leftRow('idName') =  right.isEmpty
 *     produce leftRow
 *   }
 * }
 */
case class LetSelectOrAntiSemiApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  idName: String,
  expr: Expression
)(implicit idGen: IdGen)
    extends AbstractLetSelectOrSemiApply(left, idName)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * For every row in left, set that row as the argument, and apply to right. Produce left row, and set 'idName' =
 * true if right contains at least one row.
 *
 * for ( leftRow <- left ) {
 *   right.setArgument( leftRow )
 *   leftRow('idName') = right.nonEmpty
 *   produce leftRow
 * }
 */
case class LetSemiApply(override val left: LogicalPlan, override val right: LogicalPlan, idName: String)(implicit
idGen: IdGen) extends AbstractLetSemiApply(left, right, idName)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * For every row in left, set that row as the argument, and apply to right. Produce left row, and set 'idName' =
 * true if right contains no rows.
 *
 * for ( leftRow <- left ) {
 *   right.setArgument( leftRow )
 *   leftRow('idName') = right.isEmpty
 *   produce leftRow
 * }
 */
case class LetAntiSemiApply(override val left: LogicalPlan, override val right: LogicalPlan, idName: String)(implicit
idGen: IdGen) extends AbstractLetSemiApply(left, right, idName)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/*
 * Only produce the first 'count' rows from source.
 */
case class Limit(override val source: LogicalPlan, count: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with LimitingLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Operator which loads a CSV from some URL. For every source row, the CSV is loaded. Each CSV line is produced as a
 * row consisting of the current source row + one value holding the CSV line data.
 *
 * If the CSV file has headers, each line will represented in Cypher as a MapValue, if the file has no header, each
 * line will be a ListValue.
 */
case class LoadCSV(
  override val source: LogicalPlan,
  url: Expression,
  variableName: String,
  format: CSVFormat,
  fieldTerminator: Option[String],
  legacyCsvQuoteEscaping: Boolean,
  csvBufferSize: Int
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) {

  override val availableSymbols: Set[String] = source.availableSymbols + variableName

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
}

/**
 * Merge executes the inner plan and on each found row it executes `onMatch`. If there are no found rows
 * it will first run `createNodes` and `createRelationships` followed by `onCreate`
 */
case class Merge(
  read: LogicalPlan,
  createNodes: Seq[CreateNode],
  createRelationships: Seq[CreateRelationship],
  onMatch: Seq[SetMutatingPattern],
  onCreate: Seq[SetMutatingPattern],
  nodesToLock: Set[String]
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {
  override def source: LogicalPlan = read

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(read = newLHS)(idGen)

  override def availableSymbols: Set[String] = read.availableSymbols
}

/**
 * Produces one or zero rows containing the nodes with the given labels and property values.
 *
 * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
 * node is guaranteed per seek.
 */
case class MultiNodeIndexSeek(nodeIndexSeeks: Seq[NodeIndexSeekLeafPlan])(implicit idGen: IdGen)
    extends MultiNodeIndexLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] =
    nodeIndexSeeks.flatMap(_.availableSymbols).toSet

  override def usedVariables: Set[String] = nodeIndexSeeks.flatMap(_.usedVariables).toSet

  override def argumentIds: Set[String] =
    nodeIndexSeeks.flatMap(_.argumentIds).toSet

  override def cachedProperties: Seq[CachedProperty] =
    nodeIndexSeeks.flatMap(_.cachedProperties)

  override def properties: Seq[IndexedProperty] =
    nodeIndexSeeks.flatMap(_.properties)

  override def withoutArgumentIds(argsToExclude: Set[String]): MultiNodeIndexLeafPlan =
    copy(nodeIndexSeeks.map(_.withoutArgumentIds(argsToExclude).asInstanceOf[NodeIndexSeekLeafPlan]))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): MultiNodeIndexLeafPlan =
    MultiNodeIndexSeek(nodeIndexSeeks.map(_.withMappedProperties(f)))(SameId(this.id))

  override def copyWithoutGettingValues: MultiNodeIndexSeek =
    // NOTE: This is only used by a top-down rewriter (removeCachedProperties).
    // Since our generalized tree rewriters will descend into children (including Seq) we do not need to do anything
    this

  override def idNames: Set[String] =
    nodeIndexSeeks.map(_.idName).toSet
}

/**
 * For each node element id in 'nodeIds', fetch the corresponding node. Produce one row with the contents of argument and
 * the node (assigned to 'idName').
 */
case class NodeByElementIdSeek(idName: String, nodeIds: SeekableArgs, argumentIds: Set[String])(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = nodeIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeByElementIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * For each nodeId in 'nodeIds', fetch the corresponding node. Produce one row with the contents of argument and
 * the node (assigned to 'idName').
 */
case class NodeByIdSeek(idName: String, nodeIds: SeekableArgs, argumentIds: Set[String])(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = nodeIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeByIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Produce one row for every node in the graph labelled 'label'. This row contains the node (assigned to 'idName')
 * and the contents of argument.
 */
case class NodeByLabelScan(idName: String, label: LabelName, argumentIds: Set[String], indexOrder: IndexOrder)(implicit
idGen: IdGen) extends NodeLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeByLabelScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Produce a single row with the contents of argument and a new value 'idName'. For each label in 'labelNames' the
 * number of nodes with that label is fetched from the counts store. These counts are multiplied together, and the
 * result is assigned to 'idName'
 *
 * Returns only a single row, thus a StableLeafPlan.
 */
case class NodeCountFromCountStore(idName: String, labelNames: List[Option[LabelName]], argumentIds: Set[String])(
  implicit idGen: IdGen
) extends LogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = Set(idName)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeCountFromCountStore =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * Join two result streams using a hash table. 'Left' is completely consumed and buffered in a hash table, using a
 * tuple consisting of the values assigned to 'nodes'. For every 'right' row, lookup the corresponding 'left' rows
 * based on 'nodes'. For each corresponding left row, merge that with the current right row and produce.
 *
 * hashTable = {}
 * for ( leftRow <- left )
 *   group = hashTable.getOrUpdate( key( leftRow, nodes ), List[Row]() )
 *   group += leftRow
 *
 * for ( rightRow <- right )
 *   group = hashTable.get( key( rightRow, nodes ) )
 *   for ( leftRow <- group )
 *     produce (leftRow merge rightRow)
 */
case class NodeHashJoin(nodes: Set[String], override val left: LogicalPlan, override val right: LogicalPlan)(implicit
idGen: IdGen) extends LogicalBinaryPlan(idGen) with EagerLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * This operator does a full scan of an index, producing rows for all entries that contain a string value
 *
 * It's much slower than an index seek, since all index entries must be examined, but also much faster than an
 * all-nodes scan or label scan followed by a property value filter.
 */
case class NodeIndexContainsScan(
  idName: String,
  override val label: LabelToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends NodeIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeIndexContainsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeIndexContainsScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexLeafPlan =
    copy(property = f(property))(SameId(this.id))
}

/**
 * This operator does a full scan of an index, producing rows for all entries that end with a string value
 *
 * It's much slower than an index seek, since all index entries must be examined, but also much faster than an
 * all-nodes scan or label scan followed by a property value filter.
 */
case class NodeIndexEndsWithScan(
  idName: String,
  override val label: LabelToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends NodeIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeIndexEndsWithScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeIndexEndsWithScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexLeafPlan =
    copy(property = f(property))(SameId(this.id))
}

/**
 * This operator does a full scan of an index, producing one row per entry.
 */
case class NodeIndexScan(
  idName: String,
  override val label: LabelToken,
  properties: Seq[IndexedProperty],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends NodeIndexLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeIndexScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeIndexScan =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexLeafPlan =
    copy(properties = properties.map(f))(SameId(this.id))
}

/**
 * For every node with the given label and property values, produces rows with that node.
 */
case class NodeIndexSeek(
  idName: String,
  override val label: LabelToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends NodeIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))
}

object NodeIndexSeek extends IndexSeekNames {
  override val PLAN_DESCRIPTION_INDEX_SCAN_NAME: String = "NodeIndexScan"
  override val PLAN_DESCRIPTION_INDEX_SEEK_NAME = "NodeIndexSeek"
  override val PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME = "NodeIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME = "NodeUniqueIndexSeek"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME = "NodeUniqueIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME = "NodeUniqueIndexSeek(Locking)"
}

/**
 * Produces one or zero rows containing the node with the given label and property values.
 *
 * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
 * node is guaranteed.
 */
case class NodeUniqueIndexSeek(
  idName: String,
  override val label: LabelToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends NodeIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeUniqueIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeUniqueIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeUniqueIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))
}

/**
 * NOTE: This plan is only for testing
 */
case class NonFuseable(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with TestOnlyPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * NOTE: This plan is only for testing
 */
case class InjectCompilationError(override val source: LogicalPlan)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with TestOnlyPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * NOTE: This plan is only for testing
 */
case class NonPipelined(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with TestOnlyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

case class NullifyMetadata(override val source: LogicalPlan, key: String, planId: Int)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with PhysicalPlanningPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override def availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Produces source rows, unless source is empty. In that case, a single row is produced containing argument and any
 * non-argument variables set to NO_VALUE.
 */
case class Optional(override val source: LogicalPlan, protectedSymbols: Set[String] = Set.empty)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * OrderedAggregation is like Aggregation, except that it relies on the input coming
 * in a particular order, which it can leverage by keeping less state to aggregate at any given time.
 */
case class OrderedAggregation(
  override val source: LogicalPlan,
  override val groupingExpressions: Map[String, Expression],
  override val aggregationExpressions: Map[String, Expression],
  orderToLeverage: Seq[Expression]
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with AggregatingPlan with ProjectingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val projectExpressions: Map[String, Expression] = groupingExpressions

  val groupingKeys: Set[String] = groupingExpressions.keySet

  val availableSymbols: Set[String] = groupingKeys ++ aggregationExpressions.keySet

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    orderToLeverage.forall(exp => groupingExpressions.values.exists(_ == exp)),
    s"""orderToLeverage expressions can only be grouping expression values, i.e. the expressions _before_ the aggregation.
       |Grouping expressions: $groupingExpressions
       |   Order to leverage: $orderToLeverage
       |   """.stripMargin
  )

}

/**
 * OrderedDistinct is like Distinct, except that it relies on the input coming
 * * in a particular order, which it can leverage by keeping less state to aggregate at any given time.
 */
case class OrderedDistinct(
  override val source: LogicalPlan,
  override val groupingExpressions: Map[String, Expression],
  orderToLeverage: Seq[Expression]
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with ProjectingPlan with AggregatingPlan {

  override val projectExpressions: Map[String, Expression] = groupingExpressions
  override val availableSymbols: Set[String] = groupingExpressions.keySet

  override def aggregationExpressions: Map[String, Expression] = Map.empty

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    orderToLeverage.forall(exp => groupingExpressions.values.exists(_ == exp)),
    s"""orderToLeverage expressions can only be grouping expression values, i.e. the expressions _before_ the distinct.
       |Grouping expressions: $groupingExpressions
       |   Order to leverage: $orderToLeverage
       |   """.stripMargin
  )
}

/**
 * Given two inputs that are both sorted on the same columns (`sortedColumns`),
 * produce the 'left' rows, and the 'right' rows merged in that order.
 * This operator does not guarantee row uniqueness.
 */
case class OrderedUnion(left: LogicalPlan, right: LogicalPlan, sortedColumns: Seq[ColumnOrder])(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols intersect right.availableSymbols
}

/**
 * Given an input that is sorted on a prefix of columns, e.g. [a],
 * produce an output that is sorted on more columns, e.g. [a, b, c].
 *
 * @param skipSortingPrefixLength skip sorting so many rows at the beginning.
 *                                This is an improvement if we know that these rows are skipped afterwards anyway.
 */
case class PartialSort(
  override val source: LogicalPlan,
  alreadySortedPrefix: Seq[ColumnOrder],
  stillToSortSuffix: Seq[ColumnOrder],
  skipSortingPrefixLength: Option[Expression] = None
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  val availableSymbols: Set[String] = source.availableSymbols
}

/*
 * Consume rows that are already sorted by `alreadySortedPrefix`. Sort, in chunks, by `stillToSortSuffix`.
 * Only retain the first 'limit' rows, which are produced once the TopTable is full and the current
 * chunk is read completely.
 */
case class PartialTop(
  override val source: LogicalPlan,
  alreadySortedPrefix: Seq[ColumnOrder],
  stillToSortSuffix: Seq[ColumnOrder],
  limit: Expression,
  skipSortingPrefixLength: Option[Expression] = None
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with LimitingLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Plan used to indicate that order needs to be preserved.
 */
case class PreserveOrder(override val source: LogicalPlan)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)
    with EagerLogicalPlan with PhysicalPlanningPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Install a probe to observe data flowing through the query
 *
 * NOTE: This plan is only for testing
 */
case class Prober(override val source: LogicalPlan, probe: Probe)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with TestOnlyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

object Prober {

  trait Probe {

    /**
     * Called on each row that passes through this operator.
     *
     * NOTE: The row object is transient and any data that needs to be stored
     * should be copied before the call returns.
     *
     * @param row a CypherRow representation
     */
    def onRow(
      row: AnyRef,
      queryStatistics: QueryStatistics /*TODO with ExtendedQueryStatistics*/,
      transactionsCommitted: Int
    ): Unit

    /**
     * A name to identify the prober in debug information.
     * E.g. in pipelined runtime, the name will be included in the WorkIdentity.workDescription of the operator
     */
    def name: String = ""
  }

  object NoopProbe extends Probe {
    override def onRow(row: AnyRef, queryStatistics: QueryStatistics, transactionsCommitted: Int): Unit = {}
  }
}

/**
 * For every source row, call the procedure 'call'.
 *
 *   If the procedure returns a stream, produce one row per result in this stream with result appended to the row
 *   If the procedure returns void, produce the source row
 */
case class ProcedureCall(override val source: LogicalPlan, call: ResolvedCall)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] =
    source.availableSymbols ++ call.callResults.map { result => result.variable.name }
}

/**
 * For every source row, produce a row containing only the variables in 'columns'. The ProduceResult operator is
 * always planned as the root operator in a logical plan tree.
 */
case class ProduceResult(override val source: LogicalPlan, columns: Seq[String])(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * This operator adheres to the following semantics. When we have a relationship list in scope and want retrieve the end
 * nodes of the resulting path like,
 *
 * {{{
 *   WITH ... AS rels
 *   MATCH (start)-[rels*]->(end)
 * }}}
 *
 * it should be equivalent to
 *
 * {{{
 *   WITH ... AS rels
 *   MATCH (start)-[rels2*]->(end)
 *   WHERE rels = rels2
 * }}}
 *
 * ProjectEndpoints accepts some parameters who together describe a path:
 *  - a single relationship or a relationship list,
 *  - a list of relationship types,
 *  - the direction of the path,
 *  - length quantifiers if it's a var-length path,
 *  - the names of the start and end nodes, and whether they are in scope or not,
 * It then returns the start and end nodes of any paths which are induced by the relationship(s) and matches the pattern.
 * Just like in the example above. There are a couple of different cases that should be considered:
 *
 *  - If 'rels == NO_VALUE', or 'rels' doesn't induce a valid path, or 'rels' does not match the specified 'types',
 *    the operator will return nothing.
 *
 *  - If 'direction' is OUTGOING, then produce one row where start is set to the source node of the first relationship
 *   in rels, and end is set to the target node of the last relationship. If a given node is in scope, say start. Then
 *   we require that the node in scope is equal to the source node of the first relationship of the relationship list.
 *   Otherwise return nothing.
 *
 *   - If 'direction' is INCOMING, produce one row where start is set to the target node of the first relationship in
 *   the list, and end is set to the source node of the last relationship. If a given node is in scope, say start. Then
 *   we require that the node in scope is equal to the target node of the first relationship of the relationship list.
 *   Otherwise return nothing.
 *
 *   - if 'direction' is BOTH, produce rows for each path for which the relationship list is a valid path where
 *   the internal relationships may be oriented in any order. In most cases, this will only produce one row. I.e
 *   if for example,
 *
 *      rels = `[(0)-[0]->(1), (2)-[1]->(1)]`,
 *
 *   then there is only one way to orient the relationships to build a valid undirected path,
 *
 *      `(0)-[0]->(1)<-[1]-(2)`
 *
 *   and we would return one row where start = 0, end = 2. There are cases which will produce two rows. Consider
 *   for example a scenario where
 *
 *    rels = `[(0)-[0]->(1), (1)-[1]->(0)]`,
 *
 *   then there it's possible to create two valid undirected paths with the given relationship list,
 *
 *    `(0)-[0]->(1)-[1]->(0)`,
 *    `(1)<-[0]-(0)<-[1]-(1)`,
 *
 *   in which case we'd return two rows, one with start=end=0, and one with start=end=1.
 *
 *   ASSUMPTION:
 *   If neither the start or end node is in scope, then it's guaranteed that the relationship list is non-empty.
 *   Otherwise we'd essentially need to implement an all node scan inside projectEndpoints, as with nothing in scope
 *   the following
 *   {{{
 *    WITH [] AS rels
 *    MATCH (a)-[rels2*0..]->(b)
 *    WHERE rels = rels2
 *    return a, b
 *   }}},
 *   would return every single pair of nodes (a, a) in the graph.
 *
 */
case class ProjectEndpoints(
  override val source: LogicalPlan,
  rels: String,
  start: String,
  startInScope: Boolean,
  end: String,
  endInScope: Boolean,
  types: Seq[RelTypeName],
  direction: SemanticDirection,
  length: PatternLength
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  Preconditions.checkArgument(
    startInScope || endInScope || length.isSimple || length.asInstanceOf[VarPatternLength].min > 0,
    "Var length pattern including length 0, with no end node in scope, must not be solved by ProjectEndpoints."
  )

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols + rels + start + end
}

/**
 * For each source row produce:
 * - the projected expressions (`projectExpressions`)
 * - columns from the source that are not discarded (`discardSymbols`)
 *
 * For each entry in 'expressions', the produced row get an extra variable
 * name as the key, with the value of the expression.
 *
 * Implementations are allowed to ignore the `discardSymbols`
 * (for performance reasons for example).
 */
case class Projection(
  override val source: LogicalPlan,
  override val discardSymbols: Set[String],
  projectExpressions: Map[String, Expression]
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with ProjectingPlan {

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    discardSymbols.diff(source.availableSymbols).isEmpty,
    s"Unknown discard symbols: ${discardSymbols.diff(source.availableSymbols)}"
  )

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = (source.availableSymbols -- discardSymbols) ++ projectExpressions.keySet

  /**
   * Custom copy method to make rewriting work.
   */
  def copy(
    source: LogicalPlan = this.source,
    discardSymbols: Set[String] = this.discardSymbols,
    projectExpressions: Map[String, Expression] = this.projectExpressions
  )(implicit idGen: IdGen = this.idGen): Projection = {
    val newDiscardSymbols = discardSymbols.intersect(source.availableSymbols)
    Projection(source, newDiscardSymbols, projectExpressions)(idGen)
  }
}

/**
 * Produce a single row with the contents of argument and a new value 'idName'. For each
 * relationship type in 'typeNames', the number of relationship matching
 *
 *   (:startLabel)-[:type]->(:endLabel)
 *
 * is fetched from the counts store. These counts are summed, and the result is
 * assigned to 'idName'.
 *
 * Returns only a single row, thus a StableLeafPlan.
 */
case class RelationshipCountFromCountStore(
  idName: String,
  startLabel: Option[LabelName],
  typeNames: Seq[RelTypeName],
  endLabel: Option[LabelName],
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends LogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols = Set(idName)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): RelationshipCountFromCountStore =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * For each source row, the labels in 'labelNamed' are removed from the node 'idName'.
 * The source row is produced.
 */
case class RemoveLabels(override val source: LogicalPlan, idName: String, labelNames: Set[LabelName])(implicit
idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/**
 * Variant of NodeHashJoin. Also builds a hash table using 'left' and produces merged left and right rows using this
 * table. In addition, also produces left rows with missing key values, and right rows that were not matched
 * in the hash table. In these additional rows, variables from the opposing stream are set to NO_VALUE.
 *
 * This is equivalent to a right outer join in relational algebra.
 */
case class RightOuterHashJoin(nodes: Set[String], override val left: LogicalPlan, override val right: LogicalPlan)(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with EagerLogicalPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols

}

/**
 * RollUp is the inverse of the Unwind operator. For each left row,
 * right is executed. For each right row produced, a single column value
 * is extracted and inserted into a collection. which is assigned to 'collectionName'.
 * The left row is produced.
 *
 * It is used for sub queries that return collections, such as pattern expressions (returns
 * a collection of paths) and pattern comprehension.
 *
 */
case class RollUpApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  collectionName: String,
  variableToCollect: String
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols + collectionName
}

/**
 * For each source row, produce it if all predicates are true.
 */
case class Selection(predicate: Ands, override val source: LogicalPlan)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  assert(predicate.exprs.nonEmpty, "A selection plan should never be created without predicates")

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  def numPredicates: Int = predicate.exprs.size

  val availableSymbols: Set[String] = source.availableSymbols
}

object Selection {

  def apply(predicates: Seq[Expression], source: LogicalPlan)(implicit idGen: IdGen): Selection = {
    assert(predicates.nonEmpty, "A selection plan should never be created without predicates")
    Selection(Ands(predicates)(predicates.head.position), source)
  }
  case class LabelAndRelTypeInfo(labelInfo: Map[String, Set[LabelName]], relTypeInfo: Map[String, RelTypeName])
}

/**
 * Like SemiApply, but with a precondition 'expr'. If 'expr' is true, left row will be produced without
 * executing right.
 *{{{
 * for ( leftRow <- left ) {
 *   if ( leftRow.evaluate( expr) ) {
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     if ( right.nonEmpty ) {
 *       produce leftRow
 *     }
 *   }
 * }
 * }}}
 */
case class SelectOrSemiApply(override val left: LogicalPlan, override val right: LogicalPlan, expression: Expression)(
  implicit idGen: IdGen
) extends AbstractSelectOrSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * Like AntiSemiApply, but with a precondition 'expr'. If 'expr' is true, left row will be produced without
 * executing right.
 *{{{
 * for ( leftRow <- left ) {
 *   if ( leftRow.evaluate( expr) ) {
 *     produce leftRow
 *   } else {
 *     right.setArgument( leftRow )
 *     if ( right.isEmpty ) {
 *       produce leftRow
 *     }
 *   }
 * }
 *}}}
 */
case class SelectOrAntiSemiApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  expression: Expression
)(implicit idGen: IdGen)
    extends AbstractSelectOrSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * For every row in 'left', set that row as the argument, and apply to 'right'. Produce left row, but only if right
 * produces at least one row.
 *
 * for ( leftRow <- left ) {
 *   right.setArgument( leftRow )
 *   if ( right.nonEmpty ) {
 *     produce leftRow
 *   }
 * }
 */
case class SemiApply(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
    extends AbstractSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * For every row in 'left', set that row as the argument, and apply to 'right'. Produce left row, but only if right
 * produces no rows.
 *
 * for ( leftRow <- left ) {
 *   right.setArgument( leftRow )
 *   if ( right.isEmpty ) {
 *     produce leftRow
 *   }
 * }
 */
case class AntiSemiApply(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
    extends AbstractSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

/**
 * For each source row, add the labels in 'labelNamed' to the node 'idName'.
 * The source row is produced.
 */
case class SetLabels(override val source: LogicalPlan, idName: String, labelNames: Set[LabelName])(implicit
idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

case class SetNodeProperties(
  override val source: LogicalPlan,
  idName: String,
  items: Seq[(PropertyKeyName, Expression)]
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/**
 * for ( row <- source )
 *   node = row.get(idName)
 *   for ( (key,value) <- row.evaluate( expression ) )
 *     node.setProperty( key, value )
 *
 *   produce row
 */
case class SetNodePropertiesFromMap(
  override val source: LogicalPlan,
  idName: String,
  expression: Expression,
  removeOtherProps: Boolean
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/**
 * for ( row <- source )
 *   node = row.get(idName)
 *   node.setProperty( propertyKey, row.evaluate(value) )
 *
 *   produce row
 */
case class SetNodeProperty(
  override val source: LogicalPlan,
  idName: String,
  propertyKey: PropertyKeyName,
  value: Expression
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

case class SetProperties(
  override val source: LogicalPlan,
  entity: Expression,
  items: Seq[(PropertyKeyName, Expression)]
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * for ( row <- source )
 * thing = row.get(idName)
 * for ( (key,value) <- row.evaluate( expression ) )
 *     thing.setProperty( key, value )
 *
 * produce row
 */
case class SetPropertiesFromMap(
  override val source: LogicalPlan,
  entity: Expression,
  expression: Expression,
  removeOtherProps: Boolean
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * for ( row <- source )
 *   entity = row.get(idName)
 *   entity.setProperty( propertyKey, row.evaluate(value) )
 *
 *   produce row
 */
case class SetProperty(
  override val source: LogicalPlan,
  entity: Expression,
  propertyKey: PropertyKeyName,
  value: Expression
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

case class SetRelationshipProperties(
  override val source: LogicalPlan,
  idName: String,
  items: Seq[(PropertyKeyName, Expression)]
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/**
 * for ( row <- source )
 *   rel = row.get(idName)
 *   for ( (key,value) <- row.evaluate( expression ) )
 *     rel.setProperty( key, value )
 *
 *   produce row
 */
case class SetRelationshipPropertiesFromMap(
  override val source: LogicalPlan,
  idName: String,
  expression: Expression,
  removeOtherProps: Boolean
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/**
 * for ( row <- source )
 *   rel = row.get(idName)
 *   rel.setProperty( propertyKey, row.evaluate(value) )
 *
 *   produce row
 */
case class SetRelationshipProperty(
  override val source: LogicalPlan,
  idName: String,
  propertyKey: PropertyKeyName,
  expression: Expression
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan =
    copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + idName
}

/*
 * Produce source rows except the first 'count' rows, which are ignored.
 */
case class Skip(override val source: LogicalPlan, count: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Buffer all source rows and sort them according to 'sortItems'. Produce the rows in sorted order.
 */
case class Sort(override val source: LogicalPlan, sortItems: Seq[ColumnOrder])(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with EagerLogicalPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * SubqueryForeach is a side-effect type Apply. Each left row is applied as the argument to right. Left rows are produced unchanged.
 *
 * {{{
 * for ( leftRow <- left)
 *   right.setArgument( leftRow )
 *   for ( rightRow <- right )
 *     // just consume
 *
 *   produce leftRow
 * }}}
 */
case class SubqueryForeach(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] =
    left.availableSymbols // NOTE: right.availableSymbols are not available outside
}

/*
 * Sort source rows according to the ordering in 'sortItems'. Only retain the first 'limit' rows, which are
 * produced once source if fully consumed.
 */
case class Top(override val source: LogicalPlan, sortItems: Seq[ColumnOrder], limit: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with EagerLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Special case TOP for the case when we only want one element, and all others that have the same value (tied for first place)
 */
case class Top1WithTies(override val source: LogicalPlan, sortItems: Seq[ColumnOrder])(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with EagerLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Used to solve queries like: `(start) [(innerStart)-->(innerEnd)]{i, j} (end)`
 *
 * @param left                              source plan
 * @param right                             inner plan to repeat
 * @param repetition                        how many times to repeat the RHS on each partial result
 * @param start                             the outside node variable where the quantified pattern
 *                                          starts. Assumed to be present in the output of `left`.
 *                                          [[start]] (and for subsequent iterations [[innerEnd]]) is projected to [[innerStart]].
 * @param end                               the outside node variable where the quantified pattern
 *                                          ends. Projected in output if present.
 * @param innerStart                        the node variable where the inner pattern starts
 * @param innerEnd                          the node variable where the inner pattern ends.
 *                                          [[innerEnd]] will eventually be projected to [[end]] (if present).
 * @param nodeVariableGroupings             node variables to aggregate
 * @param relationshipVariableGroupings     relationship variables to aggregate
 * @param innerRelationships                all inner relationships, whether they get projected or not
 * @param previouslyBoundRelationships      all relationship variables of the same MATCH that are present in lhs
 * @param previouslyBoundRelationshipGroups all relationship group variables of the same MATCH that are present in lhs
 * @param reverseGroupVariableProjections   if `true` reverse the group variable lists
 */
case class Trail(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  repetition: Repetition,
  start: String,
  end: String,
  innerStart: String,
  innerEnd: String,
  nodeVariableGroupings: Set[VariableGrouping],
  relationshipVariableGroupings: Set[VariableGrouping],
  innerRelationships: Set[String],
  previouslyBoundRelationships: Set[String],
  previouslyBoundRelationshipGroups: Set[String],
  reverseGroupVariableProjections: Boolean
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] =
    left.availableSymbols + end + start ++ nodeVariableGroupings.map(_.groupName) ++ relationshipVariableGroupings.map(
      _.groupName
    )
}

object Trail {

  /**
   * Describes a variable that is exposed from a QuantifiedPath.
   *
   * @param singletonName the name of the singleton variable inside the QuantifiedPath.
   * @param groupName     the name of the group variable exposed outside of the QuantifiedPath.
   */
  case class VariableGrouping(singletonName: String, groupName: String)
}

/**
 * For every batchSize rows in left:
 *   Begin a new transaction
 *   For every row in batch:
 *     Evaluate RHS with left row as an argument
 *     Record right output rows
 *   Once the output from RHS is depleted:
 *     Commit transaction
 *     Produce all recorded output rows
 *
 * {{{
 * for ( batch <- left.grouped(batchSize) {
 *   beginTx()
 *   for ( leftRow <- batch ) {
 *     right.setArgument( leftRow )
 *     for ( rightRow <- right ) {
 *       record( rightRow )
 *     }
 *   }
 *   commitTx()
 *   for ( r <- recordedRows ) {
 *     produce r
 *   }
 * }
 * }}}
 */

case class TransactionApply(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  maybeReportAs: Option[String]
)(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): TransactionApply = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): TransactionApply = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] =
    left.availableSymbols ++ right.availableSymbols ++ maybeReportAs.toList
}

/**
 * Plan used to indicate that argument completion needs to be tracked
 *
 * NOTE: Only introduced by physical plan rewriter in pipelined runtime
 */
case class TransactionCommit(override val source: LogicalPlan, batchSize: Expression)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with PhysicalPlanningPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}

/**
 * For every batchSize rows in left:
 *   Begin a new transaction
 *   For every row in batch:
 *     Evaluate RHS with left row as an argument
 *     Record the original left row
 *   Once the output from all RHS in batch is depleted:
 *     Commit transaction
 *     Produce all recorded output rows
 *
 * {{{
 * for ( batch <- left.grouped(batchSize) ) {
 *   beginTx()
 *   for ( leftRow <- batch ) {
 *     right.setArgument( leftRow )
 *     for ( rightRow <- right ) {
 *       // just consume
 *     }
 *     record( leftRow )
 *   }
 *   commitTx()
 *   for ( r <- recordedRows ) {
 *     produce r
 *   }
 * }
 * }}}
 */

case class TransactionForeach(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  maybeReportAs: Option[String]
)(
  implicit idGen: IdGen
) extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): TransactionForeach = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): TransactionForeach = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ maybeReportAs.toList
}

object TransactionForeach {
  val defaultBatchSize: Long = 1000L
  val defaultOnErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorFail
}

/**
 * Triadic selection is used to solve a common query pattern:
 * MATCH (a)-->(b)-->(c) WHERE NOT (a)-->(c)
 *
 * If this query can be solved by starting from (a) and expand first (a)-->(b)
 * and expanding (b)-->(c), we can replace the filter with a triadic selection
 * that runs the (a)-->(b) as its left hand side, caching the results for use in
 * filtering the results of its right hand side which is the (b)-->(c) expands.
 * The filtering is based on the pattern expression predicate. The classical
 * example is the friend of a friend that is not already a friend, as shown above,
 * but this works for other cases too, like fof that is a friend.
 *
 * Since the two expands are done by sub-plans, they can be substantially more
 * complex than single expands. However, what patterns actually get here need to
 * be identified by the triadic selection finder.
 *
 * In effect the triadic selection interprets the predicate pattern in:
 *     MATCH (<source>){-->(build)}{-->(target)}
 *     WHERE NOT (<source>)-->(<target>)
 *
 * as the predicate:
 *
 * WHERE (<target>) NOT IN Set(<build>, for <source>)
 *
 * With a plan that looks like:
 *
 * +TriadicSelection (c) NOT IN (b)
 * | \
 * | +<target>       (b)-->(c)
 * | |
 * | +Argument       (b)
 * |
 * +<build>          (a)-->(b)
 * |
 * +<source>         (a)
 */
case class TriadicSelection(
  override val left: LogicalPlan,
  override val right: LogicalPlan,
  positivePredicate: Boolean,
  sourceId: String,
  seenId: String,
  targetId: String
)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * TriadicBuild and TriadicFilter are used by Pipelined to perform the same logic as TriadicSelection.
 * 'triadicSelectionId' is used to link corresponding Build and Filter plans.
 */
case class TriadicBuild(
  override val source: LogicalPlan,
  sourceId: String,
  seenId: String,
  triadicSelectionId: Some[Id]
) // wrapped in Some because Id is a value class and doesn't play well with rewriting
(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with PhysicalPlanningPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override def availableSymbols: Set[String] = source.availableSymbols
}

case class TriadicFilter(
  override val source: LogicalPlan,
  positivePredicate: Boolean,
  sourceId: String,
  targetId: String,
  triadicSelectionId: Some[Id]
) // wrapped in Some because Id is a value class and doesn't play well with rewriting
(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with PhysicalPlanningPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  override def availableSymbols: Set[String] = source.availableSymbols
}

/**
 * Scans all relationships and produces two rows for each relationship it finds.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedAllRelationshipsScan(
  idName: String,
  leftNode: String,
  rightNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedAllRelationshipsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * For each relationship element id in 'relIds', fetch the corresponding relationship. For each relationship,
 * produce two rows containing argument and the relationship assigned to 'idName'. In addition, one of these
 * rows has the relationship start node as 'leftNode' and the end node as 'rightNode', while the other produced
 * row has the end node as 'leftNode' = endNode and the start node as 'rightNode'.
 */
case class UndirectedRelationshipByElementIdSeek(
  idName: String,
  relIds: SeekableArgs,
  leftNode: String,
  rightNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = relIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipByElementIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * For each relationship id in 'relIds', fetch the corresponding relationship. For each relationship,
 * produce two rows containing argument and the relationship assigned to 'idName'. In addition, one of these
 * rows has the relationship start node as 'leftNode' and the end node as 'rightNode', while the other produced
 * row has the end node as 'leftNode' = endNode and the start node as 'rightNode'.
 */
case class UndirectedRelationshipByIdSeek(
  idName: String,
  relIds: SeekableArgs,
  leftNode: String,
  rightNode: String,
  argumentIds: Set[String]
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = relIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipByIdSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * This operator does a full scan of an index, producing two rows, on for each direction, for all entries that contain a string value
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipIndexContainsScan(
  idName: String,
  leftNode: String,
  rightNode: String,
  override val typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipIndexContainsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipIndexContainsScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): RelationshipIndexLeafPlan =
    copy(property = f(property))(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * This operator does a full scan of an index, producing two rows, one for each direction, for all entries that end with a string value
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipIndexEndsWithScan(
  idName: String,
  leftNode: String,
  rightNode: String,
  override val typeToken: RelationshipTypeToken,
  property: IndexedProperty,
  valueExpr: Expression,
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override def properties: Seq[IndexedProperty] = Seq(property)

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipIndexEndsWithScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipIndexEndsWithScan =
    copy(property = property.copy(getValueFromIndex = DoNotGetValue))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): UndirectedRelationshipIndexEndsWithScan =
    copy(property = f(property))(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * This operator does a full scan of an index, producing two rows per entry.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipIndexScan(
  idName: String,
  leftNode: String,
  rightNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen)
    extends RelationshipIndexLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipIndexScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipIndexScan =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): UndirectedRelationshipIndexScan =
    copy(properties = properties.map(f))(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * For every relationship with the given type and property values, produces two rows for each relationship.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipIndexSeek(
  idName: String,
  leftNode: String,
  rightNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends RelationshipIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): UndirectedRelationshipIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))

  override def unique: Boolean = false

  override def directed: Boolean = false

  override def withNewLeftAndRightNodes(leftNode: String, rightNode: String): RelationshipIndexSeekLeafPlan =
    copy(leftNode = leftNode, rightNode = rightNode)

}

object UndirectedRelationshipIndexSeek extends IndexSeekNames {
  override val PLAN_DESCRIPTION_INDEX_SCAN_NAME = "UndirectedRelationshipIndexScan"
  override val PLAN_DESCRIPTION_INDEX_SEEK_NAME = "UndirectedRelationshipIndexSeek"
  override val PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME = "UndirectedRelationshipIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME = "UndirectedRelationshipUniqueIndexSeek"
  override val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME = "UndirectedRelationshipUniqueIndexSeekByRange"
  override val PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME = "UndirectedRelationshipUniqueIndexSeek(Locking)"
}

/**
 * Scans the relationship by type and produces two rows for each relationship it finds.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipTypeScan(
  idName: String,
  leftNode: String,
  relType: RelTypeName,
  rightNode: String,
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with RelationshipTypeScan with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipTypeScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def directed: Boolean = false
}

/**
 * Produces two or zero rows containing relationship with the given type and property values.
 *
 * This operator is used on type/property combinations under uniqueness constraint.
 *
 * Given each found relationship, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipUniqueIndexSeek(
  idName: String,
  leftNode: String,
  rightNode: String,
  override val typeToken: RelationshipTypeToken,
  properties: Seq[IndexedProperty],
  valueExpr: QueryExpression[Expression],
  argumentIds: Set[String],
  indexOrder: IndexOrder,
  override val indexType: IndexType
)(implicit idGen: IdGen) extends RelationshipIndexSeekLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipUniqueIndexSeek =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipUniqueIndexSeek =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): UndirectedRelationshipUniqueIndexSeek =
    copy(properties = properties.map(f))(SameId(this.id))

  override def unique: Boolean = true

  override def directed: Boolean = false

  override def withNewLeftAndRightNodes(leftNode: String, rightNode: String): RelationshipIndexSeekLeafPlan =
    copy(leftNode = leftNode, rightNode = rightNode)
}

/**
 * Produce one row for every relationship in the graph that has at least one of the provided types. 
 * This row contains the relationship (assigned to 'idName') and the contents of argument.
 */
case class UndirectedUnionRelationshipTypesScan(
  idName: String,
  startNode: String,
  types: Seq[RelTypeName],
  endNode: String,
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends RelationshipLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedUnionRelationshipTypesScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode

  override def directed: Boolean = false
}

/**
 * Produce first the 'left' rows, and then the 'right' rows. This operator does not guarantee row uniqueness.
 */
case class Union(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
    extends LogicalBinaryPlan(idGen) {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols intersect right.availableSymbols
}

/**
 * Produce one row for every node in the graph that has at least one of the provided labels. This row contains the node (assigned to 'idName')
 * and the contents of argument.
 */
case class UnionNodeByLabelsScan(
  idName: String,
  labels: Seq[LabelName],
  argumentIds: Set[String],
  indexOrder: IndexOrder
)(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) with StableLeafPlan {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UnionNodeByLabelsScan =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}

/**
 * For each source row, evaluate 'expression'. If 'expression' evaluates to a list, produce one row per list
 * element, containing the source row and the element assigned to 'variable'.
 * If 'expression' does evaluate to null, produce nothing.
 * If 'expression' does not evaluate to a list, produce a single row with the value.
 */
case class UnwindCollection(override val source: LogicalPlan, variable: String, expression: Expression)(implicit
idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols + variable
}

/**
 * The definition of a value join is an equality predicate between two expressions that
 * have different, non-empty variable-dependency sets.
 */
case class ValueHashJoin(override val left: LogicalPlan, override val right: LogicalPlan, join: Equals)(implicit
idGen: IdGen) extends LogicalBinaryPlan(idGen) with EagerLogicalPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

/**
 * Marker trait of light-weight simulations of a basic plans that can be used to test or benchmark runtime frameworks
 * in isolation from the database.
 */
sealed trait SimulatedPlan extends TestOnlyPlan

/**
 * Produce the given number of nodes
 */
case class SimulatedNodeScan(idName: String, numberOfRows: Long)(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) with StableLeafPlan with SimulatedPlan {

  override val availableSymbols: Set[String] = Set(idName)

  override def usedVariables: Set[String] = Set.empty

  override def argumentIds: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): SimulatedNodeScan = this
}

/**
 * Expand incoming rows by the given factor
 */
case class SimulatedExpand(
  override val source: LogicalPlan,
  fromNode: String,
  relName: String,
  toNode: String,
  factor: Double
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with SimulatedPlan {
  assert(factor >= 0.0d, "Factor must be greater or equal to 0")

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + relName + toNode
}

/**
 * Filter incoming rows by the given selectivity.
 */
case class SimulatedSelection(override val source: LogicalPlan, selectivity: Double)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with SimulatedPlan {
  assert(selectivity >= 0.0d && selectivity <= 1.0d, "Selectivity must be a fraction between 0 and 1")

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}
