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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.CanonicalStringHelper.nodeRelationCanonicalString
import org.neo4j.cypher.internal.expressions.functions.Category
import org.neo4j.cypher.internal.expressions.functions.FunctionWithName
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType

case class And(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

object And {

  def flatten(expression: Expression): ListSet[Expression] =
    expression match {
      case And(lhs, rhs) => flatten(lhs) ++ flatten(rhs)
      case other         => ListSet(other)
    }
}

object Ands {

  def create(exprs: Iterable[Expression]): Expression =
    if (exprs.isEmpty) {
      True()(InputPosition.NONE)
    } else {
      val distinct = ListSet.from(exprs)
      if (distinct.size == 1)
        distinct.head
      else
        Ands(distinct)(distinct.head.position)
    }

  def apply(exprs: IterableOnce[Expression])(position: InputPosition): Ands = {
    Ands(ListSet.from(exprs))(position)
  }

  /**
   * unwrap content of potential `Ands`
   */
  def unwrap(expr: Expression): ListSet[Expression] = expr match {
    case Ands(list) => list
    case singleton  => ListSet(singleton)
  }
}

/**
 * Conjunction of multiple expressions.
 */
case class Ands(exprs: ListSet[Expression])(val position: InputPosition) extends BooleanExpression
    with MultiOperatorExpression {
  override def canonicalOperatorSymbol = "AND"

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector.fill(exprs.size)(CTBoolean), outputType = CTBoolean)
  )
}

/**
 * Only used after planning to mark predicates that can be reordered at runtime.
 */
case class AndsReorderable(exprs: ListSet[Expression])(override val position: InputPosition) extends BooleanExpression {
  override def isConstantForQuery: Boolean = exprs.forall(_.isConstantForQuery)
}

case class Or(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

object Ors {

  def create(exprs: Iterable[Expression]): Expression = {
    val size = exprs.size
    if (size == 0) {
      False()(InputPosition.NONE)
    } else {
      val distinct = ListSet.from(exprs)
      if (distinct.size == 1)
        distinct.head
      else
        Ors(exprs)(exprs.head.position)
    }
  }

  def apply(exprs: IterableOnce[Expression])(position: InputPosition): Ors = {
    Ors(ListSet.from(exprs))(position)
  }

  /**
   * The disjunction of two expressions.
   * It wraps them into a new [[Ors]] if necessary only.
   */
  def of2(left: Expression, right: Expression): Expression =
    (left, right) match {
      case (left, Ors(rightSet)) if rightSet.isEmpty => left
      case (Ors(leftSet), right) if leftSet.isEmpty  => right
      case (Ors(leftSet), Ors(rightSet)) =>
        Ors(ListSet.newBuilder.addAll(leftSet).addAll(rightSet).result())(InputPosition.NONE)
      case (Ors(leftSet), right) =>
        if (leftSet.contains(right))
          left
        else
          Ors(ListSet.newBuilder.addAll(leftSet).addOne(right).result())(InputPosition.NONE)
      case (left, Ors(rightSet)) =>
        if (rightSet.contains(left))
          right
        else
          Ors(ListSet.newBuilder.addOne(left).addAll(rightSet).result())(InputPosition.NONE)
      case (left, right) =>
        Ors(ListSet.newBuilder.addOne(left).addOne(right).result())(InputPosition.NONE)
    }

  /**
   * unwrap content of potential `Ors`
   */
  def unwrap(expr: Expression): Iterable[Expression] = expr match {
    case Ors(list) => list
    case singleton => ListSet(singleton)
  }
}

/**
 * Disjunction of multiple expressions.
 */
case class Ors(exprs: ListSet[Expression])(val position: InputPosition) extends BooleanExpression
    with MultiOperatorExpression {
  override def canonicalOperatorSymbol = "OR"

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector.fill(exprs.size)(CTBoolean), outputType = CTBoolean)
  )
}

case class Xor(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Not(rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with LeftUnaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(Vector(CTBoolean), outputType = CTBoolean)
  )
}

/**
 * predicate that is built up like `<lhs> <OPERATOR> <rhs>` with a part 2 form like `<OPERATOR> <rhs>`
 */
trait BinaryPredicateExpression extends BooleanExpression with BinaryOperatorExpression with Part2OperatorExpression

case class Equals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression
    with ChainableBinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "="

  def switchSides: Equals = copy(rhs, lhs)(position)
}

case class NotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression
    with ChainableBinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<>"

  def switchSides: NotEquals = copy(rhs, lhs)(position)
}

case class InvalidNotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends BinaryPredicateExpression
    with ChainableBinaryOperatorExpression {
  override def canonicalOperatorSymbol = "!="
}

case class RegexMatch(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "=~"
}

case class In(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression
    with BinaryOperatorExpression {

  override def canonicalOperatorSymbol = "IN"
}

// Partial predicates are predicates that are covered by a larger predicate which is going to be solved later during planning
// (and then will replace this predicate).
// (i.e. final query graph matches up with original query)
sealed trait PartialPredicate[+P <: Expression] extends Expression {
  def coveredPredicate: P
  def coveringPredicate: Expression

  override def isConstantForQuery: Boolean = coveredPredicate.isConstantForQuery
}

object PartialPredicate {

  def apply[P <: Expression](coveredPredicate: P, coveringPredicate: Expression): Expression =
    ifNotEqual(coveredPredicate, coveringPredicate).getOrElse(coveringPredicate)

  def ifNotEqual[P <: Expression](coveredPredicate: P, coveringPredicate: Expression): Option[PartialPredicate[P]] =
    if (coveredPredicate == coveringPredicate) None
    else Some(PartialPredicateWrapper(coveredPredicate, coveringPredicate))

  final case class PartialPredicateWrapper[P <: Expression](coveredPredicate: P, coveringPredicate: Expression)
      extends PartialPredicate[P] {
    override def position: InputPosition = coveredPredicate.position
  }

  final case class PartialDistanceSeekWrapper[P <: Expression](predicate: P) extends PartialPredicate[P] {
    override def coveredPredicate: P = predicate
    override def coveringPredicate: Expression = predicate
    override def position: InputPosition = coveredPredicate.position
  }

  def unwrap(expr: Expression): Expression = expr match {
    case pp: PartialPredicate[_] => pp.coveringPredicate
    case e                       => e
  }
}

case class StartsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "STARTS WITH"
}

case class EndsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "ENDS WITH"
}

case class Contains(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BinaryPredicateExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "CONTAINS"
}

case class IsNull(lhs: Expression)(val position: InputPosition) extends BooleanExpression
    with RightUnaryOperatorExpression with Part2OperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NULL"
}

case class IsNotNull(lhs: Expression)(val position: InputPosition) extends BooleanExpression
    with RightUnaryOperatorExpression with Part2OperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NOT NULL"
}

case class PropertyExists(element: Expression, propertyKeyName: PropertyKeyName)(val position: InputPosition)
    extends BooleanExpression with OperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] = Vector(
    TypeSignature(
      argumentTypes = Vector(ClosedDynamicUnionType(Set(CTNode, CTRelationship))(InputPosition.NONE)),
      outputType = CTBoolean
    )
  )

  override def arguments: Seq[Expression] = Seq(element)

  override def isConstantForQuery: Boolean = element.isConstantForQuery

  override def asCanonicalStringVal: String =
    s"property_exists(${element.asCanonicalStringVal}, ${propertyKeyName.asCanonicalStringVal})"
}

object PropertyExistsShowInfo extends FunctionWithName {
  def name: String = "property_exists"

  val functionInfoForShow: Seq[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      function = this,
      outputType = CTBoolean,
      names = Vector("element", "propertyKeyName"),
      description =
        "Returns true if the given node or relationship has a property with the given key, otherwise false. Returns null if the first argument is null.",
      category = Category.PREDICATE,
      argumentTypes = Vector(
        ClosedDynamicUnionType(Set(CTNode, CTRelationship))(InputPosition.NONE),
        CTString
      ),
      argumentDescriptions = Map(
        "element" -> "A node or relationship to check for the property.",
        "propertyKeyName" -> "The name of the property to check for."
      ),
      scopes = Set(CypherVersion.Cypher25)
    )
  )
}

object InequalityExpression {
  def unapply(arg: InequalityExpression): Option[(Expression, Expression)] = Some((arg.lhs, arg.rhs))
}

sealed trait InequalityExpression extends BinaryPredicateExpression with ChainableBinaryOperatorExpression {

  override val signatures: Vector[ExpressionTypeSignature] =
    Vector(TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean))

  def includeEquality: Boolean

  def negated: InequalityExpression
  def swapped: InequalityExpression

  def lhs: Expression
  def rhs: Expression
}

final case class LessThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends InequalityExpression {
  override val canonicalOperatorSymbol = "<"

  override val includeEquality = false

  override def negated: InequalityExpression = GreaterThanOrEqual(lhs, rhs)(position)
  override def swapped: InequalityExpression = GreaterThan(rhs, lhs)(position)
}

final case class LessThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends InequalityExpression {
  override val canonicalOperatorSymbol = "<="

  override val includeEquality = true

  override def negated: InequalityExpression = GreaterThan(lhs, rhs)(position)
  override def swapped: InequalityExpression = GreaterThanOrEqual(rhs, lhs)(position)
}

final case class GreaterThan(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends InequalityExpression {
  override val canonicalOperatorSymbol = ">"

  override val includeEquality = false

  override def negated: InequalityExpression = LessThanOrEqual(lhs, rhs)(position)
  override def swapped: InequalityExpression = LessThan(rhs, lhs)(position)
}

final case class GreaterThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends InequalityExpression {
  override val canonicalOperatorSymbol = ">="

  override val includeEquality = true

  override def negated: InequalityExpression = LessThan(lhs, rhs)(position)
  override def swapped: InequalityExpression = LessThanOrEqual(rhs, lhs)(position)
}

case class HasDegreeLessThan(
  node: Expression,
  relType: Option[RelTypeName],
  dir: SemanticDirection,
  degree: Expression
)(val position: InputPosition) extends BooleanExpression {

  override def asCanonicalStringVal: String =
    s"getDegree(${nodeRelationCanonicalString(node, relType, dir)}) < ${degree.asCanonicalStringVal}"

  override def isConstantForQuery: Boolean = node.isConstantForQuery
}

case class HasDegreeLessThanOrEqual(
  node: Expression,
  relType: Option[RelTypeName],
  dir: SemanticDirection,
  degree: Expression
)(val position: InputPosition) extends BooleanExpression {

  override def asCanonicalStringVal: String =
    s"getDegree(${nodeRelationCanonicalString(node, relType, dir)}) <= ${degree.asCanonicalStringVal}"

  override def isConstantForQuery: Boolean = node.isConstantForQuery
}

case class HasDegreeGreaterThan(
  node: Expression,
  relType: Option[RelTypeName],
  dir: SemanticDirection,
  degree: Expression
)(val position: InputPosition) extends BooleanExpression {

  override def asCanonicalStringVal: String =
    s"getDegree(${nodeRelationCanonicalString(node, relType, dir)}) > ${degree.asCanonicalStringVal}"

  override def isConstantForQuery: Boolean = node.isConstantForQuery
}

case class HasDegreeGreaterThanOrEqual(
  node: Expression,
  relType: Option[RelTypeName],
  dir: SemanticDirection,
  degree: Expression
)(val position: InputPosition) extends BooleanExpression {

  override def asCanonicalStringVal: String =
    s"getDegree(${nodeRelationCanonicalString(node, relType, dir)}) >= ${degree.asCanonicalStringVal}"

  override def isConstantForQuery: Boolean = node.isConstantForQuery
}

case class HasDegree(node: Expression, relType: Option[RelTypeName], dir: SemanticDirection, degree: Expression)(
  val position: InputPosition
) extends BooleanExpression {

  override def asCanonicalStringVal: String =
    s"getDegree(${nodeRelationCanonicalString(node, relType, dir)}) = ${degree.asCanonicalStringVal}"

  override def isConstantForQuery: Boolean = node.isConstantForQuery
}

case class AssertIsNode(lhs: Expression)(val position: InputPosition) extends BooleanExpression {
  override def isConstantForQuery: Boolean = lhs.isConstantForQuery
}

/**
 * Predicate used for enforcing relationship uniqueness as done in AddElementUniquenessPredicates.
 */
sealed trait RelationshipUniquenessPredicate extends BooleanExpression

/**
 * Predicate used for enforcing node uniqueness as done in AddElementUniquenessPredicates.
 */
sealed trait NodeUniquenessPredicate extends BooleanExpression

/**
 * Tests whether the two relationships given are different.
 *
 * @param rel1 first relationship
 * @param rel2 second relationship
 */
case class DifferentRelationships(rel1: Expression, rel2: Expression)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = false
}

/**
 * Tests whether the two nodes given are different.
 *
 * @param node1 first node
 * @param node2 second node
 */
case class DifferentNodes(node1: Expression, node2: Expression)(val position: InputPosition)
    extends NodeUniquenessPredicate {
  override def isConstantForQuery: Boolean = false
}

/**
 * Tests whether the relationship is none of the elements from the list.
 *
 * @param relationship the relationship
 * @param listOfRelationships the list of relationships
 */
case class NoneOfRelationships(relationship: Expression, listOfRelationships: Expression)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = relationship.isConstantForQuery && listOfRelationships.isConstantForQuery
}

case class NoneOfNodes(node: Expression, listOfNodes: Expression)(val position: InputPosition)
    extends NodeUniquenessPredicate {
  override def isConstantForQuery: Boolean = node.isConstantForQuery && listOfNodes.isConstantForQuery
}

/**
 * Predicate expression that represents that a node must be different from all the nodes in a VarExpand pattern.
 * This should only be generated when the 'equivalence class' of the node and the VarExpand are different, i.e. there
 * should be a fixed-length relationship between the node and the VarExpand pattern in the path pattern.
 *
 * @param nodeVariable The node variable that needs to be different from all nodes of the VarExpand pattern
 * @param varExpandRelationshipVariable The relationship variable of the VarExpand pattern
 * @param varLengthRelDirection The direction of the VarExpand pattern
 * @param mustBeInlined Whether this predicate must be inlined in the VarExpand relationshipPredicates
 */
case class NoneOfNodesInVarLengthRelationship(
  nodeVariable: Expression,
  varExpandRelationshipVariable: Expression,
  varLengthRelDirection: SemanticDirection,
  mustBeInlined: Boolean
)(val position: InputPosition)
    extends NodeUniquenessPredicate {

  override def isConstantForQuery: Boolean =
    nodeVariable.isConstantForQuery && varExpandRelationshipVariable.isConstantForQuery
}

/**
 * Tests whether the elements in the two lists given are disjoint, that is, none of the elements from one list
 * also exist in the other list.
 *
 * @param lhs first list
 * @param rhs second list
 */
case class Disjoint(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = lhs.isConstantForQuery && rhs.isConstantForQuery
}

// The repetition bounds are only given to assist during cardinality estimation
case class DisjointNodes(
  lhs: Expression,
  rhs: Expression,
  lhsRepetitionLowerBound: Long,
  lhsRepetitionUpperBound: Option[Long]
)(val position: InputPosition)
    extends NodeUniquenessPredicate {
  override def isConstantForQuery: Boolean = lhs.isConstantForQuery && rhs.isConstantForQuery
}

/**
 * Tests whether the elements in the list given are all unique.
 * Cannot be used in Cypher directly but is generated by AddUniquenessPredicates.
 *
 * @param rhs the list to test
 */
case class Unique(rhs: Expression)(val position: InputPosition) extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = rhs.isConstantForQuery
}

/**
 *
 * @param nodeList A list of group nodes which should be matched to all unique nodes.
 * @param maybeRelationshipList A list of group relationships which should be matched to all unique relationships.
 *                              This is implied by the uniqueness requirements over the node groups,
 *                              but it is used to generate IsRepeatTrailUnique-predicates that verify, during
 *                              QPP-evaluation, relationship uniqueness with previously bound relationships
 *                              from other path patterns.
 *                              It will only be set when
 *                              - the match modes is DIFFERENT RELATIONSHIPS and
 *                              - the match consists of at least two (ACYCLIC) path patterns.
 *                              Otherwise, maybeRelationshipList=None.
 */
case class UniqueNodes(nodeList: Expression, maybeRelationshipList: Option[Expression])(val position: InputPosition)
    extends NodeUniquenessPredicate {

  override def isConstantForQuery: Boolean =
    nodeList.isConstantForQuery && maybeRelationshipList.forall(_.isConstantForQuery)
}

/**
 * Tests whether the relationship is unique across Repeat(Trail) iterations.
 */
case class IsRepeatTrailUnique(variableToCheck: Variable)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = false
}

/**
 * Tests whether the node is unique across Repeat(Acyclic) iterations.
 */
case class IsRepeatAcyclic(variableToCheck: Variable)(val position: InputPosition)
    extends NodeUniquenessPredicate {
  override def isConstantForQuery: Boolean = false
}

/**
 * Implicit predicate generated for size constraints on the list of relationships matched by variable-length relationships.
 */
abstract class VarLengthBound(val relName: Variable, val bound: Long) extends BooleanExpression {
  // We always depend on the relationship referenced
  override def isConstantForQuery: Boolean = false

  def getRewrittenPredicate: InequalityExpression = {
    val pos = position
    val size: Expression => FunctionInvocation = FunctionInvocation(FunctionName("size")(pos), _)(pos)
    val literal = SignedDecimalIntegerLiteral(bound.toString)(pos.zeroLength)

    getInequalityExpression(size(relName), literal, pos)
  }

  def getInequalityExpression(
    relationshipExpression: Expression,
    boundExpression: Expression,
    position: InputPosition
  ): InequalityExpression
}

/**
 * The implicit predicate that the list of relationships matched by a variable-length relationship is at least as long as the lower bound of the var-length relationship. 
 */
case class VarLengthLowerBound(override val relName: Variable, override val bound: Long)(val position: InputPosition)
    extends VarLengthBound(relName, bound) {

  override def getInequalityExpression(
    relationshipExpression: Expression,
    boundExpression: Expression,
    position: InputPosition
  ): InequalityExpression = GreaterThanOrEqual(relationshipExpression, boundExpression)(position)
}

/**
 * The implicit predicate that the list of relationships matched by a variable-length relationship is at most as long as the upper bound of the var-length relationship. 
 */
case class VarLengthUpperBound(override val relName: Variable, override val bound: Long)(val position: InputPosition)
    extends VarLengthBound(relName, bound) {

  override def getInequalityExpression(
    relationshipExpression: Expression,
    boundExpression: Expression,
    position: InputPosition
  ): InequalityExpression = LessThanOrEqual(relationshipExpression, boundExpression)(position)
}
