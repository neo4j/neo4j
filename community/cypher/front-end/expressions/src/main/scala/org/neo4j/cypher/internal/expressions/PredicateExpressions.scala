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

import org.neo4j.cypher.internal.expressions.CanonicalStringHelper.nodeRelationCanonicalString
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTString

case class And(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

object Ands {

  def create(exprs: ListSet[Expression]): Expression = {
    val size = exprs.size
    if (size == 0)
      True()(InputPosition.NONE)
    else if (size == 1)
      exprs.head
    else
      Ands(exprs)(exprs.head.position)
  }

  def apply(exprs: IterableOnce[Expression])(position: InputPosition): Ands = {
    Ands(ListSet.from(exprs))(position)
  }
}

/**
 * Conjunction of multiple expressions.
 */
case class Ands(exprs: ListSet[Expression])(val position: InputPosition) extends BooleanExpression
    with MultiOperatorExpression {
  override def canonicalOperatorSymbol = "AND"

  override val signatures = Vector(
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

  override val signatures: Seq[ExpressionTypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

object Ors {

  def create(exprs: ListSet[Expression]): Expression = {
    val size = exprs.size
    if (size == 0)
      False()(InputPosition.NONE)
    else if (size == 1)
      exprs.head
    else
      Ors(exprs)(exprs.head.position)
  }

  def apply(exprs: IterableOnce[Expression])(position: InputPosition): Ors = {
    Ors(ListSet.from(exprs))(position)
  }
}

/**
 * Disjunction of multiple expressions.
 */
case class Ors(exprs: ListSet[Expression])(val position: InputPosition) extends BooleanExpression
    with MultiOperatorExpression {
  override def canonicalOperatorSymbol = "OR"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector.fill(exprs.size)(CTBoolean), outputType = CTBoolean)
  )
}

case class Xor(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Not(rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with LeftUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(Vector(CTBoolean), outputType = CTBoolean)
  )
}

case class Equals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with ChainableBinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "="

  def switchSides: Equals = copy(rhs, lhs)(position)
}

case class NotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with ChainableBinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<>"

  def switchSides: NotEquals = copy(rhs, lhs)(position)
}

case class InvalidNotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with ChainableBinaryOperatorExpression {
  override def canonicalOperatorSymbol = "!="
}

case class RegexMatch(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "=~"
}

case class In(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression

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
}

case class StartsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "STARTS WITH"
}

case class EndsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "ENDS WITH"
}

case class Contains(lhs: Expression, rhs: Expression)(val position: InputPosition) extends BooleanExpression
    with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )
}

case class IsNull(lhs: Expression)(val position: InputPosition) extends BooleanExpression
    with RightUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NULL"
}

case class IsNotNull(lhs: Expression)(val position: InputPosition) extends BooleanExpression
    with RightUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NOT NULL"
}

object InequalityExpression {
  def unapply(arg: InequalityExpression): Option[(Expression, Expression)] = Some((arg.lhs, arg.rhs))
}

sealed trait InequalityExpression extends BooleanExpression with ChainableBinaryOperatorExpression {
  override val signatures = Vector(TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean))

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
 * Predicate used for enforcing relationship uniqueness as done in
 * AddRelationshipPredicates.
 */
sealed trait RelationshipUniquenessPredicate extends BooleanExpression

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
 * Tests whether the relationship is none of the elements from the list.
 *
 * @param relationship the relationship
 * @param listOfRelationships the list of relationships
 */
case class NoneOfRelationships(relationship: Expression, listOfRelationships: Expression)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
  override def isConstantForQuery: Boolean = relationship.isConstantForQuery && listOfRelationships.isConstantForQuery
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
 * Tests whether the relationship is unique across Repeat(Trail) iterations.
 */
case class IsRepeatTrailUnique(variableToCheck: Variable)(val position: InputPosition)
    extends RelationshipUniquenessPredicate {
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
    val literal = SignedDecimalIntegerLiteral(bound.toString)(pos)

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
