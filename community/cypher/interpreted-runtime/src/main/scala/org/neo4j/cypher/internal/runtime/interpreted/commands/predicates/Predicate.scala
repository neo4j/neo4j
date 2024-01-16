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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.expressions.NormalForm
import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.IsList
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedNodeHasProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedNodeProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedRelationshipHasProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedRelationshipProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueBooleanLogic
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.util.regex.Pattern

import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait IsMatchResult {
  def negate: IsMatchResult
  def ^(other: IsMatchResult): IsMatchResult
  def isKnown: Boolean
  def asBoolean: Boolean
}

object IsMatchResult {
  def apply(boolean: Boolean): IsMatchResult = if (boolean) IsTrue else IsFalse

  def apply(boolean: Value): IsMatchResult = boolean match {
    case IsNoValue()  => IsUnknown
    case Values.TRUE  => IsTrue
    case Values.FALSE => IsFalse
    case _            => throw new CypherTypeException(s"$boolean is not a boolean value")
  }
}

case object IsTrue extends IsMatchResult {
  override def negate: IsMatchResult = IsFalse

  override def ^(other: IsMatchResult): IsMatchResult = other.negate

  override def isKnown: Boolean = true

  override def asBoolean: Boolean = true
}

case object IsFalse extends IsMatchResult {
  override def negate: IsMatchResult = IsTrue

  override def ^(other: IsMatchResult): IsMatchResult = other

  override def isKnown: Boolean = true

  override def asBoolean: Boolean = false
}

case object IsUnknown extends IsMatchResult {
  override def negate: IsMatchResult = IsUnknown

  override def ^(other: IsMatchResult): IsMatchResult = IsUnknown

  override def isKnown: Boolean = false

  override def asBoolean: Boolean = throw new IllegalStateException("Unknown value cannot be turned into a boolean")
}

abstract class Predicate extends Expression {

  override def apply(row: ReadableRow, state: QueryState): Value =
    isMatch(row, state) match {
      case IsTrue    => Values.TRUE
      case IsFalse   => Values.FALSE
      case IsUnknown => Values.NO_VALUE
    }

  def isTrue(ctx: ReadableRow, state: QueryState): Boolean = isMatch(ctx, state) eq IsTrue
  def andWith(other: Predicate): Predicate = Ands(this, other)
  def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult

  def andWith(preds: Predicate*): Predicate =
    if (preds.isEmpty) this else preds.fold(this)(_ andWith _)
}

object Predicate {
  def fromSeq(in: Seq[Predicate]): Predicate = in.reduceOption(_ andWith _).getOrElse(True())
}

abstract class CompositeBooleanPredicate extends Predicate {

  def predicates: NonEmptyList[Predicate]

  def shouldExitWhen: IsMatchResult

  /**
   * This algorithm handles the case where we combine multiple AND or multiple OR groups (CNF or DNF).
   * As well as performing shortcut evaluation so that a false (for AND) or true (for OR) will exit the
   * evaluation without performing any further predicate evaluations (including those that could throw
   * exceptions). Any exception thrown is held until the end (or until the exit state) so that it is
   * superceded by exit predicates (false for AND and true for OR).
   */
  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    predicates.foldLeft[Try[IsMatchResult]](Success(shouldExitWhen.negate)) { (previousValue, predicate) =>
      previousValue match {
        // if a previous evaluation was true (false) the OR (AND) result is determined
        case Success(result) if result == shouldExitWhen => previousValue
        case _ =>
          Try(predicate.isMatch(ctx, state)) match {
            // Handle null only for non error cases
            case Success(IsUnknown) if previousValue.isSuccess => Success(IsUnknown)
            // If we get the exit case (false for AND and true for OR) ignore any error cases
            case Success(result) if result == shouldExitWhen => Success(shouldExitWhen)
            // errors or non-exit cases propagate as normal
            case Failure(e) if previousValue.isSuccess => Failure(e)
            case _                                     => previousValue
          }
      }
    } match {
      case Failure(e)      => throw e
      case Success(option) => option
    }
  }

  override def arguments: Seq[Expression] = predicates.toIndexedSeq
}

case class Not(a: Predicate) extends Predicate {

  def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = a.isMatch(ctx, state).negate
  override def toString: String = "NOT(" + a + ")"
  override def rewrite(f: Expression => Expression): Expression = f(Not(a.rewriteAsPredicate(f)))
  override def arguments: Seq[Expression] = Seq(a)
  override def children: Seq[AstNode[_]] = Seq(a)
}

case class Xor(a: Predicate, b: Predicate) extends Predicate {

  def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = a.isMatch(ctx, state) ^ b.isMatch(ctx, state)

  override def toString: String = "(" + a + " XOR " + b + ")"

  override def rewrite(f: Expression => Expression): Expression =
    f(Xor(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  override def arguments: Seq[Expression] = Seq(a, b)

  override def children: Seq[AstNode[_]] = Seq(a, b)
}

case class IsNull(expression: Expression) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = expression(ctx, state) match {
    case IsNoValue() => IsTrue
    case _           => IsFalse
  }

  override def toString: String = expression + " IS NULL"
  override def rewrite(f: Expression => Expression): Expression = f(IsNull(expression.rewrite(f)))
  override def arguments: Seq[Expression] = Seq(expression)
  override def children: Seq[AstNode[_]] = Seq(expression)
}

case class IsTyped(expression: Expression, typeName: CypherType) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    IsMatchResult(CypherFunctions.isTyped(expression(ctx, state), typeName))
  }

  override def toString: String = expression + " IS :: " + typeName
  override def rewrite(f: Expression => Expression): Expression = f(IsTyped(expression.rewrite(f), typeName))
  override def arguments: Seq[Expression] = Seq(expression)
  override def children: Seq[AstNode[_]] = Seq(expression)
}

case class IsNormalized(expression: Expression, normalForm: NormalForm) extends Expression {

  override def toString: String = expression + " IS NORMALIZED"

  override def rewrite(f: Expression => Expression): Expression = f(IsNormalized(expression.rewrite(f), normalForm))

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.isNormalized(expression(ctx, state), normalForm)

  override def arguments: Seq[Expression] = Seq(expression)

  override def children: Seq[AstNode[_]] = Seq(expression)
}

case class True() extends Predicate {
  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = IsTrue
  override def toString: String = "true"
  override def rewrite(f: Expression => Expression): Expression = f(this)
  override def arguments: Seq[Expression] = Seq.empty
  override def children: Seq[AstNode[_]] = Seq.empty
}

abstract class CachedNodePropertyExists(cp: AbstractCachedProperty) extends Predicate {

  protected def readFromStoreAndCache(
    nodeId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    nodeCursor: NodeCursor,
    propertyCursor: PropertyCursor
  ): Boolean

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val nodeId = cp.getId(ctx)
    if (nodeId == StatementConstants.NO_SUCH_NODE) {
      IsUnknown
    } else {
      val query = state.query
      cp.getPropertyKey(query) match {
        case StatementConstants.NO_SUCH_PROPERTY_KEY => IsFalse
        case propId =>
          query.nodeReadOps.hasTxStatePropertyForCachedProperty(nodeId, propId) match {
            case None => // no change in TX state
              cp.getCachedProperty(ctx) match {
                case null =>
                  // the cached node property has been invalidated
                  val cursors = state.cursors
                  IsMatchResult(readFromStoreAndCache(
                    nodeId,
                    propId,
                    ctx,
                    query,
                    cursors.nodeCursor,
                    cursors.propertyCursor
                  ))
                case IsNoValue() =>
                  IsFalse
                case _ =>
                  IsTrue
              }
            case Some(true)  => IsTrue
            case Some(false) => IsFalse
          }
      }
    }
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(CachedNodePropertyExists(cp.rewrite(f)))

  override def toString: String = s"hasCachedNodeProp($cp)"

  override def arguments: Seq[Expression] = Seq(cp)

  override def children: Seq[AstNode[_]] = Seq(cp)
}

object CachedNodePropertyExists {

  def apply(expression: Expression): CachedNodePropertyExists = expression match {
    case cp: AbstractCachedNodeProperty    => CachedNodePropertyExistsWithValue(cp)
    case cp: AbstractCachedNodeHasProperty => CachedNodePropertyExistsWithoutValue(cp)
    case _ => throw new CypherTypeException("Expected " + expression + " to be a cached node property.")
  }
}

case class CachedNodePropertyExistsWithValue(cachedNodeProperty: AbstractCachedNodeProperty)
    extends CachedNodePropertyExists(cachedNodeProperty) {

  override protected def readFromStoreAndCache(
    nodeId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    nodeCursor: NodeCursor,
    propertyCursor: PropertyCursor
  ): Boolean = {
    val property = query.nodeReadOps.getProperty(nodeId, propId, nodeCursor, propertyCursor, throwOnDeleted = false)
    // Re-cache the value
    cachedNodeProperty.setCachedProperty(ctx, property)
    !(property eq Values.NO_VALUE)
  }
}

case class CachedNodePropertyExistsWithoutValue(cachedNodeProperty: AbstractCachedNodeHasProperty)
    extends CachedNodePropertyExists(cachedNodeProperty) {

  override protected def readFromStoreAndCache(
    nodeId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    nodeCursor: NodeCursor,
    propertyCursor: PropertyCursor
  ): Boolean = {
    // NOTE: we don't need the actual value
    val hasProp = query.nodeReadOps.hasProperty(nodeId, propId, nodeCursor, propertyCursor)
    // Re-cache if the value was there or not
    cachedNodeProperty.setCachedProperty(ctx, if (hasProp) Values.TRUE else Values.NO_VALUE)
    hasProp
  }
}

abstract class CachedRelationshipPropertyExists(cp: AbstractCachedProperty) extends Predicate {

  protected def readFromStoreAndCache(
    relId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    relCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor
  ): Boolean

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val relId = cp.getId(ctx)
    if (relId == StatementConstants.NO_SUCH_RELATIONSHIP) {
      IsUnknown
    } else {
      val query = state.query
      cp.getPropertyKey(query) match {
        case StatementConstants.NO_SUCH_PROPERTY_KEY =>
          IsFalse
        case propId =>
          query.relationshipReadOps.hasTxStatePropertyForCachedProperty(relId, propId) match {
            case None => // no change in TX state
              cp.getCachedProperty(ctx) match {
                case null =>
                  // the cached rel property has been invalidated
                  val cursors = state.cursors
                  IsMatchResult(readFromStoreAndCache(
                    relId,
                    propId,
                    ctx,
                    query,
                    cursors.relationshipScanCursor,
                    cursors.propertyCursor
                  ))
                case IsNoValue() =>
                  IsFalse
                case _ =>
                  IsTrue
              }
            case Some(true)  => IsTrue
            case Some(false) => IsFalse
          }
      }
    }
  }

  override def rewrite(f: Expression => Expression): Expression = f(CachedRelationshipPropertyExists(cp.rewrite(f)))

  override def toString: String = s"hasCachedRelationshipProp($cp)"

  override def arguments: Seq[Expression] = Seq(cp)

  override def children: Seq[AstNode[_]] = Seq(cp)
}

object CachedRelationshipPropertyExists {

  def apply(expression: Expression): CachedRelationshipPropertyExists = expression match {
    case cp: AbstractCachedRelationshipProperty    => CachedRelationshipPropertyExistsWithValue(cp)
    case cp: AbstractCachedRelationshipHasProperty => CachedRelationshipPropertyExistsWithoutValue(cp)
    case _ => throw new CypherTypeException("Expected " + expression + " to be a cached relationship property.")
  }
}

case class CachedRelationshipPropertyExistsWithValue(cachedRelProperty: AbstractCachedRelationshipProperty)
    extends CachedRelationshipPropertyExists(cachedRelProperty) {

  override protected def readFromStoreAndCache(
    relId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    relCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor
  ): Boolean = {
    val property =
      query.relationshipReadOps.getProperty(relId, propId, relCursor, propertyCursor, throwOnDeleted = false)
    // Re-cache the value
    cachedRelProperty.setCachedProperty(ctx, property)
    !(property eq Values.NO_VALUE)
  }
}

case class CachedRelationshipPropertyExistsWithoutValue(cachedRelProperty: AbstractCachedRelationshipHasProperty)
    extends CachedRelationshipPropertyExists(cachedRelProperty) {

  override protected def readFromStoreAndCache(
    relId: Long,
    propId: Int,
    ctx: ReadableRow,
    query: QueryContext,
    relCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor
  ): Boolean = {
    // NOTE: we don't need the actual value
    val hasProp = query.relationshipReadOps.hasProperty(relId, propId, relCursor, propertyCursor)
    // Re-cache if the value was there or not
    cachedRelProperty.setCachedProperty(ctx, if (hasProp) Values.TRUE else Values.NO_VALUE)
    hasProp
  }
}

trait StringOperator {
  self: Predicate =>

  override def isMatch(m: ReadableRow, state: QueryState): IsMatchResult = (lhs(m, state), rhs(m, state)) match {
    case (l: TextValue, r: TextValue) => IsMatchResult(compare(l, r))
    case (_, _)                       => IsUnknown
  }

  def lhs: Expression
  def rhs: Expression
  def compare(a: TextValue, b: TextValue): Boolean
  override def arguments: Seq[Expression] = Seq(lhs, rhs)
}

case class StartsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue): Boolean = a.startsWith(b)

  override def rewrite(f: Expression => Expression): Expression = f(copy(lhs.rewrite(f), rhs.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(lhs, rhs)
}

case class EndsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue): Boolean = a.endsWith(b)

  override def rewrite(f: Expression => Expression): Expression = f(copy(lhs.rewrite(f), rhs.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(lhs, rhs)
}

case class Contains(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: TextValue, b: TextValue): Boolean = a.contains(b)

  override def rewrite(f: Expression => Expression): Expression = f(copy(lhs.rewrite(f), rhs.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(lhs, rhs)
}

case class LiteralRegularExpression(lhsExpr: Expression, regexExpr: Literal)(implicit
converter: TextValue => TextValue = identity) extends Predicate {
  lazy val pattern: Pattern = converter(regexExpr.value.asInstanceOf[TextValue]).stringValue().r.pattern

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult =
    lhsExpr(ctx, state) match {
      case s: TextValue => IsMatchResult(pattern.matcher(s.stringValue()).matches())
      case _            => IsUnknown
    }

  override def rewrite(f: Expression => Expression): Expression = f(regexExpr.rewrite(f) match {
    case lit: Literal => LiteralRegularExpression(lhsExpr.rewrite(f), lit)(converter)
    case other        => RegularExpression(lhsExpr.rewrite(f), other)(converter)
  })

  override def arguments: Seq[Expression] = Seq(lhsExpr, regexExpr)

  override def children: Seq[AstNode[_]] = Seq(lhsExpr, regexExpr)
  override def toString = s"$lhsExpr =~ $regexExpr"
}

case class RegularExpression(lhsExpr: Expression, regexExpr: Expression)(implicit converter: TextValue => TextValue =
  identity) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val lValue = lhsExpr(ctx, state)
    val rValue = regexExpr(ctx, state)
    (lValue, rValue) match {
      case (lhs: TextValue, rhs) if !(rhs eq Values.NO_VALUE) =>
        val rhsAsRegexString = converter(CastSupport.castOrFail[TextValue](rhs))
        IsMatchResult(ValueBooleanLogic.regex(lhs, rhsAsRegexString).booleanValue())
      case _ => IsUnknown
    }
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + regexExpr.toString() + "/"

  override def rewrite(f: Expression => Expression): Expression = f(regexExpr.rewrite(f) match {
    case lit: Literal => LiteralRegularExpression(lhsExpr.rewrite(f), lit)(converter)
    case other        => RegularExpression(lhsExpr.rewrite(f), other)(converter)
  })

  override def arguments: Seq[Expression] = Seq(lhsExpr, regexExpr)

  override def children: Seq[AstNode[_]] = Seq(lhsExpr, regexExpr)
}

case class NonEmpty(collection: Expression) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    collection(ctx, state) match {
      case IsList(x)   => IsMatchResult(x.nonEmpty)
      case IsNoValue() => IsUnknown
      case x           => throw new CypherTypeException(s"Expected a collection, got `$x`")
    }
  }

  override def toString: String = "nonEmpty(" + collection.toString() + ")"

  override def rewrite(f: Expression => Expression): Expression = f(NonEmpty(collection.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(collection)

  override def children: Seq[AstNode[_]] = Seq(collection)
}

case class HasALabel(entity: Expression) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case value =>
      val node = CastSupport.castOrFail[VirtualNodeValue](value)
      val nodeId = node.id
      val queryCtx = state.query
      IsMatchResult(queryCtx.isALabelSetOnNode(nodeId, state.cursors.nodeCursor))
  }

  override def toString = s"$entity:%"

  override def rewrite(f: Expression => Expression): Expression = f(HasALabel(entity.rewrite(f)))

  override def children: Seq[Expression] = Seq(entity)

  override def arguments: Seq[Expression] = Seq(entity)

}

case class HasALabelOrType(entity: Expression) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case node: VirtualNodeValue =>
      val nodeId = node.id
      val queryCtx = state.query
      IsMatchResult(queryCtx.isALabelSetOnNode(nodeId, state.cursors.nodeCursor))

    case _: VirtualRelationshipValue =>
      IsTrue

    case value =>
      throw new CypherTypeException(
        s"Expected $value to be a Node or Relationship, but it was a ${value.getClass.getName}"
      )
  }

  override def toString = s"$entity:%"

  override def rewrite(f: Expression => Expression): Expression = f(HasALabelOrType(entity.rewrite(f)))

  override def children: Seq[Expression] = Seq(entity)

  override def arguments: Seq[Expression] = Seq(entity)
}

case class HasLabelOrType(entity: Expression, labelOrType: String) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case node: VirtualNodeValue =>
      val nodeId = node.id
      val queryCtx = state.query
      val token = state.query.nodeLabel(labelOrType)
      if (token == StatementConstants.NO_SUCH_LABEL) IsFalse
      else IsMatchResult(queryCtx.isLabelSetOnNode(token, nodeId, state.cursors.nodeCursor))

    case relationship: VirtualRelationshipValue =>
      val relId = relationship.id
      val queryCtx = state.query
      val token = state.query.relationshipType(labelOrType)
      if (token == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) IsFalse
      else IsMatchResult(queryCtx.isTypeSetOnRelationship(token, relId, state.cursors.relationshipScanCursor))

    case value =>
      throw new CypherTypeException(
        s"Expected $value to be a Node or Relationship, but it was a ${value.getClass.getName}"
      )
  }

  override def toString = s"$entity:$labelOrType"

  override def rewrite(f: Expression => Expression): Expression = f(HasLabelOrType(entity.rewrite(f), labelOrType))

  override def children: Seq[Expression] = Seq(entity)

  override def arguments: Seq[Expression] = Seq(entity)
}

case class HasLabel(entity: Expression, label: KeyToken) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case value =>
      val node = CastSupport.castOrFail[VirtualNodeValue](value)
      val nodeId = node.id
      val queryCtx = state.query

      label.getOptId(state.query) match {
        case None =>
          IsFalse
        case Some(labelId) =>
          IsMatchResult(queryCtx.isLabelSetOnNode(labelId, nodeId, state.cursors.nodeCursor))
      }
  }

  override def toString = s"$entity:${label.name}"

  override def rewrite(f: Expression => Expression): Expression =
    f(HasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f)))

  override def children: Seq[Expression] = Seq(label, entity)

  override def arguments: Seq[Expression] = Seq(entity)
}

case class HasAnyLabel(entity: Expression, labels: Seq[KeyToken]) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {
    case IsNoValue() =>
      IsUnknown

    case value =>
      val nodeId = CastSupport.castOrFail[VirtualNodeValue](value).id()
      val tokens = labels.flatMap(_.getOptId(state.query)).toArray
      IsMatchResult(state.query.isAnyLabelSetOnNode(tokens, nodeId, state.cursors.nodeCursor))
  }

  override def toString = s"$entity:${labels.mkString("|")}"

  override def rewrite(f: Expression => Expression): Expression =
    f(HasAnyLabel(entity.rewrite(f), labels.map(_.typedRewrite[KeyToken](f))))

  override def children: Seq[Expression] = labels :+ entity

  override def arguments: Seq[Expression] = Seq(entity)
}

case class HasType(entity: Expression, typ: KeyToken) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case value =>
      val relationship = CastSupport.castOrFail[VirtualRelationshipValue](value)
      val relationshipId = relationship.id
      val queryCtx = state.query

      typ.getOptId(state.query) match {
        case None =>
          IsFalse
        case Some(relTypeId) =>
          IsMatchResult(queryCtx.isTypeSetOnRelationship(
            relTypeId,
            relationshipId,
            state.cursors.relationshipScanCursor
          ))
      }
  }

  override def toString = s"$entity:${typ.name}"

  override def rewrite(f: Expression => Expression): Expression =
    f(HasType(entity.rewrite(f), typ.typedRewrite[KeyToken](f)))

  override def children: Seq[Expression] = Seq(typ, entity)

  override def arguments: Seq[Expression] = Seq(entity)
}

case class CoercedPredicate(inner: Expression) extends Predicate {
  override def arguments: Seq[Expression] = Seq(inner)

  override def children: Seq[AstNode[_]] = Seq(inner)

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = inner(ctx, state) match {
    case x: BooleanValue => IsMatchResult(x.booleanValue())
    case IsNoValue()     => IsUnknown
    case IsList(coll)    => IsMatchResult(coll.nonEmpty)
    case x               => throw new CypherTypeException(s"Don't know how to treat that as a predicate: $x")
  }

  override def rewrite(f: Expression => Expression): Expression = f(CoercedPredicate(inner.rewrite(f)))

  override def toString: String = inner.toString
}
