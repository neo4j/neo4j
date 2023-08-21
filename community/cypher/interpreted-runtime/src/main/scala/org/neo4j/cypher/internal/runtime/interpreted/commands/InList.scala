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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsFalse
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsMatchResult
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsTrue
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsUnknown
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue

/**
 * These classes solve List Predicates.
 */
abstract class InList(collection: Expression, innerVariableName: String, innerVariableOffset: Int, predicate: Predicate)
    extends Predicate
    with ListSupport {

  type CollectionPredicate = (AnyValue => IsMatchResult) => IsMatchResult

  def seqMethod(f: ListValue): CollectionPredicate

  def isMatch(row: ReadableRow, state: QueryState): IsMatchResult = {
    val list = collection(row, state)

    if (list eq Values.NO_VALUE) IsUnknown
    else {
      val seq = makeTraversable(list)
      seqMethod(seq) { item =>
        state.expressionVariables(innerVariableOffset) = item
        predicate.isMatch(row, state)
      }
    }
  }

  def name: String

  override def toString: String = s"$name($innerVariableName IN $collection WHERE $predicate)"

  override def children: Seq[Expression] = Seq(collection, predicate)

  def arguments: Seq[Expression] = Seq(collection)

}

case class AllInList(collection: Expression, innerVariableName: String, innerVariableOffset: Int, inner: Predicate)
    extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def forAll(collectionValue: ListValue)(predicate: AnyValue => IsMatchResult): IsMatchResult = {
    var result: IsMatchResult = IsTrue

    val iterator = collectionValue.iterator()
    while (iterator.hasNext) {
      predicate(iterator.next()) match {
        case IsFalse   => return IsFalse
        case IsUnknown => result = IsUnknown
        case _         =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = forAll(value)
  def name = "all"

  def rewrite(f: Expression => Expression): Expression =
    f(AllInList(collection.rewrite(f), innerVariableName, innerVariableOffset, inner.rewriteAsPredicate(f)))
}

case class AnyInList(collection: Expression, innerVariableName: String, innerVariableOffset: Int, inner: Predicate)
    extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def exists(collectionValue: ListValue)(predicate: AnyValue => IsMatchResult): IsMatchResult = {
    var result: IsMatchResult = IsFalse
    val iterator = collectionValue.iterator()
    while (iterator.hasNext) {
      predicate(iterator.next()) match {
        case IsTrue    => return IsTrue
        case IsUnknown => result = IsUnknown
        case _         =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = exists(value)

  def name = "any"

  def rewrite(f: Expression => Expression): Expression =
    f(AnyInList(collection.rewrite(f), innerVariableName, innerVariableOffset, inner.rewriteAsPredicate(f)))
}

case class NoneInList(collection: Expression, innerVariableName: String, innerVariableOffset: Int, inner: Predicate)
    extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def none(collectionValue: ListValue)(predicate: AnyValue => IsMatchResult): IsMatchResult = {
    var result: IsMatchResult = IsTrue

    val iterator = collectionValue.iterator()
    while (iterator.hasNext) {
      predicate(iterator.next()) match {
        case IsTrue    => return IsFalse
        case IsUnknown => result = IsUnknown
        case _         =>
      }
    }

    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = none(value)

  def name = "none"

  def rewrite(f: Expression => Expression): Expression =
    f(NoneInList(collection.rewrite(f), innerVariableName, innerVariableOffset, inner.rewriteAsPredicate(f)))
}

case class SingleInList(collection: Expression, innerVariableName: String, innerVariableOffset: Int, inner: Predicate)
    extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def single(collectionValue: ListValue)(predicate: AnyValue => IsMatchResult): IsMatchResult = {
    var matched = false
    var atLeastOneNull = false
    val iterator = collectionValue.iterator()
    while (iterator.hasNext) {
      predicate(iterator.next()) match {
        case IsTrue if matched => return IsFalse
        case IsTrue            => matched = true
        case IsUnknown         => atLeastOneNull = true
        case _                 =>
      }
    }

    if (atLeastOneNull)
      IsUnknown
    else
      IsMatchResult(matched)
  }

  def seqMethod(value: ListValue): CollectionPredicate = single(value)

  def name = "single"

  def rewrite(f: Expression => Expression): Expression =
    f(SingleInList(collection.rewrite(f), innerVariableName, innerVariableOffset, inner.rewriteAsPredicate(f)))
}
