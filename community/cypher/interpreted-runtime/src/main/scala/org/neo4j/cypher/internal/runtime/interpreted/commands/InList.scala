/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Closure, Expression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ListSupport}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue

import scala.collection.Seq

/**
  * These classes solve List Predicates.
  */
abstract class InList(collection: Expression,
                      innerVariableName: String,
                      innerVariableOffset: Int,
                      predicate: Predicate)
  extends Predicate
  with ListSupport
  with Closure {

  type CollectionPredicate = (AnyValue => Option[Boolean]) => Option[Boolean]

  def seqMethod(f: ListValue): CollectionPredicate

  def isMatch(row: ExecutionContext, state: QueryState): Option[Boolean] = {
    val list = collection(row, state)

    if (list eq Values.NO_VALUE) None
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

  def containsIsNull: Boolean = predicate.containsIsNull

  override def children: Seq[Expression] = Seq(collection, predicate)

  def arguments: scala.Seq[Expression] = Seq(collection)

}

case class AllInList(collection: Expression,
                     innerVariableName: String,
                     innerVariableOffset: Int,
                     inner: Predicate)
  extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def forAll(collectionValue: ListValue)(predicate: AnyValue => Option[Boolean]): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(false) => return Some(false)
        case None        => result = None
        case _           =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = forAll(value)
  def name = "all"

  def rewrite(f: Expression => Expression): Expression =
    f(AllInList(collection.rewrite(f),
                innerVariableName,
                innerVariableOffset,
                inner.rewriteAsPredicate(f)))
}

case class AnyInList(collection: Expression,
                     innerVariableName: String,
                     innerVariableOffset: Int,
                     inner: Predicate)
  extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def exists(collectionValue: ListValue)(predicate: AnyValue => Option[Boolean]): Option[Boolean] = {
    var result: Option[Boolean] = Some(false)
    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) => return Some(true)
        case None       => result = None
        case _          =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = exists(value)

  def name = "any"

  def rewrite(f: Expression => Expression): Expression =
    f(AnyInList(collection.rewrite(f),
                innerVariableName,
                innerVariableOffset,
                inner.rewriteAsPredicate(f)))
}

case class NoneInList(collection: Expression,
                      innerVariableName: String,
                      innerVariableOffset: Int,
                      inner: Predicate)
  extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def none(collectionValue: ListValue)(predicate: AnyValue => Option[Boolean]): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) => return Some(false)
        case None       => result = None
        case _          =>
      }
    }

    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = none(value)

  def name = "none"

  def rewrite(f: Expression => Expression): Expression =
    f(NoneInList(collection.rewrite(f),
                 innerVariableName,
                 innerVariableOffset,
                 inner.rewriteAsPredicate(f)))
}

case class SingleInList(collection: Expression,
                        innerVariableName: String,
                        innerVariableOffset: Int,
                        inner: Predicate)
  extends InList(collection, innerVariableName, innerVariableOffset, inner) {

  private def single(collectionValue: ListValue)(predicate: AnyValue => Option[Boolean]): Option[Boolean] = {
    var matched = false
    var atLeastOneNull = false
    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) if matched => return Some(false)
        case Some(true)            => matched = true
        case None                  => atLeastOneNull = true
        case _                     =>
      }
    }

    if (atLeastOneNull)
      None
    else
      Some(matched)
  }

  def seqMethod(value: ListValue): CollectionPredicate = single(value)

  def name = "single"

  def rewrite(f: Expression => Expression): Expression =
    f(SingleInList(collection.rewrite(f),
                   innerVariableName,
                   innerVariableOffset,
                   inner.rewriteAsPredicate(f)))
}
