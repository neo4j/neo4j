/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import collection.Seq
import org.neo4j.cypher.internal.symbols.{Identifier, AnyIterableType}
import collection.Map

abstract class InIterable(expression: Expression, symbol: String, closure: Predicate) extends Predicate {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean

  def isMatch(m: Map[String, Any]): Boolean = {
    val seq = expression(m) match {
      case x:Seq[_] => x
      case x:Array[_] => x.toSeq
    }

    seqMethod(seq)(item => {
      val innerMap = m ++ Map(symbol -> item)
      closure.isMatch(innerMap)
    })
  }

  def dependencies: Seq[Identifier] = expression.dependencies(AnyIterableType()) ++ closure.dependencies.filterNot(_.name == symbol)
  def atoms: Seq[Predicate] = Seq(this)
  def exists(f: (Expression) => Boolean) = expression.exists(f)||closure.exists(f)
  def name:String
  override def toString = name + "(" + symbol + " in " + expression + " where " + closure + ")"
  def containsIsNull = closure.containsIsNull
  def filter(f: (Expression) => Boolean): Seq[Expression] = expression.filter(f) ++ closure.filter(f)
}

case class AllInIterable(iterable: Expression, symbolName: String, inner: Predicate) extends InIterable(iterable, symbolName, inner) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = f.forall _
  def name = "all"
  def rewrite(f: (Expression) => Expression) = AllInIterable(iterable.rewrite(f), symbolName, inner.rewrite(f))
}

case class AnyInIterable(iterable: Expression, symbolName: String, inner: Predicate) extends InIterable(iterable, symbolName, inner) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = f.exists _
  def name = "any"
  def rewrite(f: (Expression) => Expression) = AnyInIterable(iterable.rewrite(f), symbolName, inner.rewrite(f))
}

case class NoneInIterable(iterable: Expression, symbolName: String, inner: Predicate) extends InIterable(iterable, symbolName, inner) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = x => !f.exists(x)
  def name = "none"
  def rewrite(f: (Expression) => Expression) = NoneInIterable(iterable.rewrite(f), symbolName, inner.rewrite(f))
}

case class SingleInIterable(iterable: Expression, symbolName: String, inner: Predicate) extends InIterable(iterable, symbolName, inner) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = x => f.filter(x).length == 1
  def name = "single"
  def rewrite(f: (Expression) => Expression) = SingleInIterable(iterable.rewrite(f), symbolName, inner.rewrite(f))
}
