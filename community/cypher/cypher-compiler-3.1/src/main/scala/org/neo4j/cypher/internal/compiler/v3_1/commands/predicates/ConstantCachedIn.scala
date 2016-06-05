/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.predicates

import java.util.Map.Entry

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_1.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryState

/*
This class is used for making the common <exp> IN <constant-expression> fast

It uses a cache for the <constant-expression>, and turns it into a Set, for fast existence checking
 */
case class ConstantCachedIn(value: Expression, list: Expression) extends Predicate with CollectionSupport {

  var executed = 0
  // These two are here to make the fields accessible without conflicting with the case classes
  override def isMatch(ctx: ExecutionContext)(implicit state: QueryState) = {
    val inChecker = state.cachedIn.getOrElseUpdate(list, {
      val listValue = list(ctx)
      val checker = if (listValue == null)
        NullListChecker
      else
        new BuildUp(makeTraversable(listValue).toIterator)
      new InCheckContainer(checker)
    })

    inChecker.contains(value(ctx))
  }

  override def containsIsNull = false

  override def arguments = Seq(list)

  override def symbolTableDependencies = list.symbolTableDependencies ++ value.symbolTableDependencies

  override def rewrite(f: (Expression) => Expression) = f(ConstantCachedIn(value.rewrite(f), list.rewrite(f)))
}

/*
This class is used for making the common <exp> IN <rhs-expression> fast

It uses a cache for the <rhs-expression> value, and turns it into a Set, for fast existence checking
 */
case class DynamicCachedIn(value: Expression, list: Expression) extends Predicate with CollectionSupport {

  // These two are here to make the fields accessible without conflicting with the case classes
  override def isMatch(ctx: ExecutionContext)(implicit state: QueryState) = {
    val listValue = list(ctx)

    val inChecker = state.cachedIn.getOrElseUpdate(listValue, {
      val checker = if (listValue == null)
        NullListChecker
      else
        new BuildUp(makeTraversable(listValue).toIterator)
      new InCheckContainer(checker)
    })

    inChecker.contains(value(ctx))
  }

  override def containsIsNull = false

  override def arguments = Seq(list)

  override def symbolTableDependencies = list.symbolTableDependencies ++ value.symbolTableDependencies

  override def rewrite(f: (Expression) => Expression) = f(DynamicCachedIn(value.rewrite(f), list.rewrite(f)))
}

object CachedIn {
  def unapply(arg: Expression): Option[(Expression, Expression)] = arg match {
    case DynamicCachedIn(value, list) => Some((value, list))
    case ConstantCachedIn(value, list) => Some((value, list))
    case _ => None
  }
}

/*
This is a simple container that keep the latest state of the cached IN check
 */
class InCheckContainer(var checker: Checker) {
  def contains(value: Any): Option[Boolean] = {
    val (result, newChecker) = checker.contains(value)
    checker = newChecker
    result
  }
}

class SingleThreadedLRUCache[K,V](maxSize: Int) extends java.util.LinkedHashMap[K,V](maxSize, 0.75f, true) {
  override def removeEldestEntry(eldest: Entry[K, V]): Boolean = size() > maxSize

  def getOrElseUpdate(key: K, f: => V): V = {
    val v = get(key)
    Option(v).getOrElse {
      val value = f
      put(key, value)
      value
    }
  }
}

