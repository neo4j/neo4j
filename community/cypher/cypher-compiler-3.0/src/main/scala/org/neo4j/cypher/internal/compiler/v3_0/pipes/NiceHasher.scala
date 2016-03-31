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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

class NiceHasher(val original: Seq[Any]) {
  override def equals(p1: Any): Boolean = p1 match {
    case null => false
    case other: NiceHasher =>
      hash == other.hash && NiceHasherValue.nullSafeEquals(comparableValues, other.comparableValues)
    case _ => false
  }

  private lazy val comparableValues = if (original == null) null else original.map(NiceHasherValue.comparableValuesFun)

  override def toString = hashCode() + " : " + original

  private lazy val hash = NiceHasherValue.seqHashFun(original)

  override def hashCode() = hash
}

class NiceHasherValue(val original: Any) {
  override def equals(p1: Any): Boolean = p1 match {
    case null => false
    case other: NiceHasherValue =>
      hash == other.hash && NiceHasherValue.nullSafeEquals(comparableValue, other.comparableValue)
    case _ => false
  }

  private lazy val comparableValue = NiceHasherValue.comparableValuesFun(original)

  override def toString = hashCode() + " : " + original

  private lazy val hash = NiceHasherValue.hashFun(original)

  override def hashCode() = hash
}

object NiceHasherValue {
  def seqHashFun(itr: Iterable[Any]): Int = if (itr == null) 0
  else itr.foldLeft(0) ((hashValue, element) => hashFun(element) + hashValue * 31 )

  def hashFun(y: Any): Int = y match {
    case x: Array[Int] => java.util.Arrays.hashCode(x)
    case x: Array[Long] => java.util.Arrays.hashCode(x)
    case x: Array[Byte] => java.util.Arrays.hashCode(x)
    case x: Array[AnyRef] => java.util.Arrays.deepHashCode(x)
    case null => 0
    case x: List[_] => seqHashFun(x)
    case x: Map[_, _] => seqHashFun(x.keySet ++ x.values)
    case x => x.hashCode()
  }

  def comparableValuesFun(y: Any): Any = y match {
    case x: Array[_] => x.deep
    case x: List[_] => x.map(comparableValuesFun)
    case x: Map[_, _] => (x.keys ++ x.values).map(comparableValuesFun)
    case x => x
  }

  def nullSafeEquals(me: Any, other: Any): Boolean =
    if ( me == null ) {
      other == null
    } else {
      me equals other
    }
}
