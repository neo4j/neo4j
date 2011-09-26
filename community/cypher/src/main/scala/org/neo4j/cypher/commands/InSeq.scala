package org.neo4j.cypher.commands

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
abstract class InSeq(seqValue: Value, symbolName: String, innerClause: Clause) extends Clause {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean

  def isMatch(m: Map[String, Any]): Boolean = {
    val seq = seqValue(m).asInstanceOf[Seq[_]]
    seqMethod(seq)(item => {
      val innerMap = m ++ Map(symbolName -> item)
      innerClause.isMatch(innerMap)
    })
  }

  def dependsOn: Set[String] = (seqValue.dependsOn ++ innerClause.dependsOn).filterNot( _ == symbolName )

  def atoms: Seq[Clause] = Seq(this)
}

case class AllInSeq(seqValue: Value, symbolName: String, innerClause: Clause) extends InSeq(seqValue, symbolName, innerClause) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = f.forall _
}

case class AnyInSeq(seqValue: Value, symbolName: String, innerClause: Clause) extends InSeq(seqValue, symbolName, innerClause) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = f.exists _
}

case class NoneInSeq(seqValue: Value, symbolName: String, innerClause: Clause) extends InSeq(seqValue, symbolName, innerClause) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = x => !f.exists(x)
}

case class SingleInSeq(seqValue: Value, symbolName: String, innerClause: Clause) extends InSeq(seqValue, symbolName, innerClause) {
  def seqMethod[U](f: Seq[U]): ((U) => Boolean) => Boolean = x => f.filter(x).length == 1
}