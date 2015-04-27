/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

trait Instruction {
  // Actual code produced by element
  def generateCode(): String

  def importedClasses() = allLeafs.flatMap(_._importedClasses()).toSet

  //generate class level members
  def fields(): String

  // Initialises necessary data-structures. Is inserted at the top of the generated method
  def generateInit(): String

  def methods: Seq[Method] = {
    (allLeafs :+ this).flatMap(_._method)
  }

  // Generates import list for class
  protected def _importedClasses(): Set[String] = Set.empty

  protected def children: Seq[Instruction] = Seq.empty

  protected def _method: Option[Method] = None

  private def allLeafs: Seq[Instruction] = {
    val grandKids = children.foldLeft(Seq.empty[Instruction]) {
      case (acc, child) => acc ++ child.allLeafs
    }

    children ++ grandKids
  }
}

object Instruction {
  val empty = new Instruction {
    override def generateCode(): String = ""

    override def fields(): String = ""

    override def generateInit(): String = ""
  }
}
