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

import org.neo4j.cypher.internal.compiler.v2_3.birk.codegen.ExceptionCodeGen

trait Instruction {
  // Actual code produced by element
  def generateCode(): String

  final def importedClasses() = allLeafs.flatMap(_._importedClasses()).toSet

  //generate class level members
  def members(): String

  //Initialises necessary data-structures. Is inserted at the top of the generated method
  def generateInit(): String

  def methods: Seq[Method] = {
    (allLeafs :+ this).flatMap(_._method)
  }

  def exceptions: Set[ExceptionCodeGen] = allLeafs.flatMap(_._exceptions()).toSet

  // Generates import list for class - implement this!
  protected def _importedClasses(): Set[String]

  protected def _exceptions(): Set[ExceptionCodeGen] = Set.empty

  protected def children: Seq[Instruction] = Seq.empty

  def operatorId: Option[String] = None

  protected def _method: Option[Method] = None

  private def allLeafs: Seq[Instruction] = {
    val grandKids = children.foldLeft(Seq.empty[Instruction]) {
      case (acc, child) => acc ++ child.allLeafs
    }

    children ++ grandKids
  }

  def operatorIds: Set[String] = {
    (operatorId.getOrElse("") +: children.flatMap(_.operatorIds)).toSet.filter(_.nonEmpty)
  }
}

object Instruction {
  val empty = new Instruction {
    override def generateCode() = ""

    override def members() = ""

    override def generateInit() = ""

    override def exceptions: Set[ExceptionCodeGen] = Set.empty

    override protected def _importedClasses() = Set.empty
  }
}
