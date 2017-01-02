/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen

import java.util.concurrent.atomic.AtomicInteger

class Namer(classNameCounter: AtomicInteger, varPrefix: String = "v", methodPrefix: String = "m", operationPrefix: String = "OP") {

  private var methodNameCounter = 0
  private var varNameCounter = 0
  private var opNameCounter = 0

  def newMethodName(): String = {
    methodNameCounter += 1
    s"$methodPrefix$methodNameCounter"
  }

  def newVarName(): String = {
    varNameCounter += 1
    s"$varPrefix$varNameCounter"
  }

  def newOpName(planName: String): String = {
    opNameCounter += 1
    s"$operationPrefix${opNameCounter}_$planName"
  }
}

object Namer {

  private val classNameCounter = new AtomicInteger()

  def apply(): Namer = new Namer(classNameCounter)

  def newClassName() = {
    s"GeneratedExecutionPlan${System.nanoTime()}"
  }
}
