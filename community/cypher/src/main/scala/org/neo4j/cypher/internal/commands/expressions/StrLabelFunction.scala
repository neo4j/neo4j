/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.commands.values.LabelName

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
case class StrLabelFunction(labelStr: Expression) extends NullInNullOutExpression(labelStr) {

  def compute(value: Any, m: ExecutionContext) = value match {
    case s: String => LabelName(s)
    case _ => throw new CypherTypeException("Cannot convert value that is not a string to a label")
  }

  def rewrite(f: (Expression) => Expression) = f(StrLabelFunction(labelStr.rewrite(f)))

  def children = Seq(labelStr)

  def identifierDependencies(expectedType: CypherType) = null

  def calculateType(symbols: SymbolTable) = {
    labelStr.evaluateType(StringType(), symbols)
    LabelType()
  }

  def symbolTableDependencies = labelStr.symbolTableDependencies
}