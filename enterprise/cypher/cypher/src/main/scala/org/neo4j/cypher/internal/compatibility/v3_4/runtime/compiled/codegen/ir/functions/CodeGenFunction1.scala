/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.functions

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._

sealed trait CodeGenFunction1 {
  def apply(arg: CodeGenExpression): CodeGenExpression
}

case object IdCodeGenFunction extends CodeGenFunction1 {

  override def apply(arg: CodeGenExpression): CodeGenExpression = arg match {
    case NodeExpression(n) => IdOf(n)
    case NodeProjection(n) => IdOf(n)
    case RelationshipExpression(r) => IdOf(r)
    case RelationshipProjection(r) => IdOf(r)
    case e => throw new InternalException(s"id function only accepts nodes or relationships not $e")
  }
}

case object TypeCodeGenFunction extends CodeGenFunction1 {

  override def apply(arg: CodeGenExpression): CodeGenExpression = arg match {
    case r: RelationshipExpression => TypeOf(r.relId)
    case r: RelationshipProjection => TypeOf(r.relId)
    case e => throw new InternalException(s"type function only accepts relationships $e")
  }
}
