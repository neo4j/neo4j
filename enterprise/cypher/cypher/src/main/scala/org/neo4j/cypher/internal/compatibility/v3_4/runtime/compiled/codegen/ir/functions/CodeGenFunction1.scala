/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
