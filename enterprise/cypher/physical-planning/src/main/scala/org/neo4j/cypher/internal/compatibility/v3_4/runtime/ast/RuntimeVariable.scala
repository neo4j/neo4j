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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckResult, SemanticCheckableExpression}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, InternalException}
import org.neo4j.cypher.internal.v3_4.expressions.{LogicalVariable, Expression => ASTExpression}

abstract class RuntimeVariable(override val name: String) extends LogicalVariable with SemanticCheckableExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success

  override def position: InputPosition = InputPosition.NONE

  override def copyId = fail()

  override def renameId(newName: String) = fail()

  override def bumpId = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a RuntimeVariable as Variable")
}
