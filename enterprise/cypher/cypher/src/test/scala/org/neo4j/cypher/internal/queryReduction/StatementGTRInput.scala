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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.queryReduction.ast.ASTNodeHelper._

class StatementGTRInput(initialStatement: Statement) extends GTRInput[Statement](initialStatement) {
  override def depth: Int = getDepth(currentTree)
  override def size: Int = getSize(currentTree)
  override def getDDInput(level: Int) = StatementLevelDDInput(currentTree, level)
  override def getBTInput(level: Int) = StatementLevelBTInput(currentTree, level)
}
