/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.graphdb.Direction

class MatchClauseTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should not allow adding constraints to already bound nod identifiers") {
    val nodePattern: NodePattern = NodePattern(Some(ident("a")), Seq(LabelName("A")_), None, naked = false)_
    val pattern: Pattern = Pattern(Seq(EveryPath(nodePattern)))_
    val matchClause: Match = Match(optional = false, pattern, Seq.empty, None)_

    val state = SemanticState.clean.declareIdentifier(ident("a"), CTNode.invariant).right.get
    val result = matchClause.semanticCheck(state)

    result.errors should have size 1
    result.errors.head.msg should startWith("Cannot add labels or properties on a node which is already bound")
  }

  test("should not allow adding constraints to already bound relationship identifiers") {
    val nodePattern: NodePattern = NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_
    val relPattern: RelationshipPattern = RelationshipPattern(Some(ident("r")), optional = false, Seq(RelTypeName("R")_), None, None, Direction.OUTGOING)_
    val pattern: Pattern = Pattern(Seq(EveryPath(RelationshipChain(nodePattern, relPattern, nodePattern)_)))_
    val matchClause: Match = Match(optional = false, pattern, Seq.empty, None)_

    val state = SemanticState.clean.declareIdentifier(ident("a"), CTNode.invariant).right.get
      .declareIdentifier(ident("r"), CTRelationship.invariant).right.get
    val result = matchClause.semanticCheck(state)

    result.errors should have size 1
    result.errors.head.msg should startWith("Cannot add types or properties on a relationship which is already bound")
  }
}
