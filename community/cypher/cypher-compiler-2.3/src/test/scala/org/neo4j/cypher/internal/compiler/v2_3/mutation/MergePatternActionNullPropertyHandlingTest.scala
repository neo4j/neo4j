/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.commands.{RelatedTo, SingleNode, VarLengthRelatedTo}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, InvalidSemanticsException}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.helpers.ThisShouldNotHappenError

class MergePatternActionNullPropertyHandlingTest extends CypherFunSuite {
  import MergePatternAction.ensureNoNullRelationshipPropertiesInPatterns

  test("should detect null properties in plain patterns") {
    val pattern = RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq("X"), SemanticDirection.OUTGOING, Map("props" -> Literal(null)))
    evaluating {
      ensureNoNullRelationshipPropertiesInPatterns(Seq(pattern), ExecutionContext.empty, QueryStateHelper.empty)
    } should produce[InvalidSemanticsException]
  }

  test("should throw when given a var length pattern") {
    val pattern = VarLengthRelatedTo("p", SingleNode("a"), SingleNode("b"), None, None, Seq("X"), SemanticDirection.OUTGOING, Some("r"), Map("props" -> Literal(null)))
    evaluating {
      ensureNoNullRelationshipPropertiesInPatterns(Seq(pattern), ExecutionContext.empty, QueryStateHelper.empty)
    } should produce[ThisShouldNotHappenError]
  }

  test("should throw when given a unique link") {
    val pattern = UniqueLink(NamedExpectation("a"), NamedExpectation("b"), NamedExpectation("r"), "X", SemanticDirection.OUTGOING)
    evaluating {
      ensureNoNullRelationshipPropertiesInPatterns(Seq(pattern), ExecutionContext.empty, QueryStateHelper.empty)
    } should produce[ThisShouldNotHappenError]
  }
}
