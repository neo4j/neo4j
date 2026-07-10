/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.DifferentNodes
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.DisjointNodes
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatAcyclic
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.NoneOfNodes
import org.neo4j.cypher.internal.expressions.NoneOfNodesInVarLengthRelationship
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.UniqueNodes
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TraversalPathModeTest extends CypherFunSuite {

  private val pos = InputPosition.NONE

  private val n1 = Variable("n1", pos)
  private val n2 = Variable("n2", pos)
  private val r = Variable("r", pos)
  private val r2 = Variable("r2", pos)
  private val group = Variable("group", pos)

  private val nodeUniquenessPredicates: Seq[Expression] = Seq(
    DifferentNodes(n1, n2)(pos),
    NoneOfNodes(n1, group)(pos),
    NoneOfNodesInVarLengthRelationship(n1, r, OUTGOING, mustBeInlined = false)(pos),
    DisjointNodes(n1, n2, 1L, Some(2L))(pos),
    UniqueNodes(group, None)(pos),
    IsRepeatAcyclic(n1)(pos)
  )

  private val relationshipUniquenessPredicates: Seq[Expression] = Seq(
    DifferentRelationships(r, r2)(pos),
    NoneOfRelationships(r, group)(pos),
    Disjoint(r, r2)(pos),
    Unique(group)(pos),
    IsRepeatTrailUnique(r)(pos)
  )

  private val otherPredicate: Expression = Equals(n1, n2)(pos)

  private def forAllRepetitions(inner: Expression): Expression =
    ForAllRepetitions(group, Set(VariableGrouping(n1, group)(pos)), inner)(pos)

  for (predicate <- nodeUniquenessPredicates) {
    test(s"any node-uniqueness predicate implies Acyclic: ${predicate.getClass.getSimpleName}") {
      TraversalPathMode.getFromPredicates(Seq(predicate)) shouldBe TraversalPathMode.Acyclic
    }
  }

  for (predicate <- relationshipUniquenessPredicates) {
    test(s"any relationship-uniqueness predicate implies Trail: ${predicate.getClass.getSimpleName}") {
      TraversalPathMode.getFromPredicates(Seq(predicate)) shouldBe TraversalPathMode.Trail
    }
  }

  test("node uniqueness takes precedence over relationship uniqueness") {
    TraversalPathMode.getFromPredicates(
      relationshipUniquenessPredicates ++ nodeUniquenessPredicates
    ) shouldBe TraversalPathMode.Acyclic
  }

  test("ForAllRepetitions is classified by its inner predicate") {
    TraversalPathMode.getFromPredicates(
      Seq(forAllRepetitions(DifferentNodes(n1, n2)(pos)))
    ) shouldBe TraversalPathMode.Acyclic
    TraversalPathMode.getFromPredicates(
      Seq(forAllRepetitions(DifferentRelationships(r, r2)(pos)))
    ) shouldBe TraversalPathMode.Trail
    TraversalPathMode.getFromPredicates(
      Seq(forAllRepetitions(otherPredicate))
    ) shouldBe TraversalPathMode.Walk
  }

  test("alwaysTrail overrides any predicates") {
    TraversalPathMode.getFromPredicates(
      nodeUniquenessPredicates ++ relationshipUniquenessPredicates,
      alwaysTrail = true
    ) shouldBe TraversalPathMode.Trail
    TraversalPathMode.getFromPredicates(Seq.empty, alwaysTrail = true) shouldBe TraversalPathMode.Trail
  }

  test("no uniqueness predicates implies Walk") {
    TraversalPathMode.getFromPredicates(Seq.empty) shouldBe TraversalPathMode.Walk
    TraversalPathMode.getFromPredicates(Seq(otherPredicate)) shouldBe TraversalPathMode.Walk
  }
}
