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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter

/**
 * A transient data structure that sits in between the AST and the IR.
 * It encodes the nesting of unions and (call) sub-queries.
 * Clauses are grouped together into fragments based on which graph they need run against.
 */
sealed trait CompositeQuery

object CompositeQuery {
  sealed trait Single extends CompositeQuery

  object Single {

    /**
     * A single query that needs to run on a component graph.
     * This construct is bound to become a Fragment once we implement "USE anywhere" (CIP-69).
     *
     * @param graphReference the graph on which to run
     * @param clauses the clauses making up the query fragment, with the USE clause removed, and the importing WITH rewritten to use parameters
     * @param parameters query parameters used inside of the query fragment
     * @param importsAsParameters variables imported from the outer query inside of the query fragment are passed via additional parameters; mapping from the parameters to the original variables
     */
    final case class Foreign(
      graphReference: ast.GraphReference,
      clauses: Seq[ast.Clause],
      parameters: Set[Parameter],
      importsAsParameters: Map[Parameter, LogicalVariable]
    ) extends Single

    /**
     * A single query with its clauses and sub-queries grouped together into fragments based on which graph they need run against.
     */
    final case class Fragments(
      arguments: Set[LogicalVariable],
      fragments: Seq[Fragment]
    ) extends Single
  }

  /**
   * A group of clauses, or a sub-query, part of a composite query.
   */
  sealed trait Fragment

  object Fragment {

    /**
     * Standard consecutive clauses to be run on the composite database.
     */
    final case class Standard(clauses: Seq[ast.Clause]) extends Fragment

    /**
     * A potentially composite CALL sub-query.
     * At the time of writing, we do not support composite queries inside other types of sub-queries.
     */
    final case class SubQuery(
      innerQuery: CompositeQuery,
      isCorrelated: Boolean,
      isYielding: Boolean,
      inTransactionsParameters: Option[ast.SubqueryCall.InTransactionsParameters],
      optional: Boolean
    ) extends Fragment
  }

  /**
   * The union of potentially composite queries.
   */
  final case class Union(
    unionType: Union.Type,
    lhs: CompositeQuery,
    rhs: Single,
    unionMappings: List[ast.Union.UnionMapping]
  ) extends CompositeQuery

  object Union {
    sealed trait Type

    object Type {
      final case object All extends Type
      final case object Distinct extends Type
    }
  }
}
