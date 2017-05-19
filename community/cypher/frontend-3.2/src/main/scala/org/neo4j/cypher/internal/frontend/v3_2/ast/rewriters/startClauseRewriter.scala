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
package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, SemanticDirection, topDown}

/*
 * The objective of this rewriter is to rewrite queries containing `START` to
 * an equivalent query using `MATCH`. In a glorious future where we no longer support
 * START this rewriter can be safely removed.
 */
case object startClauseRewriter extends StatementRewriter {

  override def description: String = "Rewrites queries using START to equivalent MATCH queries"

  override def postConditions: Set[Condition] = Set.empty
  override def instance(context: BaseContext): Rewriter = topDown(Rewriter.lift {
    case start@Start(items, where) =>
      val newPredicates = asPredicates(items)
      val patterns = asPatterns(items)
      val hints = items.collect {
        case hint: LegacyIndexHint => hint
      }
      //Combine with original predicates from the START-clause
      val allPredicates = (newPredicates ++ where.map(_.expression)).toSet
      val newWhere =
        if (allPredicates.isEmpty) None
        else if (allPredicates.size == 1) Some(Where(allPredicates.head)(start.position))
        else Some(Where(Ands(allPredicates)(start.position))(start.position))

      Match(optional = false, Pattern(patterns.map(EveryPath))(start.position), hints, newWhere)(start.position)
  })

  private def asPredicates(items: Seq[StartItem]) = items.collect {
    //We can safely ignore AllNodes and AllRelationships here, i.e. nodes(*) and rels(*) since that is corresponding to
    //no predicate on the identifier

    //START n=nodes(1,5,7)....
    case n@NodeByIds(variable, ids) =>
      val pos = n.position
      val invocation = FunctionInvocation(FunctionName(functions.Id.name)(pos), variable.copyId)(pos)
      In(invocation, ListLiteral(ids)(pos))(pos)

    //START n=nodes({id})....
    case n@NodeByParameter(variable, parameter) =>
      val pos = n.position
      val invocation = FunctionInvocation(FunctionName(functions.Id.name)(pos), variable.copyId)(pos)
      In(invocation, parameter)(pos)

    //START r=rels(1,5,7)....
    case n@RelationshipByIds(variable, ids) =>
      val pos = n.position
      val invocation = FunctionInvocation(FunctionName(functions.Id.name)(pos), variable.copyId)(pos)
      In(invocation, ListLiteral(ids)(pos))(pos)

    //START r=rel({id})....
    case r@RelationshipByParameter(variable, parameter) =>
      val pos = r.position
      val invocation = FunctionInvocation(FunctionName(functions.Id.name)(pos), variable.copyId)(pos)
      In(invocation, parameter)(pos)
  }

  private def asPatterns(items: Seq[StartItem]): Seq[PatternElement] =  items.collect {
    case n: NodeStartItem => NodePattern(Some(n.variable.copyId), Seq.empty, None)(n.position)
    case r: RelationshipStartItem => RelationshipChain(
      NodePattern(Some(Variable(UnNamedNameGenerator.name(r.position))(r.position)), Seq.empty, None)(r.position),
      RelationshipPattern(Some(r.variable.copyId), Seq.empty, None, None, SemanticDirection.OUTGOING)(r.position),
      NodePattern(Some(Variable(UnNamedNameGenerator.name(r.position.bumped()))(r.position)),
                  Seq.empty, None)(r.position))(r.position)
  }

}
