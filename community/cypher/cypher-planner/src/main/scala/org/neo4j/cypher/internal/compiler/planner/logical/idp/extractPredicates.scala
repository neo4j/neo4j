/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

object extractPredicates {

  // Using type predicates to make this more readable.
  type NodePredicates = List[Expression]
  type RelationshipPredicates = List[Expression]
  type SolvedPredicates = List[Expression] // for marking predicates as solved

  def apply(availablePredicates: Seq[Expression],
            originalRelationshipName: String,
            tempRelationship: String,
            tempNode: String,
            originalNodeName: String)
    : (NodePredicates, RelationshipPredicates, SolvedPredicates) = {

    /*
    We extract predicates that we can evaluate eagerly during the traversal, which allows us to abort traversing
    down paths that would not match. To make it easy to evaluate these predicates, we rewrite them a little bit so
    a single slot can be used for all predicates against a relationship (similarly done for nodes)

    During the folding, we also accumulate the original predicate, which we can mark as solved by this plan.
     */
    val seed: (NodePredicates, RelationshipPredicates, SolvedPredicates) =
      (List.empty, List.empty, List.empty)

    /**
      * Checks if an inner predicate depends on the path (i.e. the original start node or relationship). In that case
      * we cannot solve the predicates during the traversal.
      *
      * We don't need to check for dependencies on the end node, since such predicates are not even suggested as
      * available predicates here.
      */
    def pathDependent(innerPredicate: Expression) = {
      val names = innerPredicate.dependencies.map(_.name)
      names.contains(originalRelationshipName)
    }

    availablePredicates.foldLeft(seed) {

      //MATCH ()-[r* {prop:1337}]->()
      case (
          (n, e, s),
          p @ AllRelationships(variable, `originalRelationshipName`, innerPredicate)) =>
        val rewrittenPredicate = innerPredicate.endoRewrite(replaceVariable(variable, tempRelationship))
        (n, e :+ rewrittenPredicate, s :+ p)

      //MATCH p = (a)-[x*]->(b) WHERE ALL(r in relationships(p) WHERE r.prop > 5)
      case ((n, e, s),
            p @ AllRelationshipsInPath(`originalNodeName`, `originalRelationshipName`, variable, innerPredicate))
            if !pathDependent(innerPredicate) =>
        val rewrittenPredicate = innerPredicate.endoRewrite(replaceVariable(variable, tempRelationship))
        (n, e :+ rewrittenPredicate, s :+ p)

      //MATCH p = ()-[*]->() WHERE NONE(r in relationships(p) WHERE <innerPredicate>)
      case ((n, e, s),
            p @ NoRelationshipInPath(`originalNodeName`, `originalRelationshipName`, variable, innerPredicate))
            if !pathDependent(innerPredicate) =>
        val rewrittenPredicate = innerPredicate.endoRewrite(replaceVariable(variable, tempRelationship))
        val negatedPredicate = Not(rewrittenPredicate)(innerPredicate.position)
        (n, e :+ negatedPredicate, s :+ p)

      //MATCH p = ()-[*]->() WHERE ALL(r in nodes(p) WHERE <innerPredicate>)
      case ((n, e, s),
            p @ AllNodesInPath(`originalNodeName`, `originalRelationshipName`, variable, innerPredicate))
            if !pathDependent(innerPredicate) =>
        val rewrittenPredicate = innerPredicate.endoRewrite(replaceVariable(variable, tempNode))
        (n :+ rewrittenPredicate, e, s :+ p)

      //MATCH p = ()-[*]->() WHERE NONE(r in nodes(p) WHERE <innerPredicate>)
      case ((n, e, s),
            p @ NoNodeInPath(`originalNodeName`, `originalRelationshipName`, variable, innerPredicate))
            if !pathDependent(innerPredicate) =>
        val rewrittenPredicate = innerPredicate.endoRewrite(replaceVariable(variable, tempNode))
        val negatedPredicate = Not(rewrittenPredicate)(innerPredicate.position)
        (n :+ negatedPredicate, e, s :+ p)

      case (acc, _) =>
        acc
    }
  }

  private def replaceVariable(from: LogicalVariable, to: String): Rewriter =
    bottomUp(Rewriter.lift {
      case v: Variable if v == from => Variable(to)(v.position)
    })

  object AllRelationships {
    def unapply(v: Any): Option[(LogicalVariable, String, Expression)] =
      v match {
        case AllIterablePredicate(FilterScope(variable, Some(innerPredicate)), relId @ LogicalVariable(name))
            if variable == relId || !innerPredicate.dependencies(relId) =>
          Some((variable, name, innerPredicate))

        case _ => None
      }
  }

  object AllRelationshipsInPath {
    def unapply(v: Any): Option[(String, String, LogicalVariable, Expression)] =
      v match {
        case AllIterablePredicate(
            FilterScope(variable, Some(innerPredicate)),
            FunctionInvocation(
              _, // namespace
              FunctionName(fname),
              false, //distinct
              Seq(
                PathExpression(
                  NodePathStep(
                    startNode: LogicalVariable,
                    MultiRelationshipPathStep(rel: LogicalVariable, _, _, NilPathStep))))))
            if fname == "relationships" =>
          Some((startNode.name, rel.name, variable, innerPredicate))

        case _ => None
      }
  }

  object AllNodesInPath {
    def unapply(v: Any): Option[(String, String, LogicalVariable, Expression)] =
      v match {
        case AllIterablePredicate(
            FilterScope(variable, Some(innerPredicate)),
            FunctionInvocation(
              _,
              FunctionName(fname),
              false,
              Seq(
                PathExpression(
                  NodePathStep(
                    startNode: LogicalVariable,
                    MultiRelationshipPathStep(rel: LogicalVariable, _, _, NilPathStep))))))
            if fname == "nodes" =>
          Some((startNode.name, rel.name, variable, innerPredicate))

        case _ => None
      }
  }

  object NoRelationshipInPath {
    def unapply(v: Any): Option[(String, String, LogicalVariable, Expression)] =
      v match {
        case NoneIterablePredicate(
            FilterScope(variable, Some(innerPredicate)),
            FunctionInvocation(
              _,
              FunctionName(fname),
              false,
              Seq(
                PathExpression(
                  NodePathStep(
                    startNode: LogicalVariable,
                    MultiRelationshipPathStep(rel: LogicalVariable, _, _, NilPathStep))))))
            if fname == "relationships" =>
          Some((startNode.name, rel.name, variable, innerPredicate))

        case _ => None
      }
  }

  object NoNodeInPath {
    def unapply(v: Any): Option[(String, String, LogicalVariable, Expression)] =
      v match {
        case NoneIterablePredicate(
            FilterScope(variable, Some(innerPredicate)),
            FunctionInvocation(
              _,
              FunctionName(fname),
              false,
              Seq(
                PathExpression(
                  NodePathStep(
                    startNode: LogicalVariable,
                    MultiRelationshipPathStep(rel: LogicalVariable, _, _, NilPathStep))))))
            if fname == "nodes" =>
          Some((startNode.name, rel.name, variable, innerPredicate))

        case _ => None
      }
  }

}
