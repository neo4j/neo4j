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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.queryReduction.ast.ASTNodeHelper._
import org.neo4j.cypher.internal.queryReduction.ast.copyNodeWith.NodeConverter
import org.neo4j.cypher.internal.queryReduction.ast.{copyNodeWith, domainsOf, getChildren}

case class Candidate(node: ASTNode, expectedType: Class[_])

class StatementLevelBTInput(statement: Statement,
                            level: Int) extends BTInput[Statement, Candidate] {

  override val domains: Array[BTDomain[Candidate]] = replacementCandidates(statement, 0).toArray

  private def replacementCandidates(node: ASTNode, currentLevel: Int): Seq[BTDomain[Candidate]] = {
    // Previous level (to capture the parent information)
    if (currentLevel == level - 1) {
      domainsOf(node)(makeDomain)
    } else {
      getChildren(node).flatMap(replacementCandidates(_, currentLevel + 1))
    }
  }

  private def makeDomain(parent: ASTNode, expectedType: Class[_]): BTDomain[Candidate] = {
    val keepTheNode = BTAssignment(Candidate(parent, expectedType), 0)
    val assignments =
      keepTheNode +:
      getChildren(parent)
      .filter(expectedType.isInstance(_))
      .map(child => BTAssignment(Candidate(child, expectedType), getSize(parent) - getSize(child)))
      .sortBy(_.gain)
    new BTDomain(assignments.toArray)
  }

  override def convertToInput(objects: Seq[Candidate]): Statement = {
    val (newStatement, _) = convertToInput(objects, statement, 0, 0)
    newStatement
  }

  /**
    * Returns tuple: (new node, how much the index advanced)
    */
  def convertToInput[A <: ASTNode](objects: Seq[Candidate], node: A, currentIndex: Int, currentLevel: Int): (A, Int) = {
    if (currentLevel == level) {
      //  Replace node
      (objects(currentIndex).node.asInstanceOf[A], 1)
    } else {
      var indexAdvance = 0

      // Must be invoked for all children
      def newChild[B <: ASTNode](child: B): B = {
        val (replacedChild, indexAdvanceHere) =
          convertToInput(objects, child, currentIndex + indexAdvance, currentLevel + 1)
        indexAdvance = indexAdvance + indexAdvanceHere
        replacedChild
      }

      val nodeConverter = new NodeConverter {
        override def ofOption[B <: ASTNode](o: Option[B]): Option[B] = {
          o.map(newChild)
        }

        override def ofSeq[B <: ASTNode](bs: Seq[B]): Seq[B] = {
          bs.map(newChild)
        }

        override def ofSingle[B <: ASTNode](b: B): B = {
          newChild(b)
        }

        override def ofTupledSeq[B <: ASTNode, C <: ASTNode](bs: Seq[(B, C)]): Seq[(B, C)] = {
          bs.map { case (b,c) => (newChild(b), newChild(c))}
        }
      }

      val newNode = copyNodeWith(node, nodeConverter)

      (newNode, indexAdvance)
    }
  }

  override def getNewAssignments(currentAssignment: BTAssignment[Candidate]): Seq[BTAssignment[Candidate]] = {
    val previousGain = currentAssignment.gain
    val currentNode = currentAssignment.obj.node
    val typ = currentAssignment.obj.expectedType

    getChildren(currentNode).flatMap { node =>
      val additionalGain = getSize(currentNode) - getSize(node)

      if (typ.isInstance(node)) {
        Some(BTAssignment(Candidate(node, typ), previousGain + additionalGain))
      } else {
        None
      }
    }
  }
}

object StatementLevelBTInput {
  def apply(statement: Statement, level: Int): StatementLevelBTInput = {
    new StatementLevelBTInput(statement, level)
  }
}
