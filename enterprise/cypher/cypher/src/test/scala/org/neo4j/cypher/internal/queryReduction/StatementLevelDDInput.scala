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
import org.neo4j.cypher.internal.queryReduction.ast.copyNodeWith
import org.neo4j.cypher.internal.queryReduction.ast.copyNodeWith.NodeConverter

class StatementLevelDDInput(statement: Statement,
                            level: Int,
                            originalLength: Int
                           ) extends DDInput[Statement](originalLength) {

  override def getCurrentCode: Statement = {
    val (maybeStatement, _, _) = removeChildrenInSubTree(statement, 0, 0)
    maybeStatement.get
  }

  /**
    * Returns tuple: (Node that may be modified or None, if this subtree was modified, how much the index advanced)
    */
  private def removeChildrenInSubTree[A <: ASTNode](node: A, currentIndex: Int, currentLevel: Int): (Option[A], Boolean, Int) = {
    if (currentLevel == level) {
      // Find out if the current node should be removed
      if (!activeTokens.contains(currentIndex)) {
        (None, true, 1)
      } else {
        (Some(node), false, 1)
      }
    } else {
      var hasChanged = false
      var indexAdvance = 0

      // Must be invoked for all children
      def newChild[B <: ASTNode](child: B): Option[B] = {
        val (maybeChild, hasChangedHere, indexAdvanceHere) =
          removeChildrenInSubTree(child, currentIndex + indexAdvance, currentLevel + 1)
        hasChanged = hasChanged || hasChangedHere
        indexAdvance = indexAdvance + indexAdvanceHere
        maybeChild
      }

      val nodeConverter = new NodeConverter {
        override def ofOption[B <: ASTNode](o: Option[B]): Option[B] = {
          o.flatMap(newChild)
        }

        override def ofSingle[B <: ASTNode](b: B): B = {
          newChild(b).getOrElse(throw new IllegalSyntaxException())
        }

        override def ofSeq[B <: ASTNode](bs: Seq[B]): Seq[B] = {
          bs.map(newChild).filter(_.isDefined).map(_.get)
        }

        override def ofTupledSeq[B <: ASTNode, C <: ASTNode](bs: Seq[(B, C)]): Seq[(B, C)] = {
          bs.flatMap { case (b,c) =>
            val optionTuple = (newChild(b), newChild(c))
              optionTuple match {
                case (None, None) => None
                case (Some(bb), Some(cc)) => Some((bb, cc))
                  // You must either keep or delete both children in a tuple
                case _ => throw new IllegalSyntaxException()
              }
          }
        }
      }

      val newNode = copyNodeWith(node, nodeConverter)

      if (!hasChanged) {
        (Some(node), false, indexAdvance)
      } else {
        (Some(newNode), true, indexAdvance)
      }
    }
  }
}

object StatementLevelDDInput {
  def apply(statement: Statement, level: Int): StatementLevelDDInput = {
    val originalLength: Int = countNodesOnLevel(statement, level)
    new StatementLevelDDInput(statement, level, originalLength)
  }
}
