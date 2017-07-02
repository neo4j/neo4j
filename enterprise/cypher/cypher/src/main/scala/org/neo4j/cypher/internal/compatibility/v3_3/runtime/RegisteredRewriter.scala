/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ast => runtimeAst, plans => physical}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{plans => logical}
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, ast => parserAst}

class RegisteredRewriter(pipelineInformation: Map[LogicalPlan, PipelineInformation]) extends Rewriter {

  override def apply(v1: AnyRef): AnyRef = instance.apply(v1)

  private val instance: Rewriter = {
    val stateChange: AnyRef => Option[PipelineInformation] = {
      case lp: LogicalPlan => Some(pipelineInformation(lp))
      case _ => None
    }

    val rewriteCreator: PipelineInformation => Rewriter = { (pipelineInformation: PipelineInformation) =>
      val rewriter: Rewriter = Rewriter.lift {
        case logical.ProduceResult(columns, src) =>
          val newColumns: Seq[(String, parserAst.Expression)] = columns map { c =>
            pipelineInformation(c) match {
              case LongSlot(offset, false, CTNode) =>
                c -> runtimeAst.NodeFromRegister(offset)
            }

          }
          physical.ProduceResult(newColumns, src)
      }

      rewriter
    }

    TopDownWithState(rewriteCreator, stateChange)
  }

}
