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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer

object Chainer {

  /**
   * Chain together transformers.
   * Illegal sequences are not caught because of type erasure.
   * They will lead to [[ClassCastException]]s later on.
   */
  def chainTransformers(transformers: Seq[Transformer[_ <: BaseContext, _, _]]): Transformer[_ <: BaseContext, _, _] = {
    transformers.reduceLeft[Transformer[_ <: BaseContext, _, _]] {
      case (t1: Transformer[_, _, _], t2: Transformer[_, _, _]) =>
        // We need these asInstanceOf to make the compiler happy.
        // Because of type erasure, they won't actually do anything.
        // This will even work for Transformers from/to LogicalPlanState.
        t1.asInstanceOf[Transformer[BaseContext, BaseState, BaseState]] andThen t2.asInstanceOf[Transformer[
          BaseContext,
          BaseState,
          BaseState
        ]]
    }
  }
}
