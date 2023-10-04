/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.rewriters.repeatWithSizeLimit.LINEAR_INCREASE_FACTOR_LIMIT
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Rewriter

import scala.annotation.tailrec

/*
This rewriter tries to limit rewriters that grow the product AST too much
 */
case class repeatWithSizeLimit(rewriter: Rewriter)(implicit val monitor: AstRewritingMonitor) extends Rewriter {

  private def astNodeSize(value: Any): Int = value.folder.treeCount {
    case _: ASTNode => ()
  }

  final def apply(that: AnyRef): AnyRef = {
    val initialSize = astNodeSize(that)
    val limit = LINEAR_INCREASE_FACTOR_LIMIT * initialSize

    innerApply(that, that, limit)
  }

  @tailrec
  private def innerApply(original: AnyRef, toRewrite: AnyRef, limit: Int): AnyRef = {
    val rewritten = rewriter.apply(toRewrite)
    if (rewritten == toRewrite) {
      toRewrite
    } else {
      val newSize = astNodeSize(rewritten)
      if (newSize > limit) {
        monitor.abortedRewriting(toRewrite)
        original
      } else {
        innerApply(original, rewritten, limit)
      }
    }
  }
}

case object repeatWithSizeLimit {
  // This prevents the rewritten expression from growing more than 10x
  val LINEAR_INCREASE_FACTOR_LIMIT = 10
}
