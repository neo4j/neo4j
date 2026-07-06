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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.scalactic.Equality

import scala.language.implicitConversions

/**
 * Normalize `\r\n` to `\n` on both sides so that Windows-built error messages
 * compare equal to the expected values.
 *
 * Can't just use the WindowsStringSafe as it's nested.
 */
object WindowsSemanticErrorDefSeqStringSafe extends Equality[Seq[SemanticErrorDef]] {

  override def areEqual(a: Seq[SemanticErrorDef], b: Any): Boolean = b match {
    case bSeq: Seq[_] if a.size == bSeq.size =>
      a.zip(bSeq).forall {
        case (actual, expected: SemanticErrorDef) =>
          def normalizeMsg(error: SemanticErrorDef): String =
            error.msg.replaceAll("\r\n", "\n")

          normalizeMsg(actual) == normalizeMsg(expected) &&
          actual.position == expected.position &&
          actual.gqlStatusObject == expected.gqlStatusObject
        case _ => false
      }
    case _ => false
  }
}
