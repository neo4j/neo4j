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
package org.neo4j.cypher.internal.cst.factory.neo4j

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.neo4j.cypher.internal.cst.factory.neo4j.SyntaxErrorListener.SyntaxError

class SyntaxErrorListener extends BaseErrorListener {
  private[this] var _syntaxErrors = Seq.empty[SyntaxError]

  def syntaxErrors: Seq[SyntaxError] = _syntaxErrors

  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException
  ): Unit = {
    _syntaxErrors = _syntaxErrors.appended(SyntaxError(offendingSymbol, line, charPositionInLine, msg, e))
  }

  def reset(): Unit = _syntaxErrors = Seq.empty
}

object SyntaxErrorListener {

  case class SyntaxError(
    offendingSymbol: Any,
    line: Int,
    charPositionInLine: Int,
    message: String,
    e: RecognitionException
  ) extends Exception(message)
}
