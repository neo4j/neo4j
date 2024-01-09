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

import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.neo4j.cypher.internal.parser.CypherLexer
import org.neo4j.cypher.internal.parser.CypherParser

trait AntlrRule[+T <: ParserRuleContext] {
  def apply(queryText: String): Cst[T]
}

object AntlrRule {

  type Parser = CypherParser

  def fromParser[T <: ParserRuleContext](runParser: Parser => T, checkAllTokensConsumed: Boolean = true): AntlrRule[T] =
    fromQueryAndParser(identity, runParser, checkAllTokensConsumed)

  private def areAllTokensConsumed[T <: ParserRuleContext](
    parserResult: T,
    tokenStream: CommonTokenStream
  ): Option[Exception] = {
    tokenStream.fill()
    var index = tokenStream.size() - 1
    var lastToken = tokenStream.get(index)
    var result: Option[Exception] = Option.empty

    if (parserResult.stop.getType != Token.EOF) {
      while (index >= 0 && (lastToken.getType == Token.EOF || lastToken.getChannel != Token.DEFAULT_CHANNEL)) {
        index -= 1
        lastToken = tokenStream.get(index)
      }

      if (parserResult.stop != lastToken) {
        val token = parserResult.stop
        val tokenText = token.getText
        val tokenLine = token.getLine
        val tokenColumn = token.getCharPositionInLine

        result = Some(new Exception(
          s"did not read all input, it stopped at token $tokenText at line $tokenLine, column $tokenColumn"
        ))
      }
    }

    result
  }

  def fromQueryAndParser[T <: ParserRuleContext](
    transformQuery: String => String,
    runParser: Parser => T,
    checkAllTokensConsumed: Boolean
  ): AntlrRule[T] =
    (queryText: String) => {
      val transformedQuery = transformQuery(queryText)
      val inputStream = CharStreams.fromStream(new CypherInputStream(transformedQuery))
      val lexer = new CypherLexer(inputStream)
      val tokenStream = new CommonTokenStream(lexer)
      val parser = new CypherParser(tokenStream)
      val syntaxChecker = new SyntaxChecker()
      val parserErrorListener: SyntaxErrorListener = new SyntaxErrorListener()

      // This avoids printing all ANTLR errors to console
      // https://stackoverflow.com/questions/25990158/antlr-4-avoid-error-printing-to-console
      parser.removeErrorListeners()
      parser.addParseListener(syntaxChecker)
      parser.addErrorListener(parserErrorListener)
      val parserResult = runParser(parser)

      val exhaustedInputError =
        if (checkAllTokensConsumed) areAllTokensConsumed(parserResult, tokenStream) else Option.empty
      new Cst(parserResult) {
        val parsingErrors: List[Exception] =
          (parserErrorListener.syntaxErrors ++ syntaxChecker.getErrors ++ exhaustedInputError).toList
      }
    }

  def CallClause: AntlrRule[Cst.CallClause] =
    fromParser(_.callClause())

  def MatchClause: AntlrRule[Cst.MatchClause] =
    fromParser(_.matchClause())

  def CaseExpression: AntlrRule[Cst.CaseExpression] =
    fromParser(_.caseExpression())

  def Clause: AntlrRule[Cst.Clause] =
    fromParser(_.clause())

  def Expression: AntlrRule[Cst.Expression] =
    fromParser(_.expression())

  def FunctionInvocation: AntlrRule[Cst.FunctionInvocation] =
    fromParser(_.functionInvocation())

  def ListComprehension: AntlrRule[Cst.ListComprehension] =
    fromParser(_.listComprehension())

  def MapLiteral: AntlrRule[Cst.MapLiteral] =
    fromParser(_.mapLiteral())

  def MapProjection: AntlrRule[Cst.MapProjection] =
    fromParser(_.mapProjection())

  def NodePattern: AntlrRule[Cst.NodePattern] =
    fromParser(_.nodePattern())

  def NumberLiteral: AntlrRule[Cst.NumberLiteral] =
    fromParser(_.numberLiteral())

  def Parameter: AntlrRule[Cst.Parameter] =
    fromParser(_.parameter())

  def ParenthesizedPath: AntlrRule[Cst.ParenthesizedPath] =
    fromParser(_.parenthesizedPath())

  def PatternComprehension: AntlrRule[Cst.PatternComprehension] =
    fromParser(_.patternComprehension())

  def Quantifier: AntlrRule[Cst.Quantifier] =
    fromParser(_.quantifier())

  def RelationshipPattern: AntlrRule[Cst.RelationshipPattern] =
    fromParser(_.relationshipPattern())

  def PatternPart: AntlrRule[Cst.Pattern] =
    fromParser(_.pattern())

  def Statement: AntlrRule[Cst.Statement] =
    fromParser(_.statement())

  def UseClause: AntlrRule[Cst.UseClause] =
    fromParser(_.useClause())

  // The reason for using Statements rather than Statement, is that Statements exhausts the input
  // reading until the EOF, even if it does not know how to parse all of it, whereas Statement
  // (and the rest of parser rules) will stop the moment they find a token they cannot recognize
  def Statements(checkAllTokensConsumed: Boolean = true): AntlrRule[Cst.Statement] =
    fromParser(_.statements().statement(0), checkAllTokensConsumed)

  def StringLiteral: AntlrRule[Cst.StringLiteral] =
    fromParser(_.stringLiteral())

  def SubqueryClause: AntlrRule[Cst.SubqueryClause] =
    fromParser(_.subqueryClause())

  def Variable: AntlrRule[Cst.Variable] =
    fromParser(_.variable())
}
