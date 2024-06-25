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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.scalatest.matchers.should.Matchers

trait PrettifierTestUtils extends Matchers {
  private val defaultWidth: Int = 60
  private val defaultHeight: Int = 1000
  private val defaultIndent: Int = 4
  private val defaultPadding: Int = 1

  def prettifier: Prettifier

  def printStringComparison(a: Any, b: Any, width: Int = defaultWidth): Unit = {
    val escapeAndIndent = (x: Any) => {
      val escapedString = x.toString.linesIterator.map(s => literalize(s))
      escapedString.flatMap(splitAndIndent(_, width, indentation = defaultIndent))
    }

    val as = escapeAndIndent(a)
    val bs = escapeAndIndent(b)

    printComparison(as, bs)
  }

  def printAstComparison(a: Any, b: Option[Any], width: Int = defaultWidth, height: Int = defaultHeight): Unit = {
    b match {
      case Some(bb) =>
        val as = pprint.apply(a, width, height).render.linesIterator
        val bs = pprint.apply(bb, width, height).render.linesIterator
        printComparison(as, bs)
      case None => pprint.pprintln(a, width, height)
    }
  }

  private def printComparison(as: Iterator[String], bs: Iterator[String], width: Int = defaultWidth): Unit = {
    for {
      (l, r) <- as.zipAll(bs, "", "")
      printedWidth = fansi.Str.ansiRegex.matcher(l).replaceAll("").length
      lp = l + " " * (width - printedWidth)
      sep = if (l == r) "|" else "X"
      line = lp + sep + r
    } println(line)
  }

  private def splitAndIndent(s: String, width: Int, indentation: Int): Seq[String] = {
    val actualWidth = width - defaultPadding
    val (firstLine, rest) = s.splitAt(actualWidth)
    val restLines = rest.grouped(actualWidth - indentation).map((" " * indentation) + _)
    firstLine +: restLines.toSeq
  }

  private def literalize(s: IndexedSeq[Char]): String = {
    val sb = new StringBuilder
    var i = 0
    val len = s.length
    while (i < len) {
      val c = s(i)
      if (c < ' ' || (c > '~')) sb.append("\\u%04x" format c.toInt)
      else sb.append(c)
      i += 1
    }

    sb.result()
  }

  def roundTripCheck(original: Statement): Unit = {
    val pretty = prettifier.asString(original)
    val statements =
      try {
        parse(pretty)
      } catch {
        case e: Exception =>
          printSeparator("failed query")
          println(pretty)
          printAstComparison(original, None)
          throw e
      }
    statements.foreach { statement =>
      val clean = dropQuotedSyntax(statement)
      val prettifiedClean = prettifier.asString(clean)
      try {
        pretty should equal(prettifiedClean)
      } catch {
        case e: Exception =>
          printSeparator("failed query")
          println(pretty)
          printSeparator("string diff")
          printStringComparison(pretty, prettifiedClean)
          printSeparator("AST diff")
          printAstComparison(original, Some(clean))
          throw e
      }
    }
  }

  def printSeparator(word: String, width: Int = defaultWidth): Unit = {
    val separatorLine = "#" * (width * 2)
    println()
    println(separatorLine)
    println(s"## $word")
    println(separatorLine)
  }

  def dropQuotedSyntax[T <: ASTNode](n: T): T =
    n.endoRewrite(bottomUp(Rewriter.lift({
      case i @ UnaliasedReturnItem(e, _) => UnaliasedReturnItem(e, "")(i.position)
    })))

  private def parse(original: String): Seq[Statement] = {
    val javaCcStatement = JavaCCParser.parse(original, OpenCypherExceptionFactory(None))
    val statements = CypherVersion.All.toSeq
      .map(v => AstParserFactory(v)(original, OpenCypherExceptionFactory(None), None).singleStatement())
    statements :+ javaCcStatement
  }
}
