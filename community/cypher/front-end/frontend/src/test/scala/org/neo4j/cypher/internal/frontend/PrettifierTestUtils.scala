/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.OpenCypherJavaCCParserWithFallback
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.scalatest.Assertion
import org.scalatest.Matchers

trait PrettifierTestUtils extends Matchers {

  def prettifier: Prettifier

  def printComparison(a: Any, b: Option[Any]): Unit = {
    val width = 60
    val height = 1000
    b match {
      case Some(bb) => printComparison(a, bb, width, height)
      case None     => pprint.pprintln(a, width = width, height = height)
    }
  }

  def printComparison(a: Any, b: Any, width: Int, height: Int): Unit = {
    val as = pprint.apply(a, width = width, height = height).render.linesIterator
    val bs = pprint.apply(b, width = width, height = height).render.linesIterator
    for {
      (l, r) <- as.zipAll(bs, "", "")
      printedWidth = fansi.Str.ansiRegex.matcher(l).replaceAll("").length
      lp = l + " " * (width - printedWidth)
      sep = if (l == r) "|" else "X"
      line = lp + sep + r
    } println(line)
  }

  def roundTripCheck(original: Statement): Assertion = {
    val pretty = prettifier.asString(original)
    val parsed = try {
      parse(pretty)
    } catch {
      case e: Exception =>
        println("-- failure --------------------------------------")
        println(pretty)
        printComparison(original, None)
        throw e
    }
    val clean = dropQuotedSyntax(parsed)
    try {
      original shouldEqual clean
    } catch {
      case e: Exception =>
        println("-- failure --------------------------------------")
        println(pretty)
        printComparison(original, Some(clean))
        throw e
    }
  }

  def dropQuotedSyntax[T <: ASTNode](n: T): T =
    n.endoRewrite(bottomUp(Rewriter.lift({
      case i @ UnaliasedReturnItem(e, _) => UnaliasedReturnItem(e, "")(i.position)
    })))

  def show(original: String): Unit = {
    println("original: " + original)
    val parsed1 = parse(original)
    println("  - ast1: " + parsed1)
    val pretty = prettifier.asString(parsed1)
    println("  - pret: " + pretty)
    val parsed2 = parse(pretty)
    println("  - ast2: " + parsed2)
  }

  private def parse(original: String): Statement =
    OpenCypherJavaCCParserWithFallback.parse(original, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator)

}
