package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.v4_0.parser.CypherParser
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, Rewriter, bottomUp}
import org.scalatest.{Assertion, Matchers}

trait PrettifierTestUtils extends Matchers {

  def pr: Prettifier

  def parser: CypherParser

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


  def roundTripCheck(original: String): Assertion = {
    val parsed1 = parser.parse(original)
    val pretty = pr.asString(parsed1)
    val parsed2 = parser.parse(pretty)
    try {
      parsed1 shouldEqual parsed2
    } catch {
      case e: Exception =>
        println(original)
        println(pretty)
        printComparison(parsed1, Some(parsed2))
        throw e
    }
  }

  def roundTripCheck(original: Statement): Assertion = {
    val pretty = pr.asString(original)
    val parsed = try {
      parser.parse(pretty)
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
    val parsed1 = parser.parse(original)
    println("  - ast1: " + parsed1)
    val pretty = pr.asString(parsed1)
    println("  - pret: " + pretty)
    val parsed2 = parser.parse(pretty)
    println("  - ast2: " + parsed2)
  }

}
