/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, TimeoutException}
import scala.util.{Failure, Success, Try}

/*
 * Tests so that the compiled runtime behaves in the same way as the interpreted runtime for randomized Cypher
 * statements
 */
class GrammarStressIT extends ExecutionEngineFunSuite with PropertyChecks with CypherComparisonSupport {

  //Since we can create pretty tricky patterns we add a timeout
  //to keep the running time of the test down
  private val TIMEOUT_MS = 5000
  private val NESTING_DEPTH: Int = 10
  private def numberOfTestRuns = 100
  private def maxDiscardedInputs = 500
  private def maxSize = 10
  private val NODES_PER_LAYER = 10
  private val NUM_LAYERS = 3

  override implicit val generatorDrivenConfig = PropertyCheckConfig(
    minSuccessful = numberOfTestRuns, maxDiscarded = maxDiscardedInputs, maxSize = maxSize
  )

  //we don't want scala check to shrink patterns here since it will lead to invalid cypher
  //e.g. RETURN {, RETURN [, etc
  implicit val dontShrink: Shrink[String] = Shrink(s => Stream.empty)

  test("literal stress test") {
    forAll(literal) { l =>
      whenever(l.nonEmpty) {
        withClue(s"failing on literal: $l") {
          assertQuery(s"RETURN $l")
        }
      }
    }
  }

  test("match pattern") {
    forAll(patterns) { pattern =>
      val query = s"MATCH $pattern ${returnClause(pattern)}"
      withClue(s"Failed on query: $query") {
        assertQuery(query)
      }
    }
  }

  //TODO: this test currently exposes a failure for queries like
  //MATCH (n3 :L3) OPTIONAL MATCH (n2 :L2)<-[]-(n3) RETURN id(n3), id(n2)
  //the compiled runtime returns -1 instead of null
  //Fix the bug and unignore the test
  ignore("optional match pattern") {
    forAll(patterns, patterns) { (matchPattern, optionalPattern) =>
      val query = s"MATCH $matchPattern OPTIONAL MATCH $optionalPattern ${returnClause(matchPattern, optionalPattern)}"
      withClue(s"Failed on query: $query") {
        assertQuery(query)
      }
    }
  }

  test("match with predicate") {
    forAll(matchWhere) { query =>
      withClue(s"Failed on query: $query") {
        assertQuery(query)
      }
    }
  }

  //TODO: this test currently exposes a failure for queries like
  //MATCH (n3 :L3) OPTIONAL MATCH (n2 :L2)<-[]-(n3) RETURN id(n3), id(n2)
  //the compiled runtime returns -1 instead of null
  //Fix the bug and unignore the test
  ignore("optional match with predicate") {
    forAll(optionalMatchWhere) { query =>
      withClue(s"Failed on query: $query") {
        assertQuery(query)
      }
    }
  }

  test("Double var expand") {
    assertQuery("MATCH (:L1)-[:T1 *..1 {p1: 1}]->(n2 :L2 {p2: 7})-[r2s :T2 *..2]->(:L3) RETURN 42")
  }

  case class Identifier( name:String, isSingleEntity:Boolean)

  case class Pattern(startNode:NodePattern, tail:Option[(RelPattern, Pattern)]) {

    def identifiers: Set[Identifier] = startNode.name.map(Identifier(_, isSingleEntity = true)).toSet ++ tail.map((kv) => {
      val (r, p) = kv
      r.identifier ++ p.identifiers
    }).getOrElse(Set.empty)

    override def toString: String = {
      val tailString = tail match {
        case None => ""
        case Some((r,p)) => s"$r$p"
      }

      s"$startNode$tailString"
    }
  }
  case class NodePattern(name:Option[String], labels:Set[String], properties:Map[String, Any]) {

    override def toString: String = {

      val propString = if (properties.isEmpty) None else
        Some(properties.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))
      val labelString = if (labels.isEmpty) None else Some(labels.mkString(":", ":", ""))

      (name ++ labelString ++ propString).mkString("(", " ", ")")
    }
  }

  object RelPattern {
    def relPattern(name:Option[String],
                   relType:Set[String],
                   properties:Map[String, Any],
                   length:LengthPattern): RelPattern = {
      val idName = length match {
        case _: DefaultLength => name
        case _ => name.map(_ + "s")
      }
      new RelPattern(idName, relType, properties, length)
    }
  }

  case class RelPattern private (
                         name:Option[String],
                         relType:Set[String],
                         properties:Map[String, Any],
                         length:LengthPattern ) {

    def identifier:Option[Identifier] =
      name.map(Identifier(_, isSingleEntity))

    def isSingleEntity: Boolean =
      length match {
        case DefaultLength(_) => true
        case _ => false
      }

    override def toString: String = {
      val propString = if (properties.isEmpty) None else
        Some(properties.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))
      val relTypeString = if (relType.isEmpty) None else Some(relType.mkString(":", "|", ""))
      (name ++ relTypeString ++ length.varArgPattern ++ propString).mkString(s"${length.left}[", " ", s"]${length.right}")
    }
  }

  sealed trait LengthPattern {
    def direction: SemanticDirection
    def left: String = direction match {
      case SemanticDirection.INCOMING => "<-"
      case _ => "-"
    }
    def right: String = direction match {
      case SemanticDirection.OUTGOING => "->"
      case _ => "-"
    }
    def varArgPattern: Option[String]
  }

  case class DefaultLength(direction: SemanticDirection) extends LengthPattern {

    override def varArgPattern: Option[String] = None
  }

  case class MaxLength(direction: SemanticDirection, value: Int) extends LengthPattern {

    override def varArgPattern: Option[String] = Some(s"*..$value")
  }

  case class MinMaxLength(direction: SemanticDirection, min: Int, max: Int) extends LengthPattern {

    override def varArgPattern: Option[String] = Some(s"*$min..$max")
  }

  def patterns:Gen[Pattern] = Gen.choose(1, NUM_LAYERS)
                                  .flatMap(depth => patternGen(depth, Gen.const(Set("L"+depth))))

  def patternGen(d:Int, labelGen:Gen[Set[String]]):Gen[Pattern] =
    for {
      nodeName <- Gen.option(Gen.const("n" + d))
      labels <- labelGen
      props <- Gen.mapOf(Gen.zip(Gen.const("p" + d), Gen.choose(1, NODES_PER_LAYER)))
      tail <- tailPatternGen(d+1)
    } yield Pattern(NodePattern(nodeName, labels, props), tail)

  def tailPatternGen(d:Int):Gen[Option[(RelPattern, Pattern)]] =
    if (d <= NUM_LAYERS)
      Gen.zip(relPatternGen(d-1), patternGen(d, maybeWrongLabelGen(d))).map(Some(_))
    else
      Gen.const(None)

  def relPatternGen(d:Int):Gen[RelPattern] =
    for {
      relName <- Gen.option(Gen.const("r"+ d))
      relType <- Gen.listOf(Gen.frequency(100 -> Gen.const("T" + d), 1 -> Gen.const("Y" + d))).map(_.toSet)
      props <- Gen.mapOf(Gen.zip(Gen.const("p" + d), Gen.frequency(100 -> Gen.const(d), 1 -> Gen.const("'x'"))))
      direction <- Gen.oneOf(SemanticDirection.OUTGOING, SemanticDirection.INCOMING, SemanticDirection.BOTH)
      min <- Gen.choose(1, 2)
      max <- Gen.choose(min, min + 1)
      len: LengthPattern <- Gen.oneOf(DefaultLength(direction), MaxLength(direction, max), MinMaxLength(direction, min, max))
    } yield RelPattern.relPattern(relName, relType, props, len)

  private def maybeWrongLabelGen(d:Int) =
    Gen.listOf(
      Gen.frequency(
        100 -> Gen.const("L"+d),
        1 -> Gen.const("X"+d)
      )).map(_.toSet)

  override protected def initTest(): Unit = {
    super.initTest()
    graph.inTx {
      val nodes: Seq[IndexedSeq[Node]] =
        for (i <- 1 to NUM_LAYERS) yield {
          for (j <- 1 to NODES_PER_LAYER) yield {
            val node = createLabeledNode(Map("p" + i -> j), "L" + i)
            node
          }
        }
      for (i <- 1 until NUM_LAYERS) {
        val thisLayer = nodes(i-1)
        val nextLayer = nodes(i)
        for {
          n1 <- thisLayer
          n2 <- nextLayer
        } {
          relate(n1, n2, s"T$i", Map("p"+i -> i))
        }
      }
    }
  }

  def predicateForPatterns(patterns: Pattern*): Gen[String] = {
    val identifiers = patterns.foldLeft(Set.empty[Identifier])((a,c) => a ++ c.identifiers).toIndexedSeq
    if (identifiers.isEmpty) Gen.const("")
    else
      Gen.zip(
        Gen.oneOf(identifiers),
        Gen.choose(1, NUM_LAYERS),
        Gen.choose(1, NODES_PER_LAYER)
      ).map(t => {
        val (id, layer, n) = t
        if (id.isSingleEntity)
          s" WHERE ${id.name}.p$layer < $n"
        else
          s" WHERE all(x IN ${id.name} WHERE x.p$layer < $n)"
      })
  }

  def matchWhere:Gen[String] =
    for {
      pattern <- patterns
      predicateClause <- predicateForPatterns(pattern)
    } yield s"MATCH $pattern$predicateClause ${returnClause(pattern)}"

  def optionalMatchWhere:Gen[String] =
    for {
      pattern <- patterns
      optionalPattern <- patterns
      predicateClause <- predicateForPatterns(pattern)
      optionalPredicateClause <- predicateForPatterns(pattern, optionalPattern)
    } yield s"MATCH $pattern$predicateClause OPTIONAL MATCH $optionalPattern$optionalPredicateClause ${returnClause(pattern, optionalPattern)}"

  private def returnClause(pattern: Pattern*) = {
    val identifiers = pattern.foldLeft(Set.empty[String])( (a, c) => a ++ c.identifiers.map(toReturnValue))
    if(identifiers.nonEmpty) s"RETURN ${identifiers.mkString(", ")}" else "RETURN 42"
  }

  private def toReturnValue(id:Identifier) =
    if (id.isSingleEntity) s"id(${id.name})"
    else s"size(${id.name})"

  //Check that interpreted and compiled gives the same results, it might be the case
  //that the compiled runtime fallbacks to the interpreted but that is ok here just as
  //long as we fallback gracefully.
  private def assertQuery(query: String) = {
    runWithTimeout(TIMEOUT_MS) {
      //this is an optimization just so that we only compare results when we have to
      val runtimeUsed = graph.execute(s"EXPLAIN CYPHER runtime=compiled $query")
        .getExecutionPlanDescription.getArguments.get("runtime").asInstanceOf[String]
      if (runtimeUsed == "COMPILED") {
        // We resort to using internals of CypherComparisonSupport,
        // since with randomized patterns we cannot know at compile time, for which
        // of those we expect plans to be equal or not.
        val resultInterpreted = innerExecuteDeprecated(s"CYPHER runtime=interpreted $query", Map.empty)
        val resultCompiled = innerExecuteDeprecated(s"CYPHER runtime=compiled $query", Map.empty)
        assertResultsSameDeprecated(resultCompiled, resultInterpreted, query,
          "Diverging results between interpreted and compiled runtime")
        resultCompiled
      } else None
    }
  }

  def runWithTimeout[T](timeoutMs: Long)(f: => T) : Option[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val res = Try(Await.result(scala.concurrent.Future(f), Duration.apply(timeoutMs, "ms")))
    res match {
      case Failure(_: TimeoutException) => None
      case Failure(e) => throw e
      case Success(r) => Some(r)
      }
    }

  def literal(implicit d: Int = NESTING_DEPTH): Gen[String] =
    if (d == 0) Gen.oneOf(floatLiteral, stringLiteral, intLiteral, boolLiteral)
    else Gen.oneOf(floatLiteral, stringLiteral, intLiteral, boolLiteral, mapLiteral(d), listLiteral(d))

  def floatLiteral: Gen[String] = Arbitrary.arbitrary[Double].map(_.toString)

  def intLiteral: Gen[String] = Arbitrary.arbitrary[Long].map(_.toString)

  def boolLiteral: Gen[String] = Arbitrary.arbitrary[Boolean].map(_.toString)

  //remove non-printable characters
  def stringLiteral: Gen[String] = Arbitrary.arbitrary[String]
    .map(s => "'" + s.takeWhile(c => c != '\'' && Character.isISOControl(c)) + "'")

  def keyLiteral: Gen[String] = Gen.identifier

  def listLiteral(d: Int): Gen[String] = Gen.listOf(literal(d - 1)).map(_.mkString("[", ", ", "]"))

  def mapLiteral(d: Int): Gen[String] = Gen.mapOf(Gen.zip(keyLiteral, literal(d - 1)))
    .map(_.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))


  // Need to override so that grpah.execute will not throw an exception
  override def databaseConfig(): collection.Map[Setting[_], String] = {
    Map(GraphDatabaseSettings.cypher_hints_error -> "false")
  }
}
