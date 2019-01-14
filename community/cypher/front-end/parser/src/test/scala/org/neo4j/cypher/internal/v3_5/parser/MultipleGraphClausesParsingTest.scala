/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.parser

import org.neo4j.cypher.internal.v3_5.ast.{AstConstructionTestSupport, Clause}
import org.neo4j.cypher.internal.v3_5.expressions.{Property, PropertyKeyName, RelationshipChain, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_5.util.DummyPosition
import org.neo4j.cypher.internal.v3_5.util.symbols.{CTAny, CTMap}
import org.neo4j.cypher.internal.v3_5.{ast, expressions => exp}
import org.parboiled.scala._

import scala.language.implicitConversions

class MultipleGraphClausesParsingTest
  extends ParserAstTest[ast.Clause]
  with Query
  with Expressions
  with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  val fooBarGraph = ast.CatalogName(List("foo", "bar"))
  val fooDiffGraph = ast.CatalogName(List("foo", "diff"))

  test("CONSTRUCT CREATE ()") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    yields(ast.ConstructGraph(news = List(ast.CreateInConstruct(exp.Pattern(patternParts)(pos))(pos))))
  }

  test("CONSTRUCT CLONE a") {
    val item = ast.UnaliasedReturnItem(varFor("a"), "a")(pos)
    val clone = ast.Clone(List(item))(pos)
    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a, b") {
    val item1 = ast.UnaliasedReturnItem(varFor("a"), "a")(pos)
    val item2 = ast.UnaliasedReturnItem(varFor("b"), "b")(pos)
    val clone = ast.Clone(List(item1, item2))(pos)
    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a AS x, b") {
    val item1 = ast.AliasedReturnItem(varFor("a"), varFor("x"))(pos)
    val item2 = ast.UnaliasedReturnItem(varFor("b"), "b")(pos)
    val clone = ast.Clone(List(item1, item2))(pos)
    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a CLONE b AS b") {
    val aClone: ast.Clone = ast.Clone(List(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos)
    val bClone: ast.Clone = ast.Clone(List(ast.AliasedReturnItem(varFor("b"), varFor("b"))(pos)))(pos)
    yields(ast.ConstructGraph(clones = List(aClone, bClone)))
  }

  test("CONSTRUCT CLONE x CREATE ({prop: 1})") {
    val clone: ast.Clone = ast.Clone(List(ast.UnaliasedReturnItem(varFor("x"), "x")(pos)))(pos)

    val properties = literalIntMap("prop" -> 1)
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, List(), Some(properties))(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(
      clones = List(clone),
      news = List(newClause))
    )
  }

  test("CONSTRUCT CREATE (a) SET a.prop = 1") {
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(Some(exp.Variable("a")(pos)), List(), None)(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    val set = ast.SetClause(List(ast.SetPropertyItem(
      Property(exp.Variable("a")(pos),exp.PropertyKeyName("prop")(pos))(pos),SignedDecimalIntegerLiteral("1")(pos)
    )(pos)))(pos)

    yields(ast.ConstructGraph(
      news = List(newClause),
      sets = List(set)
    ))
  }

  test("CONSTRUCT CREATE (a) SET a.prop = 1 SET a:Foo") {
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(Some(exp.Variable("a")(pos)), List(), None)(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    val set1 = ast.SetClause(List(ast.SetPropertyItem(
      Property(exp.Variable("a")(pos),exp.PropertyKeyName("prop")(pos))(pos),SignedDecimalIntegerLiteral("1")(pos)
    )(pos)))(pos)
    val set2 = ast.SetClause(List(ast.SetLabelItem(
      exp.Variable("a")(pos), Seq(exp.LabelName("Foo")(pos))
    )(pos)))(pos)

    yields(ast.ConstructGraph(
      news = List(newClause),
      sets = List(set1, set2)
    ))
  }

  test("CONSTRUCT CREATE (:A)") {
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, Seq(exp.LabelName("A")(pos)), None)(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE (b COPY OF a:A)") {
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(Some(varFor("b")), Seq(exp.LabelName("A")(pos)), None, Some(varFor("a")))(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE (COPY OF a:A)") {
    val pattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, Seq(exp.LabelName("A")(pos)), None, Some(varFor("a")))(pos))))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE ()-[r2 COPY OF r:REL]->()") {
    val relChain = RelationshipChain(
      exp.NodePattern(None, List.empty, None, None)(pos),
      exp.RelationshipPattern(Some(varFor("r2")), Seq(exp.RelTypeName("REL")(pos)), None, None, exp.SemanticDirection.OUTGOING, false, Some(varFor("r")))(pos),
      exp.NodePattern(None, List.empty, None, None)(pos)
    )(pos)
    val pattern = exp.Pattern(List(exp.EveryPath(relChain)))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE ()-[COPY OF r:REL]->()") {
    val relChain = RelationshipChain(
      exp.NodePattern(None, List.empty, None, None)(pos),
      exp.RelationshipPattern(None, Seq(exp.RelTypeName("REL")(pos)), None, None, exp.SemanticDirection.OUTGOING, false, Some(varFor("r")))(pos),
      exp.NodePattern(None, List.empty, None, None)(pos)
    )(pos)
    val pattern = exp.Pattern(List(exp.EveryPath(relChain)))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CLONE x AS y CREATE (a:A) CREATE (a)-[:T]->(y)") {
    val clone: ast.Clone = ast.Clone(List(ast.AliasedReturnItem(varFor("x"), varFor("y"))(pos)))(pos)

    val pattern1 = exp.Pattern(List(exp.EveryPath(exp.NodePattern(Some(varFor("a")), Seq(exp.LabelName("A")(pos)), None)(pos))))(pos)
    val new1: ast.CreateInConstruct = ast.CreateInConstruct(pattern1)(pos)

    val pattern2 = exp.Pattern(List(exp.EveryPath(exp.RelationshipChain(
      exp.NodePattern(Some(varFor("a")), Seq.empty, None)(pos),
      exp.RelationshipPattern(None, Seq(exp.RelTypeName("T")(pos)), None, None, exp.SemanticDirection.OUTGOING)(pos),
      exp.NodePattern(Some(varFor("y")), Seq.empty, None)(pos))(pos)
    )))(pos)
    val new2: ast.CreateInConstruct = ast.CreateInConstruct(pattern2)(pos)

    yields(ast.ConstructGraph(clones = List(clone), news = List(new1, new2)))
  }

  test("CONSTRUCT") {
    yields(ast.ConstructGraph())
  }

  test("CONSTRUCT ON foo.bar") {
    yields(ast.ConstructGraph(on = List(ast.CatalogName("foo", List("bar")))))
  }

  test("CONSTRUCT ON foo.bar, baz.boz") {
    yields(ast.ConstructGraph(on = List(
      ast.CatalogName("foo", List("bar")),
      ast.CatalogName("baz", List("boz"))
    )))
  }

  test("CONSTRUCT SET a:A CREATE (b)") {
    failsToParse
  }

  test("RETURN GRAPH") {
    yields(ast.ReturnGraph(None))
  }

  // TODO: Parsing ambiguity; is it a graph name 'union' or no graph name and a UNION clause?
  ignore("RETURN GRAPH union") {
    yields(ast.ReturnGraph(Some(ast.CatalogName("union"))))
  }

  // TODO: Causes parser to fail with its unhelpful error message
  ignore("FROM graph") {
    yields(ast.GraphLookup(ast.CatalogName(List("graph"))))
  }

  test("FROM GRAPH foo.bar") {
    yields(ast.GraphLookup(fooBarGraph))
  }

  test("FROM GRAPH foo()") {
    yields(ast.ViewInvocation(ast.CatalogName("foo"), Seq.empty))
  }

  test("FROM GRAPH foo   (    )") {
    yields(ast.ViewInvocation(ast.CatalogName("foo"), Seq.empty))
  }

  test("FROM GRAPH foo.bar(baz(grok))") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos))))
  }

  test("FROM GRAPH foo. bar   (baz  (grok   )  )") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos))))
  }

  test("FROM GRAPH foo.bar(baz(grok), another.name)") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos), ast.GraphLookup(ast.CatalogName("another", "name"))(pos))))
  }

  test("FROM foo.bar(baz(grok), another.name)") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos), ast.GraphLookup(ast.CatalogName("another", "name"))(pos))))
  }

  test("FROM GRAPH graph") {
    yields(ast.GraphLookup(ast.CatalogName(List("graph"))))
  }

  test("FROM `graph`") {
    yields(ast.GraphLookup(ast.CatalogName(List("graph"))))
  }

  test("FROM GRAPH `foo.bar.baz.baz`"){
    yields(ast.GraphLookup(ast.CatalogName(List("foo.bar.baz.baz"))))
  }

  test("FROM graph1"){
    yields(ast.GraphLookup(ast.CatalogName(List("graph1"))))
  }

  test("FROM `foo.bar.baz.baz`"){
    yields(ast.GraphLookup(ast.CatalogName(List("foo.bar.baz.baz"))))
  }

  test("FROM GRAPH `foo.bar`.baz"){
    yields(ast.GraphLookup(ast.CatalogName(List("foo.bar", "baz"))))
  }

  test("FROM GRAPH foo.`bar.baz`"){
    yields(ast.GraphLookup(ast.CatalogName(List("foo", "bar.baz"))))
  }

  test("FROM GRAPH `foo.bar`.`baz.baz`"){
    yields(ast.GraphLookup(ast.CatalogName(List("foo.bar", "baz.baz"))))
  }

  test("CONSTRUCT ON `foo.bar.baz.baz`"){
    yields(ast.ConstructGraph(List.empty, List.empty, List(ast.CatalogName(List("foo.bar.baz.baz")))))
  }

  test("CONSTRUCT ON `foo.bar`.baz"){
    yields(ast.ConstructGraph(List.empty, List.empty, List(ast.CatalogName(List("foo.bar", "baz")))))
  }

  test("CONSTRUCT ON foo.`bar.baz`"){
    yields(ast.ConstructGraph(List.empty, List.empty, List(ast.CatalogName(List("foo", "bar.baz")))))
  }

  test("CONSTRUCT ON `foo.bar`.`baz.baz`"){
    yields(ast.ConstructGraph(List.empty, List.empty, List(ast.CatalogName(List("foo.bar", "baz.baz")))))
  }


  private val nodePattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, List(), None)(pos))))(pos)

  private val complexPattern = exp.Pattern(List(
    exp.NamedPatternPart(varFor("p"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos),
    exp.NamedPatternPart(varFor("q"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos)
  ))(pos)
}
