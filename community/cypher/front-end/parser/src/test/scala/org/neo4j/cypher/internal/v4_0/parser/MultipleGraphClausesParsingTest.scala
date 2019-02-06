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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, Clause}
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}
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
    val pattern = nodePattern(None, List(), None)
    yields(ast.ConstructGraph(news = List(ast.CreateInConstruct(pattern)(pos))))
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

    val properties = mapOfInt("prop" -> 1)
    val pattern = nodePattern(None, List(), Some(properties))
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(
      clones = List(clone),
      news = List(newClause))
    )
  }

  test("CONSTRUCT CREATE (a) SET a.prop = 1") {
    val pattern = nodePattern(Some(varFor("a")), List(), None)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    val set = ast.SetClause(List(ast.SetPropertyItem(
      prop("a", "prop"), literalInt(1)
    )(pos)))(pos)

    yields(ast.ConstructGraph(
      news = List(newClause),
      sets = List(set)
    ))
  }

  test("CONSTRUCT CREATE (a) SET a.prop = 1 SET a:Foo") {
    val pattern = nodePattern(Some(varFor("a")), List(), None)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    val set1 = ast.SetClause(List(ast.SetPropertyItem(
      prop("a", "prop"), literalInt(1)
    )(pos)))(pos)
    val set2 = ast.SetClause(List(ast.SetLabelItem(
      varFor("a"), Seq(labelName("Foo"))
    )(pos)))(pos)

    yields(ast.ConstructGraph(
      news = List(newClause),
      sets = List(set1, set2)
    ))
  }

  test("CONSTRUCT CREATE (:A)") {
    val pattern = nodePattern(None, Seq(labelName("A")), None)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE (b COPY OF a:A)") {
    val pattern = nodePattern(Some(varFor("b")), Seq(labelName("A")), None, Some(varFor("a")))
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE (COPY OF a:A)") {
    val pattern = nodePattern(None, Seq(labelName("A")), None, Some(varFor("a")))
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE ()-[r2 COPY OF r:REL]->()") {
    val relChain = exp.RelationshipChain(
      exp.NodePattern(None, List.empty, None, None)(pos),
      exp.RelationshipPattern(Some(varFor("r2")), Seq(exp.RelTypeName("REL")(pos)), None, None,
        exp.SemanticDirection.OUTGOING, legacyTypeSeparator = false, Some(varFor("r")))(pos),
      exp.NodePattern(None, List.empty, None, None)(pos)
    )(pos)
    val pattern = exp.Pattern(List(exp.EveryPath(relChain)))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CREATE ()-[COPY OF r:REL]->()") {
    val relChain = exp.RelationshipChain(
      exp.NodePattern(None, List.empty, None, None)(pos),
      exp.RelationshipPattern(None, Seq(exp.RelTypeName("REL")(pos)), None, None,
        exp.SemanticDirection.OUTGOING, legacyTypeSeparator = false, Some(varFor("r")))(pos),
      exp.NodePattern(None, List.empty, None, None)(pos)
    )(pos)
    val pattern = exp.Pattern(List(exp.EveryPath(relChain)))(pos)
    val newClause: ast.CreateInConstruct = ast.CreateInConstruct(pattern)(pos)

    yields(ast.ConstructGraph(news = List(newClause)))
  }

  test("CONSTRUCT CLONE x AS y CREATE (a:A) CREATE (a)-[:T]->(y)") {
    val clone: ast.Clone = ast.Clone(List(ast.AliasedReturnItem(varFor("x"), varFor("y"))(pos)))(pos)

    val pattern1 = nodePattern(Some(varFor("a")), Seq(labelName("A")), None)
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
    yields(ast.ViewInvocation(fooBarGraph,
      Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos))))
  }

  test("FROM GRAPH foo. bar   (baz  (grok   )  )") {
    yields(ast.ViewInvocation(fooBarGraph,
      Seq(ast.ViewInvocation(ast.CatalogName("baz"), Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos))))
  }

  test("FROM GRAPH foo.bar(baz(grok), another.name)") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"),
        Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos), ast.GraphLookup(ast.CatalogName("another", "name"))(pos))))
  }

  test("FROM foo.bar(baz(grok), another.name)") {
    yields(ast.ViewInvocation(fooBarGraph, Seq(ast.ViewInvocation(ast.CatalogName("baz"),
      Seq(ast.GraphLookup(ast.CatalogName("grok"))(pos)))(pos), ast.GraphLookup(ast.CatalogName("another", "name"))(pos))))
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

  private def nodePattern(variable: Option[exp.Variable],
                          labels: Seq[exp.LabelName],
                          properties: Option[exp.Expression],
                          baseNode: Option[exp.LogicalVariable] = None) =
    exp.Pattern(List(exp.EveryPath(exp.NodePattern(variable, labels, properties, baseNode)(pos))))(pos)
}
