package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.NoGraph
import org.parboiled.scala.{Parser, Rule1}

trait Graphs extends Parser
  with Expressions {

  def GraphSpecifier: Rule1[ast.GraphSpecifier] = rule("-|GraphDef") {
    NoGraph | GraphDef
  }

  def NoGraph: Rule1[ast.NoGraph] = "-" ~~ push(ast.NoGraph()(_))

  def GraphDef: Rule1[ast.GraphDef] = rule("GraphDef") {
   NewGraph | CopyGraph | LoadGraph | AliasGraph
  }

  private def AliasGraph: Rule1[ast.AliasGraph] = rule("Graph AS") {
    GraphRef ~~ optional(keyword("AS") ~~ Variable) ~~>> (ast.AliasGraph(_, _))
  }

  private def NewGraph: Rule1[ast.NewGraph] = rule("NEW GRAPH") {
    keyword("NEW") ~~ keyword("GRAPH") ~~ (AnonNewGraph | NamedNewGraph)
  }

  private def AnonNewGraph: Rule1[ast.NewGraph] = rule("Anon NEW GRAPH") {
    keyword("AT") ~~ GraphUrl ~~>> (url => ast.NewGraph(None, Some(url)))
  }
  private def NamedNewGraph: Rule1[ast.NewGraph] = rule("Named NEW GRAPH") {
    optional(Variable) ~~ optional(keyword("AT") ~~ GraphUrl) ~~>> (ast.NewGraph(_, _))
  }

  private def CopyGraph: Rule1[ast.CopyGraph] = rule("COPY GRAPH") {
    keyword("COPY") ~~ GraphRef ~~ keyword("TO") ~~ GraphUrl ~~ optional(keyword("AS") ~~ Variable) ~~>> (ast.CopyGraph(_, _, _))
  }

  private def LoadGraph: Rule1[ast.LoadGraph] = rule("GRAPH AT") {
    keyword("GRAPH") ~~ (AnonLoadGraph | NamedLoadGraph)
  }

  private def AnonLoadGraph: Rule1[ast.LoadGraph] = rule("Anon GRAPH AT") {
    keyword("AT") ~~ GraphUrl ~~>> (url => ast.LoadGraph(None, url))
  }
  private def NamedLoadGraph: Rule1[ast.LoadGraph] = rule("Named GRAPH AT") {
    optional(Variable) ~~ keyword("AT") ~~ GraphUrl ~~>> (ast.LoadGraph(_, _))
  }

  def GraphRef: Rule1[ast.GraphRef] = rule("GraphRef") {
    SourceGraph | TargetGraph | DefaultGraph | NamedGraph
  }

  private def NamedGraph: Rule1[ast.NamedGraph] = rule("GRAPH") {
    Variable ~~>> (ast.NamedGraph(_))
  }

  private def SourceGraph: Rule1[ast.SourceGraph] = rule("SOURCE GRAPH") {
    keyword("SOURCE GRAPH").~~~>[ast.SourceGraph](ast.SourceGraph()(_))
  }

  private def TargetGraph: Rule1[ast.TargetGraph] = rule("TARGET GRAPH") {
    keyword("TARGET GRAPH").~~~>[ast.TargetGraph](ast.TargetGraph()(_))
  }

  private def DefaultGraph: Rule1[ast.DefaultGraph] = rule("DEFAULT GRAPH") {
    keyword("DEFAULT GRAPH").~~~>[ast.DefaultGraph](ast.DefaultGraph()(_))
  }

  def GraphUrl: Rule1[ast.GraphUrl] = StringLiteral ~~>> (ast.GraphUrl(_))
}
