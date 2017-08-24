package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, ast}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.parboiled.scala.{Parser, ReductionRule1, Rule1, Rule2}

trait Graphs
  extends Parser
  with Expressions {

  def GraphUrl: Rule1[ast.GraphUrl] = rule("<graph-url>") {
    ((Parameter ~~> (Left(_))) | (StringLiteral ~~> (Right(_)))) ~~>> (ast.GraphUrl(_))
  }

  def GraphRef: Rule1[ast.GraphRef] =
    Variable ~~>> (ast.GraphRef(_))

  def GraphRefList: Rule1[List[ast.GraphRef]] =
    oneOrMore(GraphRef, separator = CommaSep)

  private def GraphAlias: Rule1[ast.Variable] =
    keyword("AS") ~~ Variable

  private def GraphRefAlias: Rule1[ast.GraphRefAlias] = rule("<graph-ref> AS <name>") {
    GraphRef ~~ optional(GraphAlias) ~~>> (ast.GraphRefAlias(_, _))
  }

  private def GraphRefAliasList: Rule1[List[ast.GraphRefAlias]] =
    oneOrMore(GraphRefAlias, separator = CommaSep)

  private def GraphRefAliasItem: Rule1[ast.GraphRefAliasItem] = rule("GRAPH <graph-ref> [AS <name>]") {
    keyword("GRAPH") ~~ GraphRefAlias ~~>> (ast.GraphRefAliasItem(_))
  }

  private def GraphOfItem: Rule1[ast.GraphOfItem] = rule("GRAPH OF <pattern> [AS <name>]") {
    keyword("GRAPH") ~~ keyword("OF") ~~ Pattern ~~ optional(GraphAlias) ~~>> (ast.GraphOfItem(_, _))
  }

  private def GraphAtItem: Rule1[ast.GraphAtItem] = rule("GRAPH <graph-url> AT [AS <name>]") {
    keyword("GRAPH") ~~ keyword("AT") ~~ GraphUrl ~~ optional(GraphAlias) ~~>>(ast.GraphAtItem(_, _))
  }

  private def SourceGraphItem: Rule1[ast.SourceGraphItem] = rule("SOURCE GRAPH [AS <name>]") {
    keyword("SOURCE") ~~ keyword("GRAPH") ~~ optional(GraphAlias) ~~>> (ast.SourceGraphItem(_))
  }

  private def TargetGraphItem: Rule1[ast.TargetGraphItem] = rule("TARGET GRAPH [AS <name>]") {
    keyword("TARGET") ~~ keyword("GRAPH") ~~ optional(GraphAlias) ~~>> (ast.TargetGraphItem(_))
  }

  private def GraphOfShorthand: Rule1[ast.SingleGraphItem] =
    keyword("GRAPH") ~~ GraphAlias ~~ keyword("OF") ~~ Pattern ~~>> { (as: ast.Variable, of: ast.Pattern) => ast.GraphOfItem(of, Some(as)) }

  private def GraphAtShorthand: Rule1[ast.SingleGraphItem] =
    keyword("GRAPH") ~~ GraphAlias ~~ keyword("AT") ~~ GraphUrl ~~>> { (as: ast.Variable, url: ast.GraphUrl) => ast.GraphAtItem(url, Some(as)) }

  private def GraphAliasFirstItem = GraphOfShorthand | GraphAtShorthand

  def SingleGraphItem: Rule1[ast.SingleGraphItem] =
    SourceGraphItem | TargetGraphItem | GraphAtItem | GraphOfItem | GraphAliasFirstItem | GraphRefAliasItem

  private def SingleGraphItemList: Rule1[List[ast.SingleGraphItem]] =
    oneOrMore(SingleGraphItem, separator = CommaSep)

  private def ShortGraphItem: Rule1[ast.SingleGraphItem] =
    (GraphRefAlias ~~>> (ast.GraphRefAliasItem(_)))

  private def ShortGraphItemList: Rule1[List[ast.SingleGraphItem]] =
    keyword("GRAPHS") ~~ oneOrMore(!SingleGraphItem ~~ ShortGraphItem)

//  private def GraphItemList: Rule1[List[ast.SingleGraphItem]] =
//    oneOrMore(ShortGraphItemList | SingleGraphItemList, separator = CommaSep) ~~>(_.flatten)
//
//  private def PickedGraphItems: Rule1[Option[ast.SingleGraphItem]] =
//    keyword(">>") ~~ optional(SingleGraphItem | ShortGraphItem)
//
//  private def ManyGraphItems =
//    GraphItemList ~~ optional(PickedGraphItems ~~ optional(CommaSep ~~ GraphItemList)) ~~>> {
//      (left: List[SingleGraphItem], picked: Option[(Option[ast.SingleGraphItem], Option[List[ast.SingleGraphItem]])]) =>
//        (pos: InputPosition) =>
//          picked match {
//            // left >>, right
//            case Some((None, right)) =>
//              ast.GraphReturnItems(false, left, PickLeft, right.getOrElse(List.empty))(pos)
//
//            // left >> target, right
//            case Some((Some(target), right)) =>
//              ast.GraphReturnItems(false, left, PickBoth, target :: right.getOrElse(List.empty))(pos)
//
//            // left
//            case None =>
//              ast.GraphReturnItems(false, left, PickNone, List.empty)(pos)
//          }
//    }


      // ~~ optional(CommaSep ~~ GraphItemList)))

//    GraphItemList ~~ optional(PickedGraphItems) ~~>> {
//      (left: List[ast.SingleGraphItem], optRight: Option[(SingleGraphItem, Option[List[SingleGraphItem]])]) =>
//        ???
//    }

//    GraphItemList ~~ (optional(keyword(">>") ~~ push(true)) ~~> (_.getOrElse(false))) ~~ optional(GraphItemList) ~~>
//      withContext {
//        (star: Boolean, left: List[ast.SingleGraphItem], pick: Boolean, right: List[ast.SingleGraphItem], ctx) =>
//          ast.GraphReturnItems(star, left, pick, right)(ContextPosition(ctx))
//      }
//
//  private def OptionallyStarredGraphsReturnItems: Rule1[ast.GraphReturnItems] = rule("WITH|RETURN GRAPHS ...") {
//    keyword("GRAPHS") ~~ (
//      (keyword("*") ~~ optional(CommaSep ~~ push(true) ~~ ManyGraphReturnItems) ~~>> {
//        (items: Option[ast.GraphReturnItems]) =>
//          (pos) => items.getOrElse(ast.GraphReturnItems(true, List.empty, false, List.empty)(pos))
//      }
//      )
//      |
//      (push(false) ~~ ManyGraphReturnItems)
//    )
//  }

  def GraphReturnItems: Rule1[ast.GraphReturnItems] =
    ???
}
