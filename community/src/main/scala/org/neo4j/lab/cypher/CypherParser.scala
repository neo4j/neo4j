package org.neo4j.lab.cypher

import commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction

class CypherParser extends JavaTokenParsers {

  def query: Parser[Query] = from ~ opt(where) ~ select ^^ {
    case from ~ where ~ select => Query(select, from, where)
  }

  def from: Parser[From] = "from" ~> repsep(nodeByIds, ",") ^^ (From(_: _*))

  def relatedToWithoutRelOut = "(" ~ ident ~ ")" ~ opt("<") ~ "-[" ~ "'" ~ ident ~ "'" ~ "]-" ~ opt(">") ~ "(" ~ ident ~ ")" ^^ {
    case "(" ~ start ~ ")" ~ back ~ "-[" ~ "'" ~ relType ~ "'" ~ "]-" ~ forward ~ "(" ~ end ~ ")" => RelatedTo(start, end, "r", relType,  getDirection(back, forward))}

  def relatedToWithRelOut = "(" ~ ident ~ ")" ~ opt("<") ~ "-[" ~ ident ~ "," ~ "'" ~ ident ~ "'" ~ "]-" ~ opt(">") ~ "(" ~ ident ~ ")" ^^ {
    case "(" ~ start ~ ")" ~ back ~ "-[" ~ relName ~ "," ~ "'" ~ relType ~ "'" ~ "]-" ~ forward ~ "(" ~ end ~ ")" => RelatedTo(start, end, relName, relType, getDirection(back, forward))
  }

  def nodeByIds = ident ~ "=" ~ "node" ~ "(" ~ repsep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "node" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def select: Parser[Select] = "select" ~> repsep((propertyOutput|nodeOutput), ",") ^^ ( Select(_:_*) )

  def nodeOutput:Parser[SelectItem] = ident ^^ { EntityOutput(_) }
  def propertyOutput:Parser[SelectItem] = ident ~ "." ~ ident ^^ { case c ~ "." ~ p => PropertyOutput(c,p) }

  def where: Parser[Where] = "where" ~> rep(clause) ^^ (Where(_: _*))

  def clause: Parser[Clause] = (predicate | parens | relatedToWithoutRelOut | relatedToWithRelOut) * (
    "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
    "or" ^^^ {  (a: Clause, b: Clause) => Or(a, b) })

  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def predicate = (
    ident ~ "." ~ ident ~ "=" ~ stringLiteral ^^ {
      case c ~ "." ~ p ~ "=" ~ v => StringEquals(c, p, stripQuotes(v))
    })

  private def stripQuotes(s: String) = s.substring(1, s.length - 1)

  private def getDirection(back:Option[String], forward:Option[String]):Direction =
    (back.nonEmpty, forward.nonEmpty) match {
      case (true,false) => Direction.INCOMING
      case (false,true) => Direction.OUTGOING
      case (false,false) => Direction.BOTH
      case (true,true) => throw new RuntimeException("A relation can't point both ways")
    }

  def parse(sql: String): Option[Query] =
    parseAll(query, sql) match {
      case Success(r, q) => Option(r)
      case x => println(x); None
    }
}