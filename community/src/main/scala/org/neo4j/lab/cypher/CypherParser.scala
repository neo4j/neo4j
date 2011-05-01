package org.neo4j.lab.cypher

import commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction

class CypherParser extends JavaTokenParsers {

  def query: Parser[Query] = from ~ opt(where) ~ select ^^ {
    case from ~ where ~ select => Query(select, from, where)
  }

  def from: Parser[From] = "from" ~> repsep((nodeByIds | relatedToWithoutRelOut | relatedToWithRelOut), ",") ^^ (From(_: _*))

  def relatedToWithoutRelOut = "(" ~ ident ~ ")" ~ "-[" ~ "'" ~ ident ~ "'" ~ "]->" ~ "(" ~ ident ~ ")" ^^ {
    case "(" ~ start ~ ")" ~ "-[" ~ "'" ~ relType ~ "'" ~ "]->" ~ "(" ~ end ~ ")" => RelatedTo(start, end, "r", relType, Direction.OUTGOING)
  }

  def relatedToWithRelOut = "(" ~ ident ~ ")" ~ "-[" ~ ident ~ "," ~ "'" ~ ident ~ "'" ~ "]->" ~ "(" ~ ident ~ ")" ^^ {
    case "(" ~ start ~ ")" ~ "-[" ~ relName ~ "," ~ "'" ~ relType ~ "'" ~ "]->" ~ "(" ~ end ~ ")" => RelatedTo(start, end, relName, relType, Direction.OUTGOING)
  }

  def nodeByIds = ident ~ "=" ~ "node" ~ "(" ~ repsep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "node" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def select: Parser[Select] = "select" ~ repsep(ident, ",") ^^ {
    case "select" ~ columnNames => Select(columnNames.map(EntityOutput(_)): _*)
  }

  def where: Parser[Where] = "where" ~> rep(clause) ^^ (Where(_: _*))

  def clause: Parser[Clause] = (predicate | parens) * (
    "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
    "or" ^^^ {  (a: Clause, b: Clause) => Or(a, b) })

  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def predicate = (
    ident ~ "." ~ ident ~ "=" ~ stringLiteral ^^ {
      case c ~ "." ~ p ~ "=" ~ v => StringEquals(c, p, stripQuotes(v))
    })

  def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def parse(sql: String): Option[Query] =
    parseAll(query, sql) match {
      case Success(r, q) => Option(r)
      case x => println(x); None
    }
}