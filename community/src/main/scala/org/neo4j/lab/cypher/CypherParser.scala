package org.neo4j.lab.cypher

import commands._
import scala.util.parsing.combinator._

class CypherParser extends JavaTokenParsers {

  def query: Parser[Query] = from ~ opt(where) ~ select ^^ {
    case from ~ where ~ select => Query(select, from, where)
  }

  //  def operation:Parser[Operation] = {
  //    ("select" | "update" | "delete") ~ repsep(ident, ",") ^^ {
  //      case "select" ~ f => Select(f:_*)
  //      case _ => throw new IllegalArgumentException("Operation not implemented")
  //    }
  //  }
  //
  def from: Parser[From] = "from" ~> repsep(fromItem, ",") ^^ (From(_: _*))

  def fromItem: Parser[FromItem] = ident ~ "=" ~ "node" ~ "(" ~ repsep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "node" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def select: Parser[Select] = "select" ~ repsep(ident, ",") ^^ {
    case "select" ~ columnNames => Select(columnNames.map(NodeOutput(_)):_*)
  }

  def where: Parser[Where] = "where" ~> ident ~ "." ~ ident  ~ "=" ~ stringLiteral ^^ {
    case c ~ "." ~ p ~ "=" ~ v => Where(StringEquals(c,p,stripQuotes(v)))
  }

  //  def where:Parser[Where] = "where" ~> rep(clause) ^^ (Where(_:_*))
  //
  //  def clause:Parser[Clause] = (predicate|parens) * (
  //            "and" ^^^ { (a:Clause, b:Clause) => And(a,b) } |
  //            "or" ^^^ { (a:Clause, b:Clause) => Or(a,b) }
  //          )
  //
  //  def parens:Parser[Clause] = "(" ~> clause  <~ ")"
  //
  //  def predicate = (
  //      ident ~ "=" ~ boolean ^^ { case f ~ "=" ~ b => BooleanEquals(f,b)}
  //    | ident ~ "=" ~ stringLiteral ^^ { case f ~ "=" ~ v => StringEquals(f,stripQuotes(v))}
  //    | ident ~ "=" ~ wholeNumber ^^ { case f ~ "=" ~ i => NumberEquals(f,i.toInt)}
  //
  //  )
  //
  //  def boolean = ("true" ^^^ (true) | "false" ^^^ (false))
  //
  //  def order:Parser[Direction] = {
  //    "order" ~> "by" ~> ident  ~ ("asc" | "desc") ^^ {
  //      case f ~ "asc" => Asc(f)
  //      case f ~ "desc" => Desc(f)
  //    }
  //  }

  def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parse(sql: String): Option[Query] =
    parseAll(query, sql.toLowerCase) match {
      case Success(r, q) => Option(r)
      case x => println(x); None
    }
}