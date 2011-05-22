package org.neo4j.lab.cypher

import commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction
import scala.Some

class CypherParser extends JavaTokenParsers {
  def ignoreCase(str:String): Parser[String] = ("""(?i)\Q""" + str + """\E""").r

  def query: Parser[Query] = start ~ opt(matching) ~ opt(where) ~ select ^^ {
    case start ~ matching ~ where ~ select => Query(select, start, matching, where)
  }

  def start: Parser[Start] = ignoreCase("start") ~> repsep(nodeByIds | nodeByIndex, ",") ^^ (Start(_: _*))

  def matching: Parser[Match] = ignoreCase("match") ~> rep1sep(relatedTo, ",") ^^ { case matching:List[List[Pattern]] => Match(matching.flatten: _*) }

  def relatedTo: Parser[List[Pattern]] = relatedHead ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      val namer = new NodeNamer
      var last = namer.name(head)
      val list = tails.map((item) => {
        val back = item._1
        val relName = item._2
        val relType = item._3
        val forward = item._4
        val end = namer.name(item._5)

        val result: Pattern = RelatedTo(last, end, relName, relType, getDirection(back, forward))

        last = end

        result
      })

      list
    }
  }

  def relatedHead:Parser[Option[String]] = "(" ~> opt(ident) <~ ")" ^^ { case name => name }

  def relatedTail = opt("<") ~ "-" ~ opt("[" ~> (relationshipInfo1 | relationshipInfo2) <~ "]") ~ "-" ~ opt(">") ~ "(" ~ opt(ident) ~ ")" ^^ {
    case back ~ "-" ~ relInfo ~ "-" ~ forward ~ "(" ~ end ~ ")" => relInfo match {
      case Some(x) => (back, x._1, x._2, forward, end)
      case None => (back, None, None, forward, end)
    }
  }

  def relationshipInfo1 = opt(ident <~ ",") ~ ":" ~ ident ^^ {
    case relName ~ ":" ~ relType => (relName, Some(relType))
  }

  def relationshipInfo2 = ident ^^ {
    case relName => (Some(relName), None)
  }

  def nodeByIds = ident ~ "=" ~ ignoreCase("node") ~ "(" ~ repsep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ node ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def nodeByIndex = ident ~ "=" ~ ignoreCase("node_index") ~ "(" ~ stringLiteral ~ "," ~ stringLiteral ~ "," ~ stringLiteral ~ ")" ^^ {
    case varName ~ "=" ~ node_index ~ "(" ~ index ~ "," ~ key ~ "," ~ value ~ ")" =>
      NodeByIndex(varName, stripQuotes(index), stripQuotes(key), stripQuotes(value))
  }

  def select: Parser[Select] = ignoreCase("return") ~> rep1sep((count | propertyOutput | nodeOutput), ",") ^^ ( Select(_:_*) )

  def nodeOutput:Parser[SelectItem] = ident ^^ { EntityOutput(_) }

  def propertyOutput:Parser[SelectItem] = ident ~ "." ~ ident ^^
    { case c ~ "." ~ p => PropertyOutput(c,p) }

  def count:Parser[SelectItem] = ignoreCase("count") ~ "(" ~ "*" ~ ")" ^^
    { case count ~ "(" ~ "*" ~ ")" => Count("*") }

  def where: Parser[Where] = ignoreCase("where") ~> rep(clause) ^^ (Where(_: _*))

  def clause: Parser[Clause] = (predicate | parens ) * (
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
      case (true,true) => Direction.BOTH
    }

  def parse(sql: String): Option[Query] =
    parseAll(query, sql) match {
      case Success(r, q) => Option(r)
      case x => println(x); None
    }

  class NodeNamer {
    var lastNodeNumber = 0

    def name(s:Option[String]):String = s match {
      case None => {
        lastNodeNumber += 1
        "___NODE" + lastNodeNumber
      }
      case Some(x) => x
    }
  }
}