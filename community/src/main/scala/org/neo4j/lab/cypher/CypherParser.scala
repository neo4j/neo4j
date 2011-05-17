package org.neo4j.lab.cypher

import commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction

class CypherParser extends JavaTokenParsers {

  def query: Parser[Query] = from ~ opt(where) ~ select ^^ {
    case from ~ where ~ select => Query(select, from, where)
  }

  def from: Parser[From] = "from" ~> repsep(nodeByIds, ",") ^^ (From(_: _*))

  def relatedTo: Parser[Clause] = relatedHead ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      val namer = new NodeNamer
      var last = namer.name(head)
      val list = tails.map((item) => {
        val back = item._1
        val relName = item._2
        val relType = item._3
        val forward = item._4
        val end = namer.name(item._5)

        val result: Clause = RelatedTo(last, end, relName, relType, getDirection(back, forward))

        last = end

        result
      })

      list.reduceLeft(_ ++ _)
    }
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


  def relatedHead:Parser[Option[String]] = "(" ~> opt(ident) <~ ")" ^^ { case name => name }
  def relatedTail = opt("<") ~ "-[" ~ opt(ident <~ ",") ~ "'" ~ ident ~ "'" ~ "]-" ~ opt(">") ~ "(" ~ opt(ident) ~ ")" ^^ {
    case back ~ "-[" ~ relName ~ "'" ~ relType ~ "'" ~ "]-" ~ forward ~ "(" ~ end ~ ")" => (back, relName, relType, forward, end)
  }

  def nodeByIds = ident ~ "=" ~ "node" ~ "(" ~ repsep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "node" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def select: Parser[Select] = "select" ~> repsep((propertyOutput|nodeOutput), ",") ^^ ( Select(_:_*) )

  def nodeOutput:Parser[SelectItem] = ident ^^ { EntityOutput(_) }
  def propertyOutput:Parser[SelectItem] = ident ~ "." ~ ident ^^ { case c ~ "." ~ p => PropertyOutput(c,p) }

  def where: Parser[Where] = "where" ~> rep(clause) ^^ (Where(_: _*))

  def clause: Parser[Clause] = (predicate | parens | relatedTo) * (
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