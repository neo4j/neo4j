/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.parser.v1_7

import org.neo4j.cypher.internal.commands._


trait StartClause extends Base {
  def start: Parser[Start] = ignoreCase("start") ~> comaList(startBit) ^^ (x => Start(x: _*)) | failure("expected 'START'")

  def startBit =
    (identity ~ "=" ~ lookup ^^ {      case id ~ "=" ~ l => l(id)    }
      | identity ~> "=" ~> opt("(") ~> failure("expected either node or relationship here")
      | identity ~> failure("expected identifier assignment"))

  def nodes = ignoreCase("node")

  def rels = (ignoreCase("relationship") | ignoreCase("rel")) ^^ (x => "rel")

  def typ = nodes | rels | failure("expected either node or relationship here")


  def lookup: Parser[String => StartItem] =
    nodes ~> parens(parameter) ^^ (p => (column: String) => NodeById(column, p)) |
      nodes ~> ids ^^ (p => (column: String) => NodeById(column, p)) |
      nodes ~> idxLookup ^^ { case (idxName, key, value) => (column: String) => NodeByIndex(column, idxName, key, value) } |
      nodes ~> idxString ^^ { case (idxName, query) => (column: String) => NodeByIndexQuery(column, idxName, query) } |
      nodes ~> parens("*") ^^ ( x => (column: String) => AllNodes(column) )  |
      rels ~> parens(parameter) ^^ (p => (column: String) => RelationshipById(column, p)) |
      rels ~> ids ^^ (p => (column: String) => RelationshipById(column, p)) |
      rels ~> idxLookup ^^ { case (idxName, key, value) => (column: String) => RelationshipByIndex(column, idxName, key, value) } |
      rels ~> idxString ^^ { case (idxName, query) => (column: String) => RelationshipByIndexQuery(column, idxName, query) } |
      rels ~> parens("*") ^^ ( x => (column: String) => AllRelationships(column) ) |
      nodes ~> opt("(") ~> failure("expected node id, or *") |
      rels ~> opt("(") ~> failure("expected relationship id, or *")


  def ids =
    (parens(comaList(wholeNumber)) ^^ (x => Literal(x.map(_.toLong)))
      | parens(comaList(wholeNumber) ~ opt(",")) ~> failure("trailing coma")
      | "(" ~> failure("expected graph entity id"))


  def idxString: Parser[(String, Expression)] = ":" ~> identity ~ parens(parameter | stringLit) ^^ {
    case id ~ valu => (id, valu)
  }

  def idxLookup: Parser[(String, Expression, Expression)] =
    ":" ~> identity ~ parens(idxQueries) ^^ { case a ~ b => (a, b._1, b._2)  } |
      ":" ~> identity ~> "(" ~> (id|parameter) ~> failure("`=` expected")

  def idxQueries: Parser[(Expression, Expression)] = idxQuery

  def indexValue = parameter | stringLit | failure("string literal or parameter expected")

  def idxQuery: Parser[(Expression, Expression)] =
    ((id | parameter) ~ "=" ~ indexValue ^^ { case k ~ "=" ~ v => (k, v) }
      | "=" ~> failure("Need index key"))

  def id: Parser[Expression] = identity ^^ (x => Literal(x))

  private def stringLit: Parser[Expression] = string ^^ (x => Literal(x))

  def andQuery: Parser[String] = idxQuery ~ ignoreCase("and") ~ idxQueries ^^ {
    case q ~ and ~ qs => q + " AND " + qs
  }

  def orQuery: Parser[String] = idxQuery ~ ignoreCase("or") ~ idxQueries ^^ {
    case q ~ or ~ qs => q + " OR " + qs
  }
}







