/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.cypher.internal.commands._
import expressions._
import expressions.Literal
import expressions.ParameterExpression
import org.neo4j.graphdb.Direction
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands.AllNodes
import org.neo4j.cypher.internal.commands.NodeByIndex
import org.neo4j.cypher.internal.mutation.CreateNode
import org.neo4j.cypher.internal.mutation.CreateRelationship
import org.neo4j.cypher.internal.commands.RelationshipByIndexQuery
import org.neo4j.cypher.internal.commands.AllRelationships
import org.neo4j.cypher.internal.commands.NodeByIndexQuery
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.commands.CreateRelationshipStartItem
import org.neo4j.cypher.internal.mutation.RelationshipEndpoint
import org.neo4j.cypher.internal.commands.CreateNodeStartItem
import org.neo4j.cypher.internal.commands.RelationshipByIndex


trait StartClause extends Base with Expressions with CreateUnique {
  def start: Parser[(Seq[StartItem], Seq[NamedPath])] = createStart | readStart | failure("expected START or CREATE")

  def readStart: Parser[(Seq[StartItem], Seq[NamedPath])] = ignoreCase("start") ~> commaList(startBit) ^^ (x => (x, Seq()))

  def createStart: Parser[(Seq[StartItem], Seq[NamedPath])] = relate|create

  def create = ignoreCase("create") ~> commaList(usePattern(translate)) ^^ {
    case matching =>
      val pathsAndItems = matching.flatten.filter(_.isInstanceOf[NamedPathWStartItems]).map(_.asInstanceOf[NamedPathWStartItems])
      val startItems = matching.flatten.filter(_.isInstanceOf[StartItem]).map(_.asInstanceOf[StartItem])
      val namedPaths = pathsAndItems.map(_.path)
      val pathItems = pathsAndItems.flatMap(_.items)

      ((startItems ++ pathItems), namedPaths)
  }

  case class NamedPathWStartItems(path:NamedPath, items:Seq[StartItem])

  private def removeProperties(in:AbstractPattern):AbstractPattern = in match {
    case ParsedNamedPath(name, patterns) => throw new ThisShouldNotHappenError("Andres", "We don't support paths in paths, and so should never see this")
    case rel: ParsedRelation => rel.copy(
      props = Map(),
      start = rel.start.copy(props = Map()),
      end = rel.end.copy(props = Map())
    )
    case n:ParsedEntity => n.copy(props = Map())
    case _ => throw new ThisShouldNotHappenError("Stefan/Andres", "This non-exhaustive match would have been a RuntimeException in the past")
  }

  private def translate(abstractPattern: AbstractPattern): Maybe[Any] = abstractPattern match {
    case ParsedNamedPath(name, patterns) =>
      val namedPathPatterns: Maybe[Any] = patterns.
                                          map(removeProperties).
                                          map(matchTranslator).
                                          reduce(_ ++ _)

      val startItems = patterns.map(p => translate(p.makeOutgoing)).reduce(_ ++ _)

      startItems match {
        case No(msg)    => No(msg)
        case Yes(stuff) => namedPathPatterns.seqMap(p => {
          val namedPath: NamedPath = NamedPath(name, p.map(_.asInstanceOf[Pattern]): _*)
          Seq(NamedPathWStartItems(namedPath, stuff.map(_.asInstanceOf[StartItem])))
        })
      }

    case ParsedRelation(name, props, ParsedEntity(_, a, startProps, True()), ParsedEntity(_, b, endProps, True()), relType, dir, map, True()) if relType.size == 1 =>
      val (from, to) = if (dir != Direction.INCOMING)
                         (a, b)
                       else
                         (b, a)

      Yes(Seq(CreateRelationshipStartItem(
        CreateRelationship(name,
          RelationshipEndpoint(from, startProps, Seq.empty, bare = true),
          RelationshipEndpoint(to, endProps, Seq.empty, bare = true), relType.head, props))))


    case ParsedEntity(_, Identifier(name), props, True()) =>
      Yes(Seq(CreateNodeStartItem(CreateNode(name, props, Seq.empty))))

    case ParsedEntity(_, p: ParameterExpression, _, True()) =>
      Yes(Seq(CreateNodeStartItem(CreateNode(namer.name(None), Map[String, Expression]("*" -> p), Seq.empty))))

    case _ => No(Seq(""))
  }

  def startBit =
    (identity ~ "=" ~ lookup ^^ {
      case id ~ "=" ~ l => l(id)
    }
      | identity ~> "=" ~> opt("(") ~> failure("expected either node or relationship here")
      | identity ~> failure("expected identifier assignment"))

  def nodes = ignoreCase("node")

  def rels = (ignoreCase("relationship") | ignoreCase("rel")) ^^^ "rel"

  def typ = nodes | rels | failure("expected either node or relationship here")

  def lookup: Parser[String => StartItem] =
    nodes ~> parens(parameter) ^^ (p => (column: String) => NodeById(column, p)) |
      nodes ~> ids ^^ (p => (column: String) => NodeById(column, p)) |
      nodes ~> idxLookup ^^ nodeIndexLookup |
      nodes ~> idxString ^^ nodeIndexString |
      nodes ~> parens("*") ^^ (x => (column: String) => AllNodes(column)) |
      rels ~> parens(parameter) ^^ (p => (column: String) => RelationshipById(column, p)) |
      rels ~> ids ^^ (p => (column: String) => RelationshipById(column, p)) |
      rels ~> idxLookup ^^ relationshipIndexLookup |
      rels ~> idxString ^^ relationshipIndexString |
      rels ~> parens("*") ^^ (x => (column: String) => AllRelationships(column)) |
      nodes ~> opt("(") ~> failure("expected node id, or *") |
      rels ~> opt("(") ~> failure("expected relationship id, or *")


  def relationshipIndexString: ((String, Expression)) => (String) => RelationshipByIndexQuery = {
    case (idxName, query) => (column: String) => RelationshipByIndexQuery(column, idxName, query)
  }

  def nodeIndexString: ((String, Expression)) => (String) => NodeByIndexQuery = {
    case (idxName, query) => (column: String) => NodeByIndexQuery(column, idxName, query)
  }

  def nodeIndexLookup: ((String, Expression, Expression)) => (String) => NodeByIndex = {
    case (idxName, key, value) => (column: String) => NodeByIndex(column, idxName, key, value)
  }

  def relationshipIndexLookup: ((String, Expression, Expression)) => (String) => RelationshipByIndex = {
    case (idxName, key, value) => (column: String) => RelationshipByIndex(column, idxName, key, value)
  }

  def ids =
    (parens(commaList(wholeNumber)) ^^ (x => Literal(x.map(_.toLong)))
      | parens(commaList(wholeNumber) ~ opt(",")) ~> failure("trailing coma")
      | "(" ~> failure("expected graph entity id"))


  def idxString: Parser[(String, Expression)] = ":" ~> identity ~ parens(parameter | stringLit) ^^ {
    case id ~ valu => (id, valu)
  }

  def idxLookup: Parser[(String, Expression, Expression)] =
    ":" ~> identity ~ parens(idxQueries) ^^ {
      case a ~ b => (a, b._1, b._2)
    } |
      ":" ~> identity ~> "(" ~> (id | parameter) ~> failure("`=` expected")

  def idxQueries: Parser[(Expression, Expression)] = idxQuery

  def indexValue = parameter | stringLit | failure("string literal or parameter expected")

  def idxQuery: Parser[(Expression, Expression)] =
    ((id | parameter) ~ "=" ~ indexValue ^^ {
      case k ~ "=" ~ v => (k, v)
    }
      | "=" ~> failure("Need index key"))

  def id: Parser[Expression] = identity ^^ (x => Literal(x))

  def andQuery: Parser[String] = idxQuery ~ ignoreCase("and") ~ idxQueries ^^ {
    case q ~ and ~ qs => q + " AND " + qs
  }

  def orQuery: Parser[String] = idxQuery ~ ignoreCase("or") ~ idxQueries ^^ {
    case q ~ or ~ qs => q + " OR " + qs
  }
}







