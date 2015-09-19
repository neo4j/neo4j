/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.docgen.tooling.Admonitions.Tip

import scala.collection.mutable

/**
 * DocBuilder allows for a stack based approach to building Documents,
 * instead of having to hand craft the Document tree
 */
trait DocBuilder {

  import DocBuilder._

  def build(): Document = {
    current match {
      case b: DocScope =>
        Document(b.title, b.id, b.initQueries, b.content)
    }
  }

  private val scope = new mutable.Stack[Scope]

  private def current = scope.top

  def doc(name: String, id: String) {
    scope.push(new DocScope(name, id))
  }

  def initQueries(queries: String*) = current.init(queries.map(_.stripMargin))

  def p(text: String) = current.addContent(Paragraph(text.stripMargin))

  def resultTable() = current.addContent(QueryResultTable)

  def abstraCt(text: String) = current.addContent(Abstract(text))

  // Scopes
  private def inScope(newScope: Scope, f: => Unit) = {
    scope.push(newScope)
    f
    val pop = scope.pop()
    current.addContent(pop.toContent)
  }

  def section(title: String)(f: => Unit) = inScope(new SectionScope(title), f)

  def tip(f: => Unit) = inScope(new TipScope, f)

  def query(q: String, assertions: QueryAssertions)(f: => Unit) = inScope(new QueryScope(q, assertions), f)

}

object DocBuilder {

  trait Scope {
    private var _initQueries = Seq.empty[String]
    var content: Content = NoContent

    def initQueries = _initQueries

    def init(queries: Seq[String]) {
      _initQueries = queries
    }

    def addContent(newContent: Content) {
      content = content match {
        case NoContent => newContent
        case _ => ContentChain(content, newContent)
      }
    }

    def toContent: Content
  }

  class DocScope(val title: String, val id: String) extends Scope {
    override def toContent = throw new LiskovSubstitutionPrincipleException
  }

  class SectionScope(name: String) extends Scope {
    override def toContent = Section(name, content)
  }

  class TipScope extends Scope {
    override def initQueries = throw new LiskovSubstitutionPrincipleException

    override def toContent = Tip(content)
  }

  class QueryScope(queryText: String, assertions: QueryAssertions) extends Scope {
    override def initQueries = throw new LiskovSubstitutionPrincipleException

    override def toContent = Query(queryText, assertions, content)
  }
}

class LiskovSubstitutionPrincipleException extends Exception