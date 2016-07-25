/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.docgen.tooling.Admonitions.{Caution, Note, Tip}

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
    scope.push(DocScope(name, id))
  }

  def initQueries(queries: String*) = current.setInitQueries(queries.map(_.stripMargin))

  def p(text: String) = current.addContent(Paragraph(text.stripMargin))

  def function(syntax: String, arguments: (String, String)*) = current.addContent(Function(syntax, arguments))

  def resultTable() = {
    val queryScope = scope.collectFirst {
      case q: QueryScope => q
    }.get
    queryScope.addContent(new TablePlaceHolder(queryScope.assertions))
  }

  def executionPlan() = {
    val queryScope = scope.collectFirst {
      case q: QueryScope => q
    }.get
    queryScope.addContent(new ExecutionPlanPlaceHolder())
  }

  def profileExecutionPlan() = {
    val queryScope = scope.collectFirst {
      case q: QueryScope => q
    }.get
    queryScope.addContent(new ProfileExecutionPlanPlaceHolder(queryScope.assertions))
  }

  def graphViz(options: String = "") = current.addContent(new GraphVizPlaceHolder(options))

  def consoleData() = {
    val docScope = scope.collectFirst {
      case d: DocScope => d
    }.get
    val queryScope = scope.collectFirst {
      case q: QueryScope => q
    }.get
    queryScope.addContent(ConsoleData(docScope.initQueries, queryScope.initQueries, queryScope.queryText))
  }

  def synopsis(text: String) = current.addContent(Abstract(text))

  // Scopes
  private def inScope(newScope: Scope, f: => Unit) = {
    scope.push(newScope)
    f
    val pop = scope.pop()
    current.addContent(pop.toContent)
  }

  def section(title: String, id: String)(f: => Unit) = inScope(SectionScope(title, Some(id)), f)
  def section(title: String)(f: => Unit) = inScope(SectionScope(title, None), f)

  def tip(f: => Unit) = inScope(AdmonitionScope(Tip.apply), f)
  def note(f: => Unit) = inScope(AdmonitionScope(Note.apply), f)
  def caution(f: => Unit) = inScope(AdmonitionScope(Caution.apply), f)

  def query(q: String, assertions: QueryAssertions)(f: => Unit) =
    inScope(QueryScope(q.stripMargin, assertions), {
      f
      consoleData() // Always append console data
    })
}

object DocBuilder {

  trait Scope {
    private var _initQueries = Seq.empty[String]
    private var _content: Content = NoContent

    def initQueries = _initQueries
    def content = _content
    def setInitQueries(queries: Seq[String]) {
      _initQueries = queries
    }

    def addContent(newContent: Content) {
      _content = _content match {
        case NoContent => newContent
        case _ => ContentChain(content, newContent)
      }
    }

    def toContent: Content
  }

  case class DocScope(title: String, id: String) extends Scope {
    override def toContent = throw new LiskovSubstitutionPrincipleException
  }

  case class SectionScope(name: String, id: Option[String]) extends Scope {
    override def toContent = Section(name, id, initQueries, content)
  }

  case class AdmonitionScope(f: Content => Content) extends Scope {
    override def initQueries = throw new LiskovSubstitutionPrincipleException

    override def toContent = f(content)
  }

  case class QueryScope(queryText: String, assertions: QueryAssertions) extends Scope {
    override def toContent = Query(queryText, assertions, initQueries, content)
  }
}

class LiskovSubstitutionPrincipleException extends Exception
