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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.frontend.v2_3.Foldable._

case class AsciiDocResult(text: String, testResults: Seq[(String, Option[Exception])])

case class Document(title: String, id: String, initQueries: Seq[String], content: Content) {
  def asciiDoc: AsciiDocResult = {
    val text =
      s"""[[$id]]
         |= $title
         |
         |""".stripMargin + content.asciiDoc(0)

    AsciiDocResult(text, Seq.empty)
  }
}

sealed trait Content {
  def ~(other: Content): Content = ContentChain(this, other)

  def asciiDoc(level: Int): String

  def NewLine: String = "\n"

  def queries: Seq[Query] = this.findAllByClass[Query]
}

case object NoContent extends Content {
  override def asciiDoc(level: Int) = ""
}

case class ContentChain(a: Content, b: Content) extends Content {
  override def asciiDoc(level: Int) = a.asciiDoc(level) + b.asciiDoc(level)

  override def toString: String = s"$a ~ $b"
}

case class Abstract(s: String) extends Content {
  override def asciiDoc(level: Int) =
    s"""[abstract]
       |====
       |$s
       |====
       |
       |""".stripMargin
}

case class Heading(s: String) extends Content {
  override def asciiDoc(level: Int) = "." + s + NewLine
}

case class Paragraph(s: String) extends Content {
  override def asciiDoc(level: Int) = s + NewLine + NewLine
}

object Admonitions {

  object Tip {
    def apply(s: Content) = new Tip(None, s)

    def apply(heading: String, s: Content) = new Tip(Some(heading), s)
  }

  case class Tip(heading: Option[String], innerContent: Content) extends Admonitions

  object Warning {
    def apply(s: Content) = new Warning(None, s)

    def apply(heading: String, s: Content) = new Warning(Some(heading), s)
  }

  case class Warning(heading: Option[String], innerContent: Content) extends Admonitions

  object Note {
    def apply(s: Content) = new Note(None, s)

    def apply(heading: String, s: Content) = new Note(Some(heading), s)
  }

  case class Note(heading: Option[String], innerContent: Content) extends Admonitions

  object Caution {
    def apply(s: Content) = new Caution(None, s)

    def apply(heading: String, s: Content) = new Caution(Some(heading), s)
  }

  case class Caution(heading: Option[String], innerContent: Content) extends Admonitions

  object Important {
    def apply(s: Content) = new Important(None, s)

    def apply(heading: String, s: Content) = new Important(Some(heading), s)
  }

  case class Important(heading: Option[String], innerContent: Content) extends Admonitions {
    override def name = "IMPORTANT"
  }

}

trait Admonitions extends Content {
  def innerContent: Content

  def heading: Option[String]

  def name: String = this.getClass.getSimpleName.toUpperCase

  override def asciiDoc(level: Int) = {
    val inner = innerContent.asciiDoc(level)
    val head = heading.map("." + _ + NewLine).getOrElse("")

    s"[$name]" + NewLine + head +
      s"""====
         |$inner
          |====
          |
          |""".
        stripMargin
  }
}

case class GraphImage(s: ImageType) extends Content {
  override def asciiDoc(level: Int) = ???
}

case class ResultRow(values: Seq[Any])

case class QueryResult(columns: Seq[String], rows: Seq[ResultRow], footer: String) extends Content {
  override def asciiDoc(level: Int): String = {

    val header = if (columns.nonEmpty) "header," else ""
    val cols = if (columns.isEmpty) 1 else columns.size
    val rowsOutput: String = if (rows.isEmpty) s"$cols+|(empty result)"
    else {
      val columnHeader = columns.map(_.replace("|", "\\|")).mkString("|", "|", "")
      val tableRows =
        rows.
        map(row => row.values.map(_.toString.replace("|", "\\|")).
        mkString("||", "|", "")).
        mkString("\n")

      s"$columnHeader\n$tableRows"
    }

   s""".Result
      |[role="queryresult",options="${header}footer",cols="$cols*<m"]
      ||===
      |$rowsOutput
      |$cols+|$footer
      ||===
      |
      |""".stripMargin
  }
}

case class Query(queryText: String, assertions: QueryAssertions, content: Content) extends Content {

  override def asciiDoc(level: Int) = {
    val inner = Prettifier(queryText)
    s"""[source,cypher]
       |.Query
       |----
       |$inner
       |----
       |
       |""".stripMargin
  }
}

case class Section(heading: String, content: Content) extends Content {

  override def asciiDoc(level: Int) = {
    val levelIndent = (0 to (level + 1)).map(_ => "=").mkString
    levelIndent + " " + heading + NewLine + NewLine + content.asciiDoc(level + 1)
  }
}

sealed trait QueryAssertions

case class ResultAssertions(f: InternalExecutionResult => Unit) extends QueryAssertions

case class ResultAndDbAssertions(f: (InternalExecutionResult, GraphDatabaseService) => Unit) extends QueryAssertions

case object NoAssertions extends QueryAssertions

case class ExpectedException[EXCEPTION <: Exception](f: EXCEPTION => Unit)
                                                    (implicit m: Manifest[EXCEPTION]) extends QueryAssertions {
  def getExceptionClass = m.runtimeClass

  def handle(e: Exception) = if (e.getClass.isAssignableFrom(m.runtimeClass)) {
    f(e.asInstanceOf[EXCEPTION])
  } else {
    throw new RuntimeException("your mama")
  }
}

case object QueryResultTable extends Content {
  override def asciiDoc(level: Int) = ???
}

trait ImageType

object ImageType {

  case object INITIAL extends ImageType

}
