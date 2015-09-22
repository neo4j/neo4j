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

import java.io._

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.StringHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.spi.v2_3.TransactionBoundQueryContext
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{Assertions, Matchers}


/**
 * Base class for documentation classes
 *
 * When working with the documentation framework, it helps having the modeling
 * idea clear before trying to understand the code.
 *
 * The model used here is that these tests describe a tree structure that
 * contains both queries to be run and checked, and documentation in the same
 * class.
 *
 * Each DocumentingTest builds up one of these tree-structures, and this tree
 * is worked on in multiple steps, extracting queries and running them,
 * checking the results, and then inserting the results into the tree.
 *
 * Finally, if all the tests were successful, the results are written out to
 * disk as a single AsciiDoc file.
 */
trait DocumentingTest extends CypherFunSuite with Assertions with Matchers with GraphIcing {
  /**
   * Make sure this is implemented as a def and not a val. Since we are using it in the trait constructor,
   * and that runs before the class constructor, if it is a val, it will not have been initialised when we need it
   */
  def doc: Document

  runTestsFor(doc)

  def runTestsFor(doc: Document) = {
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    try {

      initializeDatabase(doc, db)
      val doc2 = captureStateAsGraphViz(doc, db, GraphVizBefore)
      val result = runQueries(doc2, db)
      reportResults(result)

      if (result.success) {
        val doc3 = captureStateAsGraphViz(doc2, db, GraphVizAfter)
        writeResultsToFile(doc3, db, result)
      }
    } finally db.shutdown()
  }

  private def writeResultsToFile(doc: Document, db: GraphDatabaseService, result: TestRunResult) {
    val document: Document = contentAndResultMerger(doc, result)

    val asciiDocTree = document.asciiDoc

    val dir = new File(s"target/docs/dev/ql/")
    if (!dir.exists())
      dir.mkdirs()

    val file = new File(s"target/docs/dev/ql/${doc.id}.adoc")
    val pw = new PrintWriter(file)
    println(asciiDocTree)
    pw.write(asciiDocTree)
    pw.close()
  }

  private def reportResults(result: TestRunResult) {
    result foreach {
      case QueryRunResult(q, Left(failure)) =>
        test(q.queryText)(throw failure)

      case QueryRunResult(q, Right(content)) =>
        test(q.queryText)({})
    }
  }

  private def runQueries(doc: Document, db: GraphDatabaseService): TestRunResult = {
    val builder = (tx: Transaction) => new QueryResultContentBuilder(new ValueFormatter(db, tx))

    val runner = new QueryRunner(db, builder)
    val result = runner.runQueries(init = doc.initQueries, queries = doc.queries)
    result
  }

  private def initializeDatabase(doc: Document, db: GraphDatabaseService) {
    doc.initQueries.foreach { q =>
      try {
        db.execute(q)
      } catch {
        case e: Throwable => throw new scala.RuntimeException(s"Initialising database failed on query: $q", e)
      }
    }
  }
}

// Used to format values coming from Cypher. Maps, collections, nodes, relationships and paths all have custom
// formatting applied to them
class ValueFormatter(db: GraphDatabaseService, tx: Transaction) extends (Any => String) with StringHelper with GraphIcing {
  def apply(x: Any): String = {
    val ctx = new TransactionBoundQueryContext(db.asInstanceOf[GraphDatabaseAPI], tx, true, db.statement)
    text(x, ctx)
  }
}

