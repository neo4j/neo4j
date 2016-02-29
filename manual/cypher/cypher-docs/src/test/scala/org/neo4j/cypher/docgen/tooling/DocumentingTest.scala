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

import java.io._

import org.neo4j.cypher.internal.compiler.v3_0.CypherSerializer
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.spi.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.v3_0.TransactionBoundQueryContext
import org.neo4j.cypher.internal.spi.v3_0.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.coreapi.{PropertyContainerLocker, InternalTransaction}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext
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

  def outputPath: String = "target/docs/dev/ql/"

  runTestsFor(doc, outputPath)

  def runTestsFor(doc: Document, outputPath: String) = {
    val result = runQueries(doc)
    reportResults(result)
    if (result.success) {
      writeResultsToFile(doc, result, outputPath)
    }
  }

  private def writeResultsToFile(doc: Document, result: TestRunResult, outputPath: String) {
    val document: Document = contentAndResultMerger(doc, result)

    val asciiDocTree = document.asciiDoc

    val outputPathWithSeparator = if(outputPath.endsWith(File.separator)) outputPath else outputPath + File.separator
    val dir = new File(outputPathWithSeparator)
    if (!dir.exists())
      dir.mkdirs()

    val file = new File(s"$outputPathWithSeparator${doc.id}.adoc")
    val pw = new PrintWriter(file)
    pw.write(asciiDocTree)
    pw.close()
  }

  private def reportResults(result: TestRunResult) {
    var count = 0

    def testName(q: String) = {
      count +=1
      s"$count: $q".replaceAll(System.lineSeparator(), " ")
    }

    result foreach {
      case QueryRunResult(q, _, Left(failure)) =>
        test(testName(q))(throw failure)

      case QueryRunResult(q, _, Right(content)) =>
        test(testName(q))({})

      case ExecutionPlanRunResult(q, _, executionPlan) =>
        test(testName(q))({})

      case _:GraphVizRunResult => // Nothing to report here, unless we got a failure
    }
  }

  private def runQueries(doc: Document): TestRunResult = {
    val builder = (db: GraphDatabaseQueryService, tx: InternalTransaction) => new QueryResultContentBuilder(new ValueFormatter(db, tx))

    val runner = new QueryRunner(builder)
    val result = runner.runQueries(contentsWithInit = doc.contentWithQueries, doc.title)
    result
  }
}

// Used to format values coming from Cypher. Maps, lists, nodes, relationships and paths all have custom
// formatting applied to them
class ValueFormatter(db: GraphDatabaseQueryService, tx: InternalTransaction) extends (Any => String) with CypherSerializer with GraphIcing {
  def apply(x: Any): String = {
    val transactionalContext = new TransactionalContextWrapper(new Neo4jTransactionalContext(db, tx, db.statement, new PropertyContainerLocker))
    val ctx = new TransactionBoundQueryContext(transactionalContext)(QuietMonitor)
    serialize(x, ctx)
  }
}

object QuietMonitor extends IndexSearchMonitor {
  override def indexSeek(index: IndexDescriptor, value: Any) = {}
  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any) = {}
}
