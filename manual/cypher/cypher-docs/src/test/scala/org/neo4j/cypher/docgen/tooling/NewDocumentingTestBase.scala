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

import org.scalatest.{Assertions, FunSuiteLike, Matchers}

trait NewDocumentingTestBase extends FunSuiteLike with Assertions with Matchers {
  /**
   * Make sure this is implemented as a def and not a val. Since we are using it in the trait constructor,
   * and that runs before the class constructor, if it is a val, it will not have been initialised when we need it
   */
   def doc: Document

   runTestsFor(doc)

   def runTestsFor(doc: Document) = {

     val runner = new QueryRunner(QueryResultContentBuilder)

     runner.runQueries(init = doc.initQueries, queries = doc.content.queries) foreach {
       case QueryRunResult(q, Left(failure)) => test(q) { throw failure }
       case QueryRunResult(q, Right(content)) => test(q) {}
     }
   }
 }
