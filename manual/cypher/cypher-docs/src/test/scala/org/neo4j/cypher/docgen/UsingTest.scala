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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.docgen.tooling._
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.JavaConverters._

class UsingTest extends DocumentingTest {
  override def outputPath = "target/docs/dev/ql/"
  override def doc = new DocBuilder {
    doc("Using", "query-using")
    initQueries(
      "CREATE INDEX ON :Scientist(name)",
      "CREATE INDEX ON :Science(name)",

      """CREATE (curie:Scientist {name: 'Curie', born: 1867})-[:RESEARCHED]->(chemistry:Science {name: 'Chemistry'}),
        |       (liskov:Scientist {name: 'Liskov', born: 1939})-[:RESEARCHED]->(cs:Science {name: 'Computer Science'}),
        |       (conway:Scientist {name: 'Conway', born: 1938})-[:RESEARCHED]->(cs),
        |       (wing:Scientist {name: 'Wing', born: 1956})-[:RESEARCHED]->(cs),
        |       (liskov)-[:KNOWS]->(wing)
      """
    )
    synopsis("The `USING` clause is used to influence the decisions of the planner when building an execution plan for a query.")
    caution {
      p("Forcing planner behavior is an advanced feature, and should be used with caution by experienced developers and/or database administrators only, as it may cause queries to perform poorly.")
    }
    section("Introduction") {
      p("""When executing a query, Neo4j needs to decide where in the query graph to start matching.
          |This is done by looking at the `MATCH` clause and the `WHERE` conditions and using that information to find useful indexes.""")
      p("This index might not be the best choice though -- sometimes multiple indexes could be used, and Neo4j has picked the wrong one (from a performance point of view).")
      p("You can force Neo4j to use a specific starting point through the `USING` clause. This is called giving an index hint.")
      p("""If your query matches large parts of an index, it might be faster to scan the label and filter out nodes that do not match.
          |To do this, you can use `USING SCAN`.
          |This will force Cypher to not use an index that could have been used, and instead do a label scan.""")
      note {
        p("You cannot use index hints if your query has a `START` clause.")
      }
      p("The following graph is used for the examples below:")
      graphViz()
    }
    section("Index hints") {
      section("Query using an index hint") {
        p("To query using an index hint, use `USING INDEX`.")
        query(s"MATCH (s:Scientist) USING INDEX s:Scientist(name) WHERE s.name = 'Curie' RETURN s.born AS $columnName",
              assertIntegersReturned(1867)) {
          p("Returns the year Marie Curie was born.")
          profileExecutionPlan()
        }
      }
      section("Query using multiple index hints") {
        p("To query using multiple index hint, use `USING INDEX`.")
        query(s"""MATCH (barbara:Scientist {name: 'Liskov'})-->(field:Science {name: 'Computer Science'})
                 |USING INDEX barbara:Scientist(name)
                 |USING INDEX field:Science(name)
                 |RETURN barbara.born AS $columnName""",
              assertIntegersReturned(1939)) {
          p("Returns the year Barbara Liskov was born.")
          profileExecutionPlan()
        }
      }
    }
    section("Scan hints") {
      section("Hinting a label scan") {
        p("If the best performance is to be had by scanning all nodes in a label and then filtering on that set, use `USING SCAN`.")
        query(s"""MATCH (s:Scientist)
                 |USING SCAN s:Scientist
                 |WHERE s.born < 1938
                 |RETURN s.born AS $columnName""",
              assertIntegersReturned(1867, 1939)) {
          p("Returns all scientists born before 1938.")
          profileExecutionPlan()
        }
      }
    }
    section("Join hints") {
      section("Hinting a join on a single node") {
        p("To force the query planner to produce a join on a variable, use `USING JOIN`.")
        query(s"""MATCH (liskov {name: 'Liskov'})-->(x:Science)<--(conway {name: 'Conway'})
                 |USING JOIN ON x
                 |RETURN x.name AS $columnName""",
              assertStringsReturnedAndUsingHashJoin("Computer Science")) {
          p("Returns the name of the science with both Barbara Liskov and Lynn Conway are related to, by joining.")
          profileExecutionPlan()
        }
      }
      section("Hinting a join on multiple nodes") {
        p("Force the query planner to produce a join between two specified variables.")
        query(s"""MATCH (conway {name: 'Conway'})-->(cs)<--(liskov {name: 'Liskov'})<--(wing)
                 |USING JOIN ON cs, liskov
                 |RETURN cs.name AS $columnName""", assertStringsReturnedAndUsingHashJoin("Computer Science")) {
          p("")
          profileExecutionPlan()
        }
      }
    }
  }.build()

  private val columnName = "column"

  private def assertIntegersReturned(values: Long*) = ResultAssertions(result =>
    values.foreach { value =>
      result.columnAs[Long](columnName) should equal(value)
    }
  )

  private def assertStringsReturnedAndUsingHashJoin(values: String*) = ResultAssertions(result => {
    values.foreach { value =>
      result.columnAs[String](columnName) should equal(value)
    }
    result.executionPlanDescription().toString should contain("NodeHashJoin")
  }
  )
}
