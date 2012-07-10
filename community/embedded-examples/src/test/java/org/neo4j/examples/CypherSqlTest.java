/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import static org.junit.Assert.assertTrue;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createCypherSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createQueryResultSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createSqlSnippet;

import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData.Title;

public class CypherSqlTest extends AbstractJavaDocTestbase
{
    private static CypherSql cyperSql;

    @BeforeClass
    public static void setUpDbs() throws SQLException
    {
        cyperSql = new CypherSql( new ImpermanentGraphDatabase() );
        cyperSql.createDbs();
    }

    @AfterClass
    public static void tearDownDbs() throws SQLException
    {
        cyperSql.shutdown();
    }

    /**
     * This guide is for people who understand SQL. You can use that prior
     * knowledge to quickly get going with Cypher and start exploring Neo4j.
     * 
     * == Start ==
     * 
     * SQL starts with the result you want -- we `SELECT` what we want and then
     * declare how to source it. In Cypher, the `START` clause is quite a
     * different concept which specifies starting points in the graph from which
     * the query will execute.
     * 
     * From a SQL point of view, the identifiers in `START` are like table names
     * that point to a set of nodes or relationships. The set can be listed
     * literally, come via parameters, or as I show in the following example, be
     * defined by an index look-up.
     * 
     * So in fact rather than being `SELECT`-like, the `START` clause is
     * somewhere between the `FROM` and the `WHERE` clause in SQL.
     * 
     * .SQL Query
     * @@Start-sql-query
     * 
     * @@Start-sql-result
     * 
     * .Cypher Query
     * @@Start-cypher-query
     * 
     * @@Start-cypher-result
     * 
     * Cypher allows multiple starting points. This should not be strange from a SQL perspective -- 
     * every table in the `FROM` clause is another starting point.
     * 
     * == Match ==
     * 
     * Unlike SQL which operates on sets, Cypher predominantly works on sub-graphs. 
     * The relational equivalent is the current set of tuples being evaluated during a `SELECT` query.
     * 
     * The shape of the sub-graph is specified in the `MATCH` clause. 
     * The `MATCH` clause is analogous to the `JOIN` in SQL. A normal a->b relationship is an 
     * inner join between nodes a and b -- both sides have to have at least one match, or nothing is returned.
     * 
     * We'll start with a simple example, where we find all email addresses that are connected to
     * the person ``Anakin''. This is an ordinary one-to-many relationship.
     * 
     * .SQL Query
     * @@Match-sql-query
     * 
     * @@Match-sql-result
     * 
     * .Cypher Query
     * @@Match-cypher-query
     * 
     * @@Match-cypher-result
     * 
     * There is no join table here, but if one is necessary the next example will show how to do that, writing the pattern relationship like so:
     * `-[r:belongs_to]->` will introduce (the equivalent of) join table available as the variable `r`. 
     * In reality this is a named relationship in Cypher, so we're saying ``join `Person` to `Group` via `belongs_to`.'' 
     * To illustrate this, consider this image, comparing the SQL model and Neo4j/Cypher.
     * 
     * ifdef::nonhtmloutput[]
     * image::RDBMSvsGraph.svg[scaledwidth="100%"]
     * endif::nonhtmloutput[]
     * ifndef::nonhtmloutput[]
     * image::RDBMSvsGraph.svg.png[scaledwidth="100%"]
     * endif::nonhtmloutput[]
     * 
     * And here are example queries:
     * 
     * .SQL Query
     * @@JoinEntity-sql-query
     * 
     * @@JoinEntity-sql-result
     * 
     * .Cypher Query
     * @@JoinEntity-cypher-query
     * 
     * @@JoinEntity-cypher-result
     * 
     * An http://www.codinghorror.com/blog/2007/10/a-visual-explanation-of-sql-joins.html[outer join] is just as easy.
     * Add a question mark `-[?:KNOWS]->` and it's an optional relationship between nodes -- the outer join of Cypher.
     * 
     * Whether it's a left outer join, or a right outer join is defined by which side of the pattern has a starting point.
     * This example is a left outer join, because the bound node is on the left side:
     * 
     * .SQL Query
     * @@LeftJoin-sql-query
     * 
     * @@LeftJoin-sql-result
     * 
     * .Cypher Query
     * @@LeftJoin-cypher-query
     * 
     * @@LeftJoin-cypher-result
     * 
     * Relationships in Neo4j are first class citizens -- it's like the SQL tables are pre-joined with each other. 
     * So, naturally, Cypher is designed to be able to handle highly connected data easily.
     * 
     * One such domain is tree structures -- anyone that has tried storing tree structures in SQL knows 
     * that you have to work hard to get around the limitations of the relational model. 
     * There are even books on the subject.
     * 
     * To find all the groups and sub-groups that Bridget belongs to, this query is enough in Cypher:
     * 
     * .Cypher Query
     * @@RecursiveJoin-cypher-query
     * 
     * @@RecursiveJoin-cypher-result
     * 
     * The * after the relationship type means that there can be multiple hops across +belongs_to+ relationships between group and user. 
     * Some SQL dialects have recursive abilities, that allow the expression of queries like this, but you may have a hard time wrapping your head around those. 
     * Expressing something like this in SQL is hugely impractical if not practically impossible.
     * 
     * == Where ==
     * 
     * This is the easiest thing to understand -- it's the same animal in both languages. 
     * It filters out result sets/subgraphs. 
     * Not all predicates have an equivalent in the other language, but the concept is the same.
     * 
     * .SQL Query
     * @@Where-sql-query
     * 
     * @@Where-sql-result
     * 
     * .Cypher Query
     * @@Where-cypher-query
     * 
     * @@Where-cypher-result
     * 
     * == Return ==
     * This is SQL's `SELECT`. 
     * We just put it in the end because it felt better to have it there -- 
     * you do a lot of matching and filtering, and finally, you return something.
     * 
     * Aggregate queries work just like they do in SQL, apart from the fact that there is no explicit `GROUP BY` clause. 
     * Everything in the return clause that is not an aggregate function will be used as the grouping columns.
     * 
     * .SQL Query
     * @@Return-sql-query
     *
     * @@Return-sql-result
     * 
     * .Cypher Query
     * @@Return-cypher-query
     * 
     * @@Return-cypher-result
     * 
     * Order by is the same in both languages - `ORDER BY` expression `ASC`/`DESC`. 
     * Nothing weird here.
     * 
     * // == Recursive queries ==
     * 
     * 
     * 
     */
    @Test
    @Documented
    @Title( "From SQL to Cypher" )
    public void test() throws Exception
    {
        for ( CypherSql.TestData query : cyperSql.queries )
        {
            String rawSqlResult = null;
            System.out.println( "\n*** " + query.name + " ***\n" );
            if ( query.sql != null )
            {
                String sqlSnippet = createSqlSnippet( query.sql );
                gen.get()
                        .addSnippet( query.name + "-sql-query", sqlSnippet );
                rawSqlResult = cyperSql.executeSql( query.sql );
                String sqlResult = createQueryResultSnippet( rawSqlResult );
                gen.get()
                        .addSnippet( query.name + "-sql-result", sqlResult );
                System.out.println( sqlSnippet );
                System.out.println( sqlResult );
            }
            String cypherSnippet = createCypherSnippet( query.cypher );
            gen.get()
                    .addSnippet( query.name + "-cypher-query", cypherSnippet );
            String rawCypherResult = cyperSql.executeCypher( query.cypher );
            String cypherResult = createQueryResultSnippet( rawCypherResult );
            gen.get()
                    .addSnippet( query.name + "-cypher-result", cypherResult );
            System.out.println( cypherSnippet );
            System.out.println( cypherResult );
            for ( String match : query.matchStrings )
            {
                if ( query.sql != null )
                {
                    assertTrue( "SQL result doesn't contain: '" + match + "'",
                            rawSqlResult.contains( match ) );
                }
                assertTrue( "Cypher result doesn't contain: '" + match + "'",
                        rawCypherResult.contains( match ) );
            }
        }
    }
}
