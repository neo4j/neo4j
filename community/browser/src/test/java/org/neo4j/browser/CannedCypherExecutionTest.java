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
package org.neo4j.browser;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The browser code includes a number of canned example cypher statements. It's important that these statements continue
 * to work as the database and the examples continue to evolve. This test provides the most basic coverage: it asserts
 * that the statements can be executed without throwing an exception. There is no attempt to assert that the statements
 * have an expected behaviour or return correct results. However, this test should be good enough to catch the majority
 * of accidental syntax errors in the browser code. It may also provide early warning that examples need to be updated
 * when cypher syntax moves on.
 */
public class CannedCypherExecutionTest
{
    @Test
    public void shouldBeAbleToExecuteAllTheCannedCypherQueriesContainedInStaticHtmlFiles() throws Exception
    {
        URL resourceLoc = getClass().getClassLoader().getResource( "browser" );
        assertNotNull( resourceLoc );

        final GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final AtomicInteger cypherStatementCount = new AtomicInteger( 0 );

        Files.walkFileTree( Paths.get( resourceLoc.toURI() ), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attributes ) throws IOException
            {
                if ( file.getFileName().toString().endsWith( ".html" ) )
                {
                    String content = FileUtils.readTextFile( file.toFile(), Charsets.UTF_8 );
                    Elements cypherElements = Jsoup.parse( content ).select( "pre.runnable" );
                    for ( Element cypherElement : cypherElements )
                    {
                        String statement = replaceAngularExpressions( cypherElement.text() );

                        if ( !statement.startsWith( ":" ) )
                        {
                            try ( Transaction transaction = database.beginTx() )
                            {
                                database.execute( statement );
                                transaction.success();
                            }
                            catch ( QueryExecutionException e )
                            {
                                throw new AssertionError( format( "Failed to execute query [%s] in file [%s]",
                                        statement, file ), e );
                            }
                            cypherStatementCount.incrementAndGet();
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        } );

        assertTrue( "Static files should contain at least one valid cypher statement",
                cypherStatementCount.intValue() > 0 );
        System.out.printf( "Successfully executed %s cypher statements extracted from HTML files.%n",
                cypherStatementCount );
    }

    private String replaceAngularExpressions( String statement )
    {
        Pattern angularExpressionPattern = Pattern.compile( "\\{\\{(.*?)}}" );
        Matcher matcher = angularExpressionPattern.matcher( statement );

        StringBuffer buffer = new StringBuffer(  );
        while( matcher.find() )
        {
            String expression = matcher.group( 1 );
            matcher.appendReplacement( buffer, chooseSuitableExpressionValue( expression ) );
        }
        matcher.appendTail( buffer );
        return buffer.toString();
    }

    private String chooseSuitableExpressionValue( String expression )
    {
        // Generally we can safely return any old string, but in rare situations, a number might be
        // required. The rare situation is had-coded below. Unfortunately, if the canned cypher queries use more
        // parameters that need to be integers, this code will have to be updated.
        return "relationshipDepth".equals( expression ) ? "1" : "string";
    }
}
