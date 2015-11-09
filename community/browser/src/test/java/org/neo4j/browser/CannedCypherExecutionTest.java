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


import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.jsoup.helper.StringUtil.join;
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

        final AtomicInteger explainCount = new AtomicInteger( 0 );
        final AtomicInteger executionCount = new AtomicInteger( 0 );

        Files.walkFileTree( Paths.get( resourceLoc.toURI() ), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attributes ) throws IOException
            {
                final GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();

                String fileName = file.getFileName().toString();
                if ( fileName.endsWith( ".html" ) )
                {
                    String content = FileUtils.readTextFile( file.toFile(), StandardCharsets.UTF_8 );
                    Elements cypherElements = Jsoup.parse( content ).select( "pre.runnable" )
                            .not( ".standalone-example" );
                    for ( Element cypherElement : cypherElements )
                    {
                        String statement = replaceAngularExpressions( cypherElement.text() );

                        if ( !statement.startsWith( ":" ) )
                        {
                            if ( shouldExplain( statement ) )
                            {
                                try ( Transaction transaction = database.beginTx() )
                                {
                                    Iterable<Notification> notifications = database.execute(
                                            prependExplain( statement ) ).getNotifications();

                                    List<Status.Statement> ignorableStatusCodes = new ArrayList<>();
                                    if ( hasWarningInHtml( cypherElement ) )
                                    {
                                        ignorableStatusCodes.add( Status.Statement.CartesianProduct );
                                        ignorableStatusCodes.add( Status.Statement.UnboundedPatternWarning );
                                    }

                                    List<Notification> filteredNotifications = new ArrayList<>();
                                    for ( Notification notification : notifications )
                                    {
                                        if ( !hasIgnorableNotificationCode( notification, ignorableStatusCodes ) )
                                        {
                                            filteredNotifications.add( notification );
                                        }
                                    }

                                    assertThat( format( "Query [%s] should only produce %s notifications. [%s]",
                                            ignorableStatusCodes, statement, fileName ),
                                            filteredNotifications, empty() );

                                    explainCount.incrementAndGet();
                                    transaction.success();
                                }
                                catch ( QueryExecutionException e )
                                {
                                    throw new AssertionError( format( "Failed to explain query [%s] in file [%s]",
                                            statement, file ), e );
                                }
                            }
                            try ( Transaction transaction = database.beginTx() )
                            {
                                database.execute( statement );
                                executionCount.incrementAndGet();
                                transaction.success();
                            }
                            catch ( QueryExecutionException e )
                            {
                                throw new AssertionError( format( "Failed to execute query [%s] in file [%s]",
                                        statement, file ), e );
                            }
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        } );

        assertTrue( "Static files should contain at least one valid cypher statement",
                executionCount.intValue() >= 1 );
    }

    private boolean hasWarningInHtml( Element cypherElement )
    {
        return !cypherElement.parent().select( ".warn" ).isEmpty();
    }

    private boolean hasIgnorableNotificationCode( Notification notification, List<Status.Statement> ignorableCodes )
    {
        for ( Status.Statement ignorableCode : ignorableCodes )
        {
            if ( ignorableCode.code().toString().contains( notification.getCode() ) )
            {
                return true;
            }
        }
        return false;
    }

    private static String replaceAngularExpressions( String statement )
    {
        Pattern angularExpressionPattern = Pattern.compile( "\\{\\{(.*?)}}" );
        Matcher matcher = angularExpressionPattern.matcher( statement );

        StringBuffer buffer = new StringBuffer();
        while ( matcher.find() )
        {
            String expression = matcher.group( 1 );
            matcher.appendReplacement( buffer, chooseSuitableExpressionValue( expression ) );
        }
        matcher.appendTail( buffer );
        return buffer.toString();
    }

    private static String chooseSuitableExpressionValue( String expression )
    {
        // Generally we can safely return any old string, but in rare situations, a number might be
        // required. The rare situation is had-coded below. Unfortunately, if the canned cypher queries use more
        // parameters that need to be integers, this code will have to be updated.
        return "relationshipDepth".equals( expression ) ? "1" : "string";
    }

    private static boolean shouldExplain( String statement )
    {
        return !stripComments( statement ).toUpperCase().startsWith( "PROFILE" );
    }

    private static String prependExplain( String statement )
    {
        if ( !stripComments( statement ).toUpperCase().startsWith( "EXPLAIN" ) )
        {
            return "EXPLAIN " + statement;
        }
        return statement;
    }

    private static String stripComments( String statement )
    {
        String[] lines = statement.replaceAll( "/\\*.*\\*/", "" ).split( "\n" );
        List<String> nonCommentLines = new ArrayList<>();
        for ( String line : lines )
        {
            if ( !line.trim().startsWith( "//" ) )
            {
                nonCommentLines.add( line );
            }
        }
        return join( nonCommentLines, "\n" );
    }
}
