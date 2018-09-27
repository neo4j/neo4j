/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.javacompat;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.cypher.internal.javacompat.ExpressionEngineConfigurationTest.ExpressionEngine.COMPILED;
import static org.neo4j.cypher.internal.javacompat.ExpressionEngineConfigurationTest.ExpressionEngine.INTERPRETED;

class ExpressionEngineConfigurationTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldNotUseCompiledExpressionsFirstTimeWithDefaultSettings()
    {
        // Given
        GraphDatabaseService db = withEngineAndLimit( "DEFAULT", 1 );

        // When
        ExpressionEngine engine = engineUsedForQuery( db, "RETURN sin(cos(sin(cos(rand()))))" );

        // Then
        assertThat( engine, equalTo( INTERPRETED ) );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenLimitIsZero()
    {
        // Given
        GraphDatabaseService db = withEngineAndLimit( "DEFAULT", 0 );

        // When
        ExpressionEngine engine = engineUsedForQuery( db, "RETURN sin(cos(sin(cos(rand()))))" );

        // Then
        assertThat( engine, equalTo( COMPILED ) );
    }

    @Test
    void shouldUseCompiledExpressionsWhenQueryIsHotWithDefaultSettings()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( "DEFAULT", 3 );
        db.execute( query );
        db.execute( query );
        db.execute( query );

        // When
        ExpressionEngine engine = engineUsedForQuery( db, query );

        // Then
        assertThat( engine, equalTo( COMPILED ) );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenConfigured()
    {
        // Given
        GraphDatabaseService db = withEngineAndLimit( "COMPILED", 42 );

        // When
        ExpressionEngine engine = engineUsedForQuery( db, "RETURN sin(cos(sin(cos(rand()))))" );

        // Then
        assertThat( engine, equalTo( COMPILED ) );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenExplicitlyAskedFor()
    {
        // Given
        GraphDatabaseService db = withEngineAndLimit( "DEFAULT", 42 );

        // When
        ExpressionEngine engine =
                engineUsedForQuery( db, "CYPHER expressionEngine=COMPILED RETURN sin(cos(sin(cos(rand()))))" );

        // Then
        assertThat( engine, equalTo( COMPILED ) );
    }

    @Test
    void shouldNotUseCompiledExpressionsWhenExplicitlyAskingForInterpreted()
    {
        // Given
        GraphDatabaseService db = withEngineAndLimit( "COMPILED", 42 );

        // When
        ExpressionEngine engine =
                engineUsedForQuery( db, "CYPHER expressionEngine=INTERPRETED RETURN sin(cos(sin(cos(rand()))))" );

        // Then
        assertThat( engine, equalTo( INTERPRETED ) );
    }


    private GraphDatabaseService withEngineAndLimit( String engine, int limit )
    {

        GraphDatabaseService db = new TestGraphDatabaseFactory().
                setInternalLogProvider( logProvider )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cypher_expression_engine, engine )
                .setConfig( GraphDatabaseSettings.cypher_expression_recompilation_limit, Integer.toString( limit ) )
                .newGraphDatabase();
        return db;
    }

    enum ExpressionEngine
    {
        COMPILED,
        INTERPRETED
    }

    private ExpressionEngine engineUsedForQuery( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        db.execute( query ).resultAsString();

        try
        {
            logProvider.assertAtLeastOnce(
                    AssertableLogProvider.inLog( EnterpriseCompilerFactory.class )
                            .debug( Matchers.<String>anyOf(
                                    containsString( "Compiling expression:" ),
                                    containsString( "Compiling projection:" )
                            ) ) );
            return ExpressionEngine.COMPILED;
        }
        catch ( AssertionError error )
        {
            return INTERPRETED;
        }

    }

}
