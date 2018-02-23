/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.scripting.javascript;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.server.rest.domain.EvaluationException;
import org.neo4j.server.rest.web.ScriptExecutionMode;
import org.neo4j.server.scripting.ScriptExecutor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;

class TestJavascriptSecurityRestrictions
{

    static void methodThatShouldNotBeCallable()
    {

    }

    @BeforeAll
    static void doBullshitGlobalStateCrap()
    {
        GlobalJavascriptInitializer.initialize( ScriptExecutionMode.SANDBOXED );
    }

    @Test
    void shouldBeAbleToAccessWhiteListedThings()
    {
        // Given
        String classThatShouldBeInaccessible = TestJavascriptSecurityRestrictions.class.getName();

        ScriptExecutor executor = new JavascriptExecutor(
                Evaluation.class.getName() + ".INCLUDE_AND_CONTINUE;" );

        // When
        Object result = executor.execute( null );

        // Then
        assertThat( result, is( instanceOf( Evaluation.class ) ) );
        assertThat( result, is(INCLUDE_AND_CONTINUE) );
    }

    @Test
    void shouldNotBeAbleToImportUnsafeClasses()
    {
        assertThrows( EvaluationException.class, () -> {
            // Given
            String classThatShouldBeInaccessible = TestJavascriptSecurityRestrictions.class.getName();

            ScriptExecutor executor =
                    new JavascriptExecutor( classThatShouldBeInaccessible + ".methodThatShouldNotBeCallable();" );

            // When
            executor.execute( null );
        } );
    }

    @Test
    void shouldNotBeAbleToUseReflectionToInstantiateThings()
    {
        assertThrows( EvaluationException.class, () -> {
            // Given
            ScriptExecutor executor =
                    new JavascriptExecutor( Evaluation.class.getName() + ".getClass().getClassLoader();" );

            // When
            executor.execute( null );
        } );
    }
}
