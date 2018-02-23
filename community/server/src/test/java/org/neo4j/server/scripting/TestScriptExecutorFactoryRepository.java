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
package org.neo4j.server.scripting;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.server.scripting.ScriptExecutor.Factory;

public class TestScriptExecutorFactoryRepository
{

    @Test
    public void shouldThrowNoSuchScriptLanguageExceptionForUnkownLanguages()
    {
        assertThrows( NoSuchScriptLanguageException.class, () -> {
            // Given
            ScriptExecutorFactoryRepository repo = new ScriptExecutorFactoryRepository( new HashMap<>() );

            // When
            repo.getFactory( "Blah" );
        } );
    }

    @Test
    public void shouldReturnRegisteredFactory()
    {
        // Given
        Map<String, ScriptExecutor.Factory> languages = new HashMap<>();
        languages.put( "js", mock(ScriptExecutor.Factory.class) );

        ScriptExecutorFactoryRepository repo = new ScriptExecutorFactoryRepository( languages );

        // When
        Factory factory = repo.getFactory( "js" );

        // Then
        assertThat(factory, not(nullValue()));
    }

}
