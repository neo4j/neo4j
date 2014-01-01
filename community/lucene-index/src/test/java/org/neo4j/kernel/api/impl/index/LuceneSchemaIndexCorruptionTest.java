/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Test;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

public class LuceneSchemaIndexCorruptionTest
{

    @Test
    public void shouldMarkIndexAsFailedIfIndexIsCorrupt() throws Exception
    {
        // Given
        DirectoryFactory dirFactory = mock(DirectoryFactory.class);

        // This isn't quite correct, but it will trigger the correct code paths in our code
        when(dirFactory.open( any(File.class) )).thenThrow(new CorruptIndexException( "It's borken." ));

        LuceneSchemaIndexProvider p = new LuceneSchemaIndexProvider( dirFactory,
                new Config( stringMap( "store_dir", forTest( getClass() ).graphDbDir( true ).getAbsolutePath() ) ) );

        // When
        InternalIndexState initialState = p.getInitialState( 1l );

        // Then
        assertThat( initialState, equalTo(InternalIndexState.FAILED) );
    }

}
