/**
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;

import org.junit.Test;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Outcome;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CurrentDatabaseTest
{
    @Test
    public void shouldRejectStoreWhereOneFileHasTheWrongVersion() throws Exception
    {
        File workingDirectory = new File( "target/" + CurrentDatabaseTest.class.getSimpleName() );

        StoreVersionCheck storeVersionCheck = mock( StoreVersionCheck.class );
        when( storeVersionCheck.hasVersion( eq( new File( workingDirectory, "neostore.nodestore.db" ) ), anyString() ) )
                .thenReturn( Pair.<Outcome,String>of( Outcome.missingStoreFile, null ) );
        when( storeVersionCheck.hasVersion( not( eq( new File( workingDirectory, "neostore.nodestore.db" ) ) ),
                anyString() ) )
                .thenReturn( Pair.<Outcome,String>of( Outcome.ok, null ) );

        assertFalse( new CurrentDatabase( storeVersionCheck ).storeFilesAtCurrentVersion( workingDirectory ) );
    }

    @Test
    public void shouldAcceptStoreWhenAllFilesHaveTheCorrectVersion()
    {
        File workingDirectory = new File( "target/" + CurrentDatabaseTest.class.getSimpleName() );

        StoreVersionCheck storeVersionCheck = mock( StoreVersionCheck.class );
        when( storeVersionCheck.hasVersion( any( File.class ), anyString() ) ).thenReturn(
                Pair.<Outcome,String>of( Outcome.ok, null ) );

        assertTrue( new CurrentDatabase( storeVersionCheck ).storeFilesAtCurrentVersion( workingDirectory ) );
    }
}
