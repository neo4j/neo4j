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
package org.neo4j.io.fs;

import java.io.File;

import org.junit.Test;

import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SelectiveFileSystemAbstractionTest
{
    @Test
    public void shouldUseCorrectFileSystemForChosenFile() throws Exception
    {
        // given
        File specialFile = new File("special");
        FileSystemAbstraction normal = mock( FileSystemAbstraction.class );
        FileSystemAbstraction special = mock( FileSystemAbstraction.class );

        // when
        new SelectiveFileSystemAbstraction( specialFile, special, normal ).open( specialFile, "r" );

        // then
        verify( special).open( specialFile, "r" ) ;
        verifyNoMoreInteractions( special );
        verifyNoMoreInteractions( normal );
    }

    @Test
    public void shouldUseDefaultFileSystemForOtherFiles() throws Exception
    {
        // given
        File specialFile = new File("special");
        File otherFile = new File("other");

        FileSystemAbstraction normal = mock( FileSystemAbstraction.class );
        FileSystemAbstraction special = mock( FileSystemAbstraction.class );

        // when
        SelectiveFileSystemAbstraction fs = new SelectiveFileSystemAbstraction( specialFile, special, normal );
        fs.create( otherFile );
        fs.open( otherFile, "r" );

        // then
        verify( normal ).create( otherFile ) ;
        verify( normal ).open( otherFile, "r" ) ;
        verifyNoMoreInteractions( special );
        verifyNoMoreInteractions( normal );
    }
}
