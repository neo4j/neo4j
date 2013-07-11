/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

public class WindowsLaunch4jIniValueTest
{
    @Test
    public void shouldReadAndWriteHeapSize() throws Exception
    {
        // GIVEN
        Environment environment = mock( Environment.class );
        when( environment.getAppFile() ).thenReturn( new File( dir, "Test.exe" ) );
        when( environment.isRunByApp() ).thenReturn( true );
        ListIO listIo = new InMemoryListIo( asList( "-Xmx123M" ) );
        Value<Integer> value = new WindowsLaunch4jIniValue( environment, listIo );
        
        // WHEN
        int initialValue = value.get();
        value.set( 456 );
        int secondValue = value.get();
        
        // THEN
        assertEquals( 123, initialValue );
        assertEquals( 456, secondValue );
    }

    private final File dir = new File( "target/test-data/ini" );
    
    @Before
    public void before() throws Exception
    {
        deleteRecursively( dir );
        dir.mkdirs();
    }

    @After
    public void after() throws Exception
    {
    }
    
    private static class InMemoryListIo implements ListIO
    {
        private final List<String> items = new ArrayList<String>();
        
        public InMemoryListIo( List<String> initialItems )
        {
            items.addAll( initialItems );
        }

        @Override
        public List<String> read( List<String> target, File file ) throws IOException
        {
            target.addAll( items );
            return target;
        }

        @Override
        public void write( List<String> list, File file ) throws IOException
        {
            items.clear();
            items.addAll( list );
        }
    }
}
