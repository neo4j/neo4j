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
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

public class Launch4jIniValueTest
{
    @Test
    public void shouldReadAndWriteHeapSize() throws Exception
    {
        // GIVEN
        Environment environment = new Environment()
        {
            @Override
            public boolean isRunByApp()
            {
                return true;
            }

            @Override
            public File getAppFile()
            {
                return new File( dir, "Test.exe" );
            }
        };
        storeInIniFile( "-Xmx123M", new File( dir, "Test.l4j.ini" ) );
        Value<Integer> value = new Launch4jIniValue( environment );
        
        // WHEN
        int initialValue = value.get();
        value.set( 456 );
        int secondValue = value.get();
        
        // THEN
        assertEquals( 123, initialValue );
        assertEquals( 456, secondValue );
    }
    
    private void storeInIniFile( String string, File file ) throws FileNotFoundException
    {
        PrintStream out = new PrintStream( file );
        out.println( string );
        out.close();
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
}
