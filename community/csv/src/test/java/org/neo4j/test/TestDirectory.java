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
package org.neo4j.test;

import java.io.File;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TestDirectory implements TestRule
{
    private File directory;

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                directory = new File( new File( new File( new File( "target" ), "test-data" ),
                                                description.getClassName() ), description.getMethodName() );
                delete( directory );
                directory.mkdirs();
                boolean success = false;
                try
                {
                    base.evaluate();
                    success = true;
                }
                finally
                {
                    if ( success )
                    {
                        delete( directory );
                        delete( directory.getParentFile() ); // works if it's empty
                    }
                }
            }
        };
    }

    private void clear( File file )
    {
        if ( file.exists() )
        {
            for ( File child : file.listFiles() )
            {
                delete( child );
            }
        }
    }

    private void delete( File child )
    {
        if ( child.isDirectory() )
        {
            clear( child );
        }
        child.delete();
    }

    public File directory()
    {
        return directory;
    }

    public String getAbsolutePath()
    {
        return directory.getAbsolutePath();
    }

    public File directory( String name )
    {
        File result = new File( directory, name );
        result.mkdirs();
        return result;
    }

    public File file( String name )
    {
        return new File( directory, name );
    }
}
