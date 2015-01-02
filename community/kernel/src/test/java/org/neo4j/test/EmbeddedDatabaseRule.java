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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;

import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * JUnit @Rule for configuring, creating and managing an EmbeddedGraphDatabase instance.
 */
public class EmbeddedDatabaseRule extends DatabaseRule
{
    private final TempDirectory temp;
    
    public EmbeddedDatabaseRule()
    {
        this.temp = new TempDirectory()
        {
            private final TemporaryFolder folder = new TemporaryFolder();
            
            @Override
            public File root()
            {
                return folder.getRoot();
            }
            
            @Override
            public void delete()
            {
                folder.delete();
            }
            
            @Override
            public void create() throws IOException
            {
                folder.create();
            }
        };
    }
    
    public EmbeddedDatabaseRule( final Class<?> testClass )
    {
        this.temp = new TempDirectory()
        {
            private final TargetDirectory targetDirectory = TargetDirectory.forTest( testClass );
            private File dbDir;

            @Override
            public File root()
            {
                return dbDir;
            }
            
            @Override
            public void delete() throws IOException
            {
                targetDirectory.cleanup();
            }
            
            @Override
            public void create()
            {
                dbDir = targetDirectory.makeGraphDbDir();
            }
        };
    }
    
    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return new GraphDatabaseFactory();
    }
    
    @Override
    protected GraphDatabaseBuilder newBuilder(GraphDatabaseFactory factory )
    {
        return factory.newEmbeddedDatabaseBuilder( temp.root().getAbsolutePath() );
    }

    @Override
    protected void createResources() throws IOException
    {
        temp.create();
    }
    
    @Override
    protected void deleteResources()
    {
        try
        {
            temp.delete();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private interface TempDirectory
    {
        File root();
        
        void create() throws IOException;
        
        void delete() throws IOException;
    }
}
