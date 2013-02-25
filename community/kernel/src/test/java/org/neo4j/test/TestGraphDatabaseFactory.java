/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

/**
 * Test factory for graph databases
 */
public class TestGraphDatabaseFactory
    extends GraphDatabaseFactory
{
    public GraphDatabaseService newImpermanentDatabase()
    {
        return newImpermanentDatabaseBuilder().newGraphDatabase();
    }
    
    public GraphDatabaseService newImpermanentDatabase( String storeDir )
    {
        return newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
    }
    
    public GraphDatabaseBuilder newImpermanentDatabaseBuilder()
    {
        return newImpermanentDatabaseBuilder( ImpermanentGraphDatabase.PATH );
    }
    
    public GraphDatabaseBuilder newImpermanentDatabaseBuilder( final String storeDir )
    {
        return new TestGraphDatabaseBuilder( new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                return new ImpermanentGraphDatabase( storeDir, config, indexProviders, kernelExtensions,
                        cacheProviders, txInterceptorProviders, schemaIndexProviders )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        if ( TestGraphDatabaseFactory.this.fileSystem != null )
                            return TestGraphDatabaseFactory.this.fileSystem;
                        else
                            return super.createFileSystemAbstraction();
                    }
                };
            }
        } );
    }
    
    public TestGraphDatabaseFactory setFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        return this;
    }
}
