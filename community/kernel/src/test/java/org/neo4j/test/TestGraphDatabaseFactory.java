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
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

/**
 * Test factory for graph databases
 */
public class TestGraphDatabaseFactory extends GraphDatabaseFactory
{
    public TestGraphDatabaseFactory()
    {
       super( new TestGraphDatabaseFactoryState() );
    }

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

    @Override
    protected TestGraphDatabaseFactoryState getCurrentState()
    {
        return (TestGraphDatabaseFactoryState) super.getCurrentState();
    }

    @Override
    protected TestGraphDatabaseFactoryState getStateCopy()
    {
        return new TestGraphDatabaseFactoryState( getCurrentState() );
    }

    public FileSystemAbstraction getFileSystem()
    {
        return getCurrentState().getFileSystem();
    }

    public TestGraphDatabaseFactory setFileSystem( FileSystemAbstraction fileSystem )
    {
        getCurrentState().setFileSystem( fileSystem );
        return this;
    }

    public boolean isSystemOutLogging()
    {
        return getCurrentState().isSystemOutLogging();
    }

    public TestGraphDatabaseFactory setSystemOutLogging( boolean systemOutLogging )
    {
        getCurrentState().setSystemOutLogging( systemOutLogging );
        return this;
    }

    public GraphDatabaseBuilder newImpermanentDatabaseBuilder( final String storeDir )
    {
        final TestGraphDatabaseFactoryState state = getStateCopy();
        return new TestGraphDatabaseBuilder( new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            @SuppressWarnings("deprecation")
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                return new ImpermanentGraphDatabase( storeDir, config,
                        state.getIndexProviders(),
                        state.getKernelExtension(),
                        state.getCacheProviders(),
                        state.getTransactionInterceptorProviders() )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        FileSystemAbstraction fs = state.getFileSystem();
                        if ( fs != null )
                            return fs;
                        else
                            return super.createFileSystemAbstraction();
                    }

                    @Override
                    protected Logging createLogging()
                    {
                        boolean systemOutLogging = state.isSystemOutLogging();
                        if ( systemOutLogging )
                            return new SingleLoggingService( StringLogger.SYSTEM );
                        else
                            return super.createLogging();
                    }
                };
            }
        } );
    }
}
