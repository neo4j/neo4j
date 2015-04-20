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
package org.neo4j.test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Test factory for graph databases
 */
public class TestGraphDatabaseFactory extends GraphDatabaseFactory
{
    public TestGraphDatabaseFactory()
    {
       super( new TestGraphDatabaseFactoryState() );
    }

    public TestGraphDatabaseFactory( Logging logging )
    {
        super( new TestGraphDatabaseFactoryState() );
        setLogging( logging );
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
    protected void configure( GraphDatabaseBuilder builder )
    {
        super.configure( builder );
        // Reduce the default page cache memory size to 8 mega-bytes for test databases.
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
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

    public GraphDatabaseFactory setMonitors( Monitors monitors )
    {
        getCurrentState().setMonitors( monitors );
        return this;
    }
    
    @Override
    public TestGraphDatabaseFactory setLogging( Logging logging )
    {
        getCurrentState().setLogging( logging );
        return this;
    }

    @Override
    public TestGraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        return (TestGraphDatabaseFactory) super.addKernelExtensions( newKernelExtensions );
    }

    @Override
    public TestGraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        return (TestGraphDatabaseFactory) super.addKernelExtension( newKernelExtension );
    }

    public GraphDatabaseBuilder newImpermanentDatabaseBuilder( final String storeDir )
    {
        final TestGraphDatabaseFactoryState state = getStateCopy();
        GraphDatabaseBuilder.DatabaseCreator creator =
                createImpermanentDatabaseCreator( storeDir, state );
        TestGraphDatabaseBuilder builder = createImpermanentGraphDatabaseBuilder( creator );
        configure( builder );
        return builder;
    }

    protected TestGraphDatabaseBuilder createImpermanentGraphDatabaseBuilder(
            GraphDatabaseBuilder.DatabaseCreator creator )
    {
        return new TestGraphDatabaseBuilder( creator );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final String storeDir,
                                                                                     final TestGraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            @SuppressWarnings("deprecation")
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                return new ImpermanentGraphDatabase( storeDir, config, state.databaseDependencies() )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        FileSystemAbstraction fs = state.getFileSystem();
                        if ( fs != null )
                        {
                            return fs;
                        }
                        else
                        {
                            return super.createFileSystemAbstraction();
                        }
                    }
                };
            }
        };
    }
}
