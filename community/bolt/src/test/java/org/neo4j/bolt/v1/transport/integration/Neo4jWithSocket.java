/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;

public class Neo4jWithSocket extends ExternalResource
{
    public static final String DEFAULT_CONNECTOR_KEY = "bolt";

    private Supplier<FileSystemAbstraction> fileSystemProvider;
    private final Consumer<Map<String,String>> configure;
    private final TestDirectory testDirectory;
    private TestGraphDatabaseFactory graphDatabaseFactory;
    private GraphDatabaseService gdb;
    private File workingDirectory;
    private ConnectorPortRegister connectorRegister;

    public Neo4jWithSocket( Class<?> testClass )
    {
        this( testClass, settings ->
        {
        } );
    }

    public Neo4jWithSocket( Class<?> testClass, Consumer<Map<String,String>> configure )
    {
        this( testClass, new TestGraphDatabaseFactory(), configure );
    }

    public Neo4jWithSocket( Class<?> testClass, TestGraphDatabaseFactory graphDatabaseFactory,
            Consumer<Map<String,String>> configure )
    {
        this( testClass, graphDatabaseFactory, EphemeralFileSystemAbstraction::new, configure );
    }

    public Neo4jWithSocket( Class<?> testClass, TestGraphDatabaseFactory graphDatabaseFactory,
            Supplier<FileSystemAbstraction> fileSystemProvider, Consumer<Map<String,String>> configure )
    {
        this.testDirectory = TestDirectory.testDirectory( testClass, fileSystemProvider.get() );
        this.graphDatabaseFactory = graphDatabaseFactory;
        this.fileSystemProvider = fileSystemProvider;
        this.configure = configure;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return this.graphDatabaseFactory.getFileSystem();
    }

    public File getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public Statement apply( final Statement statement, final Description description )
    {
        Statement testMethod = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // If this is used as class rule then getMethodName() returns null, so use
                // getClassName() instead.
                String name =
                        description.getMethodName() != null ? description.getMethodName() : description.getClassName();
                workingDirectory = testDirectory.directory( name );
                ensureDatabase( settings -> {} );
                try
                {
                    statement.evaluate();
                }
                finally
                {
                    shutdownDatabase();
                }
            }
        };

        Statement testMethodWithBeforeAndAfter = super.apply( testMethod, description );

        return testDirectory.apply( testMethodWithBeforeAndAfter, description );
    }

    public HostnamePort lookupConnector( String connectorKey )
    {
        return connectorRegister.getLocalAddress( connectorKey );
    }

    public HostnamePort lookupDefaultConnector()
    {
        return connectorRegister.getLocalAddress( DEFAULT_CONNECTOR_KEY );
    }

    public void shutdownDatabase()
    {
        try
        {
            if ( gdb != null )
            {
                gdb.shutdown();
            }
        }
        finally
        {
            connectorRegister = null;
            gdb = null;
        }
    }

    public void ensureDatabase( Consumer<Map<String,String>> overrideSettingsFunction )
    {
        if ( gdb != null )
        {
            return;
        }

        Map<String,String> settings = configure( overrideSettingsFunction );
        File storeDir = new File( workingDirectory, "storeDir" );
        graphDatabaseFactory.setFileSystem( fileSystemProvider.get() );
        gdb = graphDatabaseFactory.newImpermanentDatabaseBuilder( storeDir ).
                setConfig( settings ).newGraphDatabase();
        connectorRegister =
                ((GraphDatabaseAPI) gdb).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }

    private Map<String,String> configure( Consumer<Map<String,String>> overrideSettingsFunction )
    {
        Map<String,String> settings = new HashMap<>();
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).type.name(), "BOLT" );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).enabled.name(), "true" );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).encryption_level.name(), OPTIONAL.name() );
        configure.accept( settings );
        overrideSettingsFunction.accept( settings );
        return settings;
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return gdb;
    }
}
