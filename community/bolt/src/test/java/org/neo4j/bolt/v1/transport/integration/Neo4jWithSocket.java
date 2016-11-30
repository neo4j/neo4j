/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport.integration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;

public class Neo4jWithSocket extends ExternalResource
{
    private Supplier<FileSystemAbstraction> fileSystemProvider;
    private final Consumer<Map<String,String>> configure;
    private final TestDirectory testDirectory;
    private TestGraphDatabaseFactory graphDatabaseFactory;
    private GraphDatabaseService gdb;
    private File workingDirectory;

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

    public void shutdownDatabase()
    {
        try
        {
            if ( gdb != null)
            {
                gdb.shutdown();
            }
        }
        finally
        {
            gdb = null;
        }
    }

    public void ensureDatabase( Consumer<Map<String,String>> overrideSettingsFunction ) throws IOException
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
    }

    private Map<String,String> configure( Consumer<Map<String,String>> overrideSettingsFunction ) throws IOException
    {
        Map<String,String> settings = new HashMap<>();
        settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "bolt" ).encryption_level.name(), OPTIONAL.name() );
        settings.put( BoltKernelExtension.Settings.tls_key_file.name(), tempPath( "key.key" ) );
        settings.put( BoltKernelExtension.Settings.tls_certificate_file.name(), tempPath( "cert.cert" ) );
        configure.accept( settings );
        overrideSettingsFunction.accept( settings );
        return settings;
    }

    private String tempPath( String filename ) throws IOException
    {
        File file = new File( new File( workingDirectory, "security" ), filename );
        file.deleteOnExit();
        return file.getAbsolutePath();
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return gdb;
    }
}
