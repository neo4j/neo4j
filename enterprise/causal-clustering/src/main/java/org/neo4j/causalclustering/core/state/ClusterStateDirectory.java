/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;

/**
 * This represents the base directory for cluster state and contains
 * functionality capturing the migration paths.
 */
public class ClusterStateDirectory
{
    static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";

    private final File stateDir;
    private final File storeDir;
    private final boolean readOnly;

    private boolean initialized;

    public ClusterStateDirectory( File dataDir )
    {
        this( dataDir, null, true );
    }

    public ClusterStateDirectory( File dataDir, boolean readOnly )
    {
        this( dataDir, dataDir, readOnly );
    }

    public ClusterStateDirectory( File dataDir, File storeDir, boolean readOnly )
    {
        this.storeDir = storeDir;
        this.readOnly = readOnly;
        this.stateDir = new File( dataDir, CLUSTER_STATE_DIRECTORY_NAME );
    }

    /**
     * Returns true if the cluster state base directory exists or
     * could be created. This method also takes care of any necessary
     * migration.
     *
     * It is a requirement to initialize before using the class, unless
     * the non-migrating version is used.
     */
    public ClusterStateDirectory initialize( FileSystemAbstraction fs ) throws ClusterStateException
    {
        assert !initialized;
        if ( !readOnly )
        {
            migrateIfNeeded( fs );
        }
        ensureDirectoryExists( fs );
        initialized = true;
        return this;
    }

    /**
     * For use by special tooling which does not need the functionality
     * of migration or ensuring the directory for cluster state actually
     * exists.
     */
    public static ClusterStateDirectory withoutInitializing( File dataDir )
    {
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir );
        clusterStateDirectory.initialized = true;
        return clusterStateDirectory;
    }

    /**
     * The cluster state directory was previously badly placed under the
     * store directory, and this method takes care of the migration path from
     * that. It will now reside under the data directory.
     */
    private void migrateIfNeeded( FileSystemAbstraction fs ) throws ClusterStateException
    {
        File oldStateDir = new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME );
        if ( !fs.fileExists( oldStateDir ) || oldStateDir.equals( stateDir ) )
        {
            return;
        }

        if ( fs.fileExists( stateDir ) )
        {
            throw new ClusterStateException( "Cluster state exists in both old and new locations" );
        }

        try
        {
            fs.moveToDirectory( oldStateDir, stateDir.getParentFile() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to migrate cluster state directory", e );
        }
    }

    private void ensureDirectoryExists( FileSystemAbstraction fs ) throws ClusterStateException
    {
        if ( !fs.fileExists( stateDir ) )
        {
            if ( readOnly )
            {
                throw new ClusterStateException( "Cluster state directory does not exist" );
            }
            else
            {
                try
                {
                    fs.mkdirs( stateDir );
                }
                catch ( IOException e )
                {
                    throw new ClusterStateException( e );
                }
            }
        }
    }

    public File get()
    {
        assertInitialized();
        return stateDir;
    }

    public boolean isEmpty() throws IOException
    {
        assertInitialized();
        return FileUtils.isEmptyDirectory( stateDir );
    }

    public void clear( FileSystemAbstraction fs ) throws IOException, ClusterStateException
    {
        assertInitialized();
        fs.deleteRecursively( stateDir );
        ensureDirectoryExists( fs );
    }

    private void assertInitialized()
    {
        if ( !initialized )
        {
            throw new IllegalStateException( "Cluster state has not been initialized" );
        }
    }
}
