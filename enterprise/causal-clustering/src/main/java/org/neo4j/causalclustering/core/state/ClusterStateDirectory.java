/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

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
        if ( !initialized )
        {
            throw new IllegalStateException( "Cluster state has not been initialized" );
        }
        return stateDir;
    }
}
