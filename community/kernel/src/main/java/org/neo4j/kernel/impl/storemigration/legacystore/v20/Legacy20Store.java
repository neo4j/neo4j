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
package org.neo4j.kernel.impl.storemigration.legacystore.v20;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;

/**
 * Reader for a database in an older store format version.
 *
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the reader code is specific for the current upgrade and changes with each store format version.
 *
 * {@link #LEGACY_VERSION} marks which version it's able to read.
 */
public class Legacy20Store implements LegacyStore
{
    public static final String LEGACY_VERSION = "v0.A.1";

    private final File storageFileName;
    private final Collection<Closeable> allStoreReaders = new ArrayList<>();
    private LegacyNodeStoreReader nodeStoreReader;
    private LegacyRelationshipStoreReader relStoreReader;

    private final FileSystemAbstraction fs;

    public Legacy20Store( FileSystemAbstraction fs, File storageFileName ) throws IOException
    {
        this.fs = fs;
        this.storageFileName = storageFileName;
        assertLegacyAndCurrentVersionHaveSameLength( LEGACY_VERSION, CommonAbstractStore.ALL_STORES_VERSION );
        initStorage();
    }

    /**
     * Store files that don't need migration are just copied and have their trailing versions replaced
     * by the current version. For this to work the legacy version and the current version must have the
     * same encoded length.
     */
    static void assertLegacyAndCurrentVersionHaveSameLength( String legacyVersion, String currentVersion )
    {
        if ( UTF8.encode( legacyVersion ).length != UTF8.encode( currentVersion ).length )
        {
            throw new IllegalStateException( "Encoded version string length must remain the same between versions" );
        }
    }

    protected void initStorage() throws IOException
    {
        allStoreReaders.add( nodeStoreReader = new Legacy20NodeStoreReader( fs,
                new File( getStorageFileName().getPath() + StoreFactory.NODE_STORE_NAME ) ) );
        allStoreReaders.add( relStoreReader = new Legacy20RelationshipStoreReader( fs,
                new File( getStorageFileName().getPath() + StoreFactory.RELATIONSHIP_STORE_NAME ) ) );
    }

    @Override
    public File getStorageFileName()
    {
        return storageFileName;
    }

    public static long getUnsignedInt( ByteBuffer buf )
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }

    @Override
    public void close() throws IOException
    {
        for ( Closeable storeReader : allStoreReaders )
        {
            storeReader.close();
        }
    }

    @Override
    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }

    @Override
    public LegacyRelationshipStoreReader getRelStoreReader()
    {
        return relStoreReader;
    }

    static void readIntoBuffer( StoreChannel fileChannel, ByteBuffer buffer, int nrOfBytes )
    {
        buffer.clear();
        buffer.limit( nrOfBytes );
        try
        {
            fileChannel.read( buffer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        buffer.flip();
    }
}
