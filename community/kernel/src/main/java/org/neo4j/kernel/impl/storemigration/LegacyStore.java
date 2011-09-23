/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LegacyStore
{
    static final String FROM_VERSION = "NodeStore v0.9.9";
    private String storageFileName;
    private LegacyPropertyStoreReader propertyStoreReader;
    private LegacyNodeStoreReader legacyNodeStoreReader;
    private LegacyDynamicRecordFetcher dynamicRecordFetcher;

    public LegacyStore( String storageFileName ) throws IOException
    {
        this.storageFileName = storageFileName;
        initStorage();
    }

    protected void initStorage() throws IOException
    {
//        relTypeStore = new RelationshipTypeStore( getStorageFileName()
//            + ".relationshiptypestore.db", getConfig(), IdType.RELATIONSHIP_TYPE );
        propertyStoreReader = new LegacyPropertyStoreReader( getStorageFileName() + ".propertystore.db" );
        dynamicRecordFetcher = new LegacyDynamicRecordFetcher( getStorageFileName() + ".propertystore.db.strings", getStorageFileName() + ".propertystore.db.arrays" );
//        relStore = new RelationshipStore( getStorageFileName()
//            + ".relationshipstore.db", getConfig() );
        legacyNodeStoreReader = new LegacyNodeStoreReader( getStorageFileName() + ".nodestore.db" );
    }

    public String getStorageFileName()
    {
        return storageFileName;
    }

    public LegacyPropertyStoreReader getPropertyStoreReader()
    {
        return propertyStoreReader;
    }

    public LegacyNodeStoreReader getLegacyNodeStoreReader()
    {
        return legacyNodeStoreReader;
    }

    public LegacyDynamicRecordFetcher getDynamicRecordFetcher()
    {
        return dynamicRecordFetcher;
    }

    public static long getUnsignedInt(ByteBuffer buf)
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }
}
