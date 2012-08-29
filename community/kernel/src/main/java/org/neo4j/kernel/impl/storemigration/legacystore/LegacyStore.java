/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.util.StringLogger;

public class LegacyStore
{
    public static final String FROM_VERSION = "NeoStore v0.9.9";

    private String storageFileName;
    private LegacyNeoStoreReader neoStoreReader;
    private LegacyPropertyStoreReader propertyStoreReader;
    private LegacyNodeStoreReader nodeStoreReader;
    private LegacyDynamicRecordFetcher dynamicRecordFetcher;
    private LegacyPropertyIndexStoreReader propertyIndexStoreReader;
    private LegacyDynamicStoreReader propertyIndexKeyStoreReader;
    private LegacyRelationshipStoreReader relationshipStoreReader;
    private LegacyRelationshipTypeStoreReader relationshipTypeStoreReader;
    private LegacyDynamicStoreReader relationshipTypeNameStoreReader;
    private StringLogger log;

    public LegacyStore( String storageFileName ) throws IOException
    {
        this( storageFileName, StringLogger.DEV_NULL );
    }

    public LegacyStore( String storageFileName, StringLogger log ) throws IOException
    {
        this.storageFileName = storageFileName;
        this.log = log;
        initStorage();
    }

    protected void initStorage() throws IOException
    {
        neoStoreReader = new LegacyNeoStoreReader( getStorageFileName(), log );
        propertyStoreReader = new LegacyPropertyStoreReader( getStorageFileName() + ".propertystore.db", log );
        dynamicRecordFetcher = new LegacyDynamicRecordFetcher( getStorageFileName() + ".propertystore.db.strings", getStorageFileName() + ".propertystore.db.arrays", log );
        nodeStoreReader = new LegacyNodeStoreReader( getStorageFileName() + ".nodestore.db" );
        relationshipStoreReader = new LegacyRelationshipStoreReader( getStorageFileName() + ".relationshipstore.db" );
        relationshipTypeStoreReader = new LegacyRelationshipTypeStoreReader( getStorageFileName() + ".relationshiptypestore.db" );
        relationshipTypeNameStoreReader = new LegacyDynamicStoreReader( getStorageFileName() + ".relationshiptypestore.db.names", LegacyDynamicStoreReader.FROM_VERSION_STRING, log );
        propertyIndexStoreReader = new LegacyPropertyIndexStoreReader( getStorageFileName() + ".propertystore.db.index" );
        propertyIndexKeyStoreReader = new LegacyDynamicStoreReader( getStorageFileName() + ".propertystore.db.index.keys", LegacyDynamicStoreReader.FROM_VERSION_STRING, log );
    }

    public String getStorageFileName()
    {
        return storageFileName;
    }

    public LegacyNeoStoreReader getNeoStoreReader()
    {
        return neoStoreReader;
    }

    public LegacyPropertyStoreReader getPropertyStoreReader()
    {
        return propertyStoreReader;
    }

    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }

    public LegacyRelationshipStoreReader getRelationshipStoreReader()
    {
        return relationshipStoreReader;
    }

    public LegacyDynamicRecordFetcher getDynamicRecordFetcher()
    {
        return dynamicRecordFetcher;
    }

    public LegacyPropertyIndexStoreReader getPropertyIndexStoreReader()
    {
        return propertyIndexStoreReader;
    }

    public LegacyDynamicStoreReader getPropertyIndexKeyStoreReader()
    {
        return propertyIndexKeyStoreReader;
    }

    public LegacyDynamicStoreReader getRelationshipTypeNameStoreReader()
    {
        return relationshipTypeNameStoreReader;
    }

    public static long getUnsignedInt(ByteBuffer buf)
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }

    public LegacyRelationshipTypeStoreReader getRelationshipTypeStoreReader()
    {
        return relationshipTypeStoreReader;
    }

    public void close() throws IOException
    {
        neoStoreReader.close();
        propertyStoreReader.close();
        dynamicRecordFetcher.close();
        nodeStoreReader.close();
        relationshipStoreReader.close();
        relationshipTypeNameStoreReader.close();
        propertyIndexKeyStoreReader.close();
    }
}
