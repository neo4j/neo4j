/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.kernel.Config;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStore doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStore extends AbstractStore
{
    // neo store version, store should end with this string
    // (byte encoded)
    private static final String VERSION = "NeoStore v0.9.6";

    // 3 longs in header (long + in use), time | random | version
    private static final int RECORD_SIZE = 9;

    private NodeStore nodeStore;
    private PropertyStore propStore;
    private RelationshipStore relStore;
    private RelationshipTypeStore relTypeStore;
    
    private final int REL_GRAB_SIZE;

    public NeoStore( Map<?,?> config )
    {
        super( (String) config.get( "neo_store" ), config );
        if ( getConfig() != null )
        {
            String grabSize = (String) getConfig().get( "relationship_grab_size" );
            if ( grabSize != null )
            {
                REL_GRAB_SIZE = Integer.parseInt( grabSize ); 
            }
            else
            {
                REL_GRAB_SIZE = 100;
            }
        }
        else
        {
            REL_GRAB_SIZE = 100;
        }
    }

    public NeoStore( String fileName )
    {
        super( fileName );
        REL_GRAB_SIZE = 100;
    }

    /**
     * Initializes the node,relationship,property and relationship type stores.
     */
    @Override
    protected void initStorage()
    {
        relTypeStore = new RelationshipTypeStore( getStorageFileName()
            + ".relationshiptypestore.db", getConfig() );
        propStore = new PropertyStore( getStorageFileName()
            + ".propertystore.db", getConfig() );
        relStore = new RelationshipStore( getStorageFileName()
            + ".relationshipstore.db", getConfig() );
        nodeStore = new NodeStore( getStorageFileName() + ".nodestore.db",
            getConfig() );
    }
    
    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    @Override
    protected void closeStorage()
    {
        if ( relTypeStore != null )
        {
            relTypeStore.close();
            relTypeStore = null;
        }
        if ( propStore != null )
        {
            propStore.close();
            propStore = null;
        }
        if ( relStore != null )
        {
            relStore.close();
            relStore = null;
        }
        if ( nodeStore != null )
        {
            nodeStore.close();
            nodeStore = null;
        }
    }

    public void flushAll()
    {
        if ( relTypeStore == null || propStore == null || relStore == null || 
                nodeStore == null )
        {
            return;
        }
        relTypeStore.flushAll();
        propStore.flushAll();
        relStore.flushAll();
        nodeStore.flushAll();
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    private static final Random r = new Random( System.currentTimeMillis() );
    
    /**
     * Creates the neo,node,relationship,property and relationship type stores.
     * 
     * @param fileName
     *            The name of store
     * @throws IOException
     *             If unable to create stores or name null
     */
    public static void createStore( String fileName, Map<?,?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) Config.getFromConfig( config,
                IdGeneratorFactory.class, IdGeneratorFactory.DEFAULT );
                
        createEmptyStore( fileName, VERSION, idGeneratorFactory );
        NodeStore.createStore( fileName + ".nodestore.db", idGeneratorFactory );
        RelationshipStore.createStore( fileName + ".relationshipstore.db", idGeneratorFactory );
        PropertyStore.createStore( fileName + ".propertystore.db", config );
        RelationshipTypeStore.createStore( fileName
            + ".relationshiptypestore.db", idGeneratorFactory );
        NeoStore neoStore = new NeoStore( fileName );
        // created time | random long | backup version
        neoStore.nextId(); neoStore.nextId(); neoStore.nextId();
        long time = System.currentTimeMillis();
        neoStore.setCreationTime( time );
        neoStore.setRandomNumber( r.nextLong() );
        neoStore.setVersion( 0 );
        neoStore.setLastCommittedTx( 1 );
        neoStore.close();
    }
    
    public long getCreationTime()
    {
        return getRecord( 0 );
    }
    
    public void setCreationTime( long time )
    {
        setRecord( 0, time );
    }
    
    public long getRandomNumber()
    {
        return getRecord( 1 );
    }
    
    public void setRandomNumber( long nr )
    {
        setRecord( 1, nr );
    }
    
    public void setRecoveredStatus( boolean status )
    {
        if ( status )
        {
            setRecovered();
        }
        else
        {
            unsetRecovered();
        }
    }
    
    public long getVersion()
    {
        return getRecord( 2 );
    }
    
    public void setVersion( long version )
    {
        setRecord( 2, version );
    }
    
    public synchronized void setLastCommittedTx( long txId )
    {
        long current = getRecord( 3 );
        if ( (current + 1) != txId && !isInRecoveryMode() )
        {
            throw new InvalidRecordException( "Could not set tx commit id[" +  
                txId + "] since the current one is[" + current + "]" );
        }
        setRecord( 3, txId );
    }
    
    public long getNextCommitId()
    {
        return getRecord( 3 ) + 1;
    }
    
    public long getLastCommittedTx()
    {
        return getRecord( 3 );
    }
    
    public long incrementVersion()
    {
        long current = getVersion();
        setVersion( current + 1 );
        return current;
    }
    
    private long getRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.get();
            return buffer.getLong();
        }
        finally
        {
            releaseWindow( window );
        }
    }
    
    private void setRecord( int id, long value )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.WRITE );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.put( Record.IN_USE.byteValue() ).putLong( value );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    /**
     * Returns the node store.
     * 
     * @return The node store
     */
    public NodeStore getNodeStore()
    {
        return nodeStore;
    }

    /**
     * The relationship store.
     * 
     * @return The relationship store
     */
    public RelationshipStore getRelationshipStore()
    {
        return relStore;
    }

    /**
     * Returns the relationship type store.
     * 
     * @return The relationship type store
     */
    public RelationshipTypeStore getRelationshipTypeStore()
    {
        return relTypeStore;
    }

    /**
     * Returns the property store.
     * 
     * @return The property store
     */
    public PropertyStore getPropertyStore()
    {
        return propStore;
    }

    @Override
    public void makeStoreOk()
    {
        relTypeStore.makeStoreOk();
        propStore.makeStoreOk();
        relStore.makeStoreOk();
        nodeStore.makeStoreOk();
        super.makeStoreOk();
    }
    
    
    public void rebuildIdGenerators()
    {
        relTypeStore.rebuildIdGenerators();
        propStore.rebuildIdGenerators();
        relStore.rebuildIdGenerators();
        nodeStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }

    public void updateIdGenerators()
    {
        this.updateHighId();
        relTypeStore.updateIdGenerators();
        propStore.updateIdGenerators();
        relStore.updateHighId();
        nodeStore.updateHighId();
    }
    
    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "NeoStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
        if ( version.equals( "NeoStore v0.9.5" ) )
        {
            ByteBuffer buffer = ByteBuffer.wrap( new byte[ 3 * RECORD_SIZE ] );
            long time = System.currentTimeMillis();
            long random = r.nextLong();
            buffer.put( Record.IN_USE.byteValue() ).putLong( time );
            buffer.put( Record.IN_USE.byteValue() ).putLong( random );
            buffer.put( Record.IN_USE.byteValue() ).putLong( 0 );
            buffer.put( Record.IN_USE.byteValue() ).putLong( 0 );
            buffer.flip();
            try
            {
                getFileChannel().write( buffer, 0 );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            rebuildIdGenerator();
            closeIdGenerator();
            return true;
        }
        throw new IllegalStoreVersionException( "Store version [" + version  + 
            "]. Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }

    public int getRelationshipGrabSize()
    {
        return REL_GRAB_SIZE;
    }
    
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.addAll( nodeStore.getAllWindowPoolStats() );
        list.addAll( propStore.getAllWindowPoolStats() );
        list.addAll( relStore.getAllWindowPoolStats() );
        list.addAll( relTypeStore.getAllWindowPoolStats() );
        return list;
    }
    
    @Override
    protected IdType getIdType()
    {
        return IdType.NEOSTORE_BLOCK;
    }
}