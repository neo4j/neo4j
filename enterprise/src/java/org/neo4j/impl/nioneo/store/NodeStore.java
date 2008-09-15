/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractStore implements Store
{
    // node store version, each node store should end with this string
    // (byte encoded)
    private static final String VERSION = "NodeStore v0.9.3";

    // in_use(byte)+next_rel_id(int)+next_prop_id(int)
    private static final int RECORD_SIZE = 9;

    /**
     * See {@link AbstractStore#AbstractStore(String, Map)}
     */
    public NodeStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    /**
     * See {@link AbstractStore#AbstractStore(String)}
     */
    public NodeStore( String fileName )
    {
        super( fileName );
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    /**
     * Creates a new node store contained in <CODE>fileName</CODE> If filename
     * is <CODE>null</CODE> or the file already exists an 
     * <CODE>IOException</CODE> is thrown.
     * 
     * @param fileName
     *            File name of the new node store
     * @throws IOException
     *             If unable to create node store or name null
     */
    public static void createStore( String fileName )
    {
        createEmptyStore( fileName, VERSION );
        NodeStore store = new NodeStore( fileName );
        NodeRecord nodeRecord = new NodeRecord( store.nextId() );
        nodeRecord.setInUse( true );
        store.updateRecord( nodeRecord );
        store.close();
    }

    public NodeRecord getRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            NodeRecord record = getRecord( id, window.getBuffer(), false );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void updateRecord( NodeRecord record, boolean recovered )
    {
        assert recovered;
        setRecovered();
        try
        {
            updateRecord( record );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public void updateRecord( NodeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window.getBuffer() );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public boolean loadLightNode( int id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( StoreFailureException e )
        {
            // ok id to high
            return false;
        }

        try
        {
            NodeRecord record = getRecord( id, window.getBuffer(), true );
            if ( record == null )
            {
                return false;
            }
            return true;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private NodeRecord getRecord( int id, Buffer buffer, boolean check )
    {
        int offset = (int) (id - buffer.position()) * getRecordSize();
        buffer.setOffset( offset );
        boolean inUse = (buffer.get() == Record.IN_USE.byteValue());
        if ( !inUse )
        {
            if ( check )
            {
                return null;
            }
            throw new StoreFailureException( "Record[" + id + "] not in use" );
        }
        NodeRecord nodeRecord = new NodeRecord( id );
        nodeRecord.setInUse( inUse );
        nodeRecord.setNextRel( buffer.getInt() );
        nodeRecord.setNextProp( buffer.getInt() );
        return nodeRecord;
    }

    private void updateRecord( NodeRecord record, Buffer buffer )
    {
        int id = record.getId();
        int offset = (int) (id - buffer.position()) * getRecordSize();
        buffer.setOffset( offset );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() ).putInt( 
                record.getNextRel() ).putInt( record.getNextProp() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    public String toString()
    {
        return "NodeStore";
    }
}