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
package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the relationship store.
 */
public class RelationshipStore extends AbstractStore implements Store
{
    // relationship store version, each rel store ends with this
    // string (byte encoded)
    private static final String VERSION = "RelationshipStore v0.9.5";

    // record header size
    // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
    // first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
    // second_next_rel_id+next_prop_id(int)
    private static final int RECORD_SIZE = 33;

    /**
     * See {@link AbstractStore#AbstractStore(String, Map)}
     */
    public RelationshipStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    /**
     * See {@link AbstractStore#AbstractStore(String)}
     */
    public RelationshipStore( String fileName )
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

    @Override
    public void close()
    {
        super.close();
    }

    /**
     * Creates a new relationship store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an <CODE>IOException</CODE>
     * is thrown.
     * 
     * @param fileName
     *            File name of the new relationship store
     * @throws IOException
     *             If unable to create relationship store or name null
     */
    public static void createStore( String fileName )
    {
        createEmptyStore( fileName, VERSION );
    }

    public RelationshipRecord getRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            RelationshipRecord record = getRecord( id, window, false );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public RelationshipRecord getLightRel( int id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( StoreFailureException e )
        {
            // ok to high id
            return null;
        }
        try
        {
            RelationshipRecord record = getRecord( id, window, true );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void updateRecord( RelationshipRecord record, boolean recovered )
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

    public void updateRecord( RelationshipRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private void updateRecord( RelationshipRecord record, 
        PersistenceWindow window )
    {
        int id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            buffer.put( inUse ).putInt( record.getFirstNode() ).putInt(
                record.getSecondNode() ).putInt( record.getType() ).putInt(
                record.getFirstPrevRel() ).putInt( record.getFirstNextRel() )
                .putInt( record.getSecondPrevRel() ).putInt(
                    record.getSecondNextRel() ).putInt( record.getNextProp() );
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

    private RelationshipRecord getRecord( int id, PersistenceWindow window, 
        boolean check )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        byte inUse = buffer.get();
        boolean inUseFlag = ((inUse & Record.IN_USE.byteValue()) == 
            Record.IN_USE.byteValue());
        if ( !inUseFlag )
        {
            if ( check )
            {
                return null;
            }
            throw new StoreFailureException( "Record[" + id + "] not in use" );
        }
        RelationshipRecord record = new RelationshipRecord( id,
            buffer.getInt(), buffer.getInt(), buffer.getInt() );
        record.setInUse( inUseFlag );
        record.setFirstPrevRel( buffer.getInt() );
        record.setFirstNextRel( buffer.getInt() );
        record.setSecondPrevRel( buffer.getInt() );
        record.setSecondNextRel( buffer.getInt() );
        record.setNextProp( buffer.getInt() );
        return record;
    }

    public String toString()
    {
        return "RelStore";
    }

    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "RelationshipStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
        if ( version.equals( "RelationshipStore v0.9.3" ) )
        {
            rebuildIdGenerator();
            closeIdGenerator();
            return true;
        }
        throw new RuntimeException( "Unknown store version " + version  + 
            " Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }
}