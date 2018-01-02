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
package org.neo4j.kernel.impl.storemigration.legacystore.v19;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.LegacyDynamicStringStoreReader;

import static org.neo4j.kernel.impl.store.StoreFactory.KEYS_PART;
import static org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store.readIntoBuffer;

public class Legacy19PropertyIndexStoreReader implements Closeable
{
    public static final String FROM_VERSION = "PropertyIndexStore " + Legacy19Store.LEGACY_VERSION;
    public static final int RECORD_SIZE = 9;
    private final StoreChannel fileChannel;
    private final LegacyDynamicStringStoreReader nameStoreReader;
    private final long maxId;
    
    public Legacy19PropertyIndexStoreReader( FileSystemAbstraction fs, File file ) throws IOException
    {
        fileChannel = fs.open( file, "r" );
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_SIZE;
        
        nameStoreReader = new LegacyDynamicStringStoreReader( fs, new File( file.getPath() + KEYS_PART ),
                "StringPropertyStore" );
    }
    
    public Token[] readTokens() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap( new byte[RECORD_SIZE] );
        Collection<Token> tokens = new ArrayList<Token>();
        for ( long id = 0; id < maxId; id++ )
        {
            readIntoBuffer( fileChannel, buffer, RECORD_SIZE );
            byte inUseByte = buffer.get();
            boolean inUse = (inUseByte == Record.IN_USE.byteValue());
            if ( inUseByte != Record.IN_USE.byteValue() && inUseByte != Record.NOT_IN_USE.byteValue() )
            {
                throw new InvalidRecordException( "Record[" + id + "] unknown in use flag[" + inUse + "]" );
            }
            if ( !inUse )
            {
                continue;
            }

            buffer.getInt(); // unused "property count"
            int nameId = buffer.getInt();
            String name = nameStoreReader.readDynamicString( nameId );
            tokens.add( new Token( name, (int) id ) );
        }
        return tokens.toArray( new Token[tokens.size()] );
    }
    
    @Override
    public void close() throws IOException
    {
        nameStoreReader.close();
        fileChannel.close();
    }
}
