/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordSerializer;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.register.Register;

/**
 * Node Key:
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
 * [x, , , , , , , , , , , ,x,x,x,x]
 *  _                       _ _ _ _
 *  |                          |
 * entry                      label
 * type                        id
 *
 * Relationship Key:
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
 * [x, ,x,x,x,x, ,x,x,x,x, ,x,x,x,x]
 *  _   _ _ _ _   _ _ _ _   _ _ _ _
 *  |      |         |         |
 * entry  label      rel      label
 * type    id        type      id
 *
 * Count value:
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
 * [ , , , , , , , ,x,x,x,x,x,x,x,x]
 *                  _ _ _ _ _ _ _ _
 *                         |
 *                       value
 */
public class CountsRecordSerializer implements KeyValueRecordSerializer<CountsKey, Register.LongRegister>
{
    static final byte EMPTY_RECORD_KEY = 0;
    static final byte NODE_KEY = 1;
    static final byte RELATIONSHIP_KEY = 2;

    @Override
    public boolean visitRecord(ByteBuffer buffer, KeyValueRecordVisitor<CountsKey, Register.LongRegister> visitor)
    {
        // read type
        byte type = buffer.get();

        // read key
        buffer.get(); // skip unused byte
        int one = buffer.getInt();
        buffer.get(); // skip unused byte
        int two = buffer.getInt();
        buffer.get(); // skip unused byte
        int three = buffer.getInt();

        // read value
        buffer.getLong(); // skip unused long
        long count = buffer.getLong();
        visitor.valueRegister().write( count );

        CountsKey key;
        switch ( type )
        {
            case EMPTY_RECORD_KEY:
                assert one == 0;
                assert two == 0;
                assert three == 0;
                assert count == 0;
                return false;

            case NODE_KEY:
                assert one == 0;
                assert two == 0;
                key = nodeKey( three /* label id*/ );
                break;

            case RELATIONSHIP_KEY:
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                break;

            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }
        visitor.visit( key );
        return true;
    }

    @Override
    public CountsKey readRecord( PageCursor cursor, int offset, Register.LongRegister value ) throws IOException
    {
        byte type;
        int one, two, three;
        long count;

        do
        {
            cursor.setOffset( offset );

            // read type
            type = cursor.getByte();

            // read key
            cursor.getByte(); // skip unused byte
            one = cursor.getInt();
            cursor.getByte(); // skip unused byte
            two = cursor.getInt();
            cursor.getByte(); // skip unused byte
            three = cursor.getInt();

            // read value
            cursor.getLong(); // skip unused long
            count =  cursor.getLong();
        } while ( cursor.shouldRetry() );

        CountsKey key;
        switch ( type )
        {
            case EMPTY_RECORD_KEY:
                throw new IllegalStateException( "Reading empty record" );
            case NODE_KEY:
                assert one == 0;
                assert two == 0;
                key = nodeKey( three /* label id*/ );
                value.write( count );
                break;
            case RELATIONSHIP_KEY:
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                value.write( count );
                break;
            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }
        return key;
    }

    @Override
    public void writeDefaultValue( Register.LongRegister valueRegister )
    {
        valueRegister.write( 0 );
    }
}
