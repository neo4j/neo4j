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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordSerializer;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.register.Register.DoubleLongRegister;

import static org.neo4j.kernel.impl.store.counts.CountsKey.indexCountsKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.relationshipKey;

/**
 * Node Key:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [x, , , , , , , , , , , ,x,x,x,x]
 *  _                       _ _ _ _
 *  |                          |
 *  entry                      label
 *  type                        id
 * <p/>
 *
 * Relationship Key:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [x, ,x,x,x,x, ,x,x,x,x, ,x,x,x,x]
 *  _   _ _ _ _   _ _ _ _   _ _ _ _
 *  |      |         |         |
 *  entry  label      rel      label
 *  type    id        type      id
 *  id
 * <p/>
 *
 * Index Key:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [x, , , , , , ,x,x,x,x, ,x,x,x,x]
 *  _             _ _ _ _   _ _ _ _
 *  |                |         |
 *  entry       property key   label
 *  type id          id        id
 * <p/>
 *
 * Count value:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [ , , , , , , , ,x,x,x,x,x,x,x,x]
 *                  _ _ _ _ _ _ _ _
 *                   |
 *                  value
 * <p/>
 *
 * Index count:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [u,u,u,u,u,u,u,u,s,s,s,s,s,s,s,s]
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *  |               |
 *  updates seen    index size
 * <p/>
 *
 * Index sample:
 *  0 1 2 3 4 5 6 7 8 9 A B C D E F
 * [u,u,u,u,u,u,u,u,s,s,s,s,s,s,s,s]
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *  |               |
 *  unique count    sample size
 * <p/>
 *
 * 'entry type' - see {@link org.neo4j.kernel.impl.store.counts.CountsKeyType}
 */
public final class CountsRecordSerializer implements KeyValueRecordSerializer<CountsKey, DoubleLongRegister>
{
    public static final CountsRecordSerializer INSTANCE = new CountsRecordSerializer();

    public static final long DEFAULT_FIRST_VALUE = 0l;
    public static final long DEFAULT_SECOND_VALUE = 0l;

    private CountsRecordSerializer()
    {
    }

    @Override
    public boolean visitRecord( ByteBuffer buffer,
                                KeyValueRecordVisitor<CountsKey, DoubleLongRegister> visitor,
                                DoubleLongRegister valueRegister )
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
        long first = buffer.getLong();
        long second = buffer.getLong();
        valueRegister.write( first, second );

        CountsKey key;
        switch ( CountsKeyType.fromCode( type ) )
        {
            case EMPTY:
                assert one == 0;
                assert two == 0;
                assert three == 0;
                assert first == 0;
                assert second == 0;
                return false;

            case ENTITY_NODE:
                assert one == 0;
                assert two == 0;
                assert first == 0;
                key = nodeKey( three /* label id*/ );
                break;

            case ENTITY_RELATIONSHIP:
                assert first == 0;
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                break;

            case INDEX_COUNTS:
                assert one == 0;
                key = indexCountsKey( three /* label id */, two /* pk id */ );
                break;

            case INDEX_SAMPLE:
                assert one == 0;
                key = indexSampleKey( three /* label id */, two /* pk id */ );
                break;

            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }
        visitor.visit( key, valueRegister );
        return true;
    }

    @Override
    public CountsKey readRecord( PageCursor cursor, int offset, DoubleLongRegister value ) throws IOException
    {
        byte type;
        int one, two, three;
        long first, second;

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
            first = cursor.getLong();
            second = cursor.getLong();
        } while ( cursor.shouldRetry() );

        value.write(first, second );

        CountsKey key;
        switch ( CountsKeyType.fromCode( type ) )
        {
            case EMPTY:
                throw new IllegalStateException( "Reading empty record" );

            case ENTITY_NODE:
                assert one == 0;
                assert two == 0;
                key = nodeKey( three /* label id*/ );
                break;

            case ENTITY_RELATIONSHIP:
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                break;

            case INDEX_COUNTS:
                assert one == 0;
                key = indexCountsKey( three /* label id */, two /* pk id */ );
                break;

            case INDEX_SAMPLE:
                assert one == 0;
                key = indexSampleKey( three /* label id */, two /* pk id */ );
                break;

            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }

        return key;
    }

    @Override
    public void writeDefaultValue( DoubleLongRegister valueRegister )
    {
        valueRegister.write( DEFAULT_FIRST_VALUE, DEFAULT_SECOND_VALUE );
    }
}
