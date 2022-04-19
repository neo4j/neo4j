/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.storageengine.api;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreIdTest
{
    @Test
    void testCompatibilityCheck()
    {
        var storeId = new StoreId( 1234, 789, "engine-1", "family-1", 3, 7 );
        assertTrue( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 3, 7 ) ) );
        assertTrue( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 3, 8 ) ) );
        assertTrue( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 3, 15 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 666, 789, "engine-1", "family-1", 3, 7 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 666, "engine-1", "family-1", 3, 7 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 3, 6 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 4, 7 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-1", 2, 7 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-2", "family-1", 3, 7 ) ) );
        assertFalse( storeId.isSameOrUpgradeSuccessor( new StoreId( 1234, 789, "engine-1", "family-2", 3, 7 ) ) );
    }

    @Test
    void testSerialization() throws IOException
    {
        var buffer = new ChannelBuffer( 100 );
        var storeId = new StoreId( 1234, 789, "engine-1", "family-1", 3, 7 );
        storeId.serialize( buffer );
        buffer.flip();
        var deserializedStoreId = StoreId.deserialize( buffer );
        assertEquals( storeId, deserializedStoreId );
    }

    private static class ChannelBuffer implements WritableChannel, ReadableChannel
    {

        private final ByteBuffer buffer;

        ChannelBuffer( int capacity )
        {
            this.buffer = ByteBuffer.allocate( capacity );
        }

        @Override
        public byte get() throws IOException
        {
            return buffer.get();
        }

        @Override
        public short getShort() throws IOException
        {
            return buffer.getShort();
        }

        @Override
        public int getInt() throws IOException
        {
            return buffer.getInt();
        }

        @Override
        public long getLong() throws IOException
        {
            return buffer.getLong();
        }

        @Override
        public float getFloat() throws IOException
        {
            return buffer.getFloat();
        }

        @Override
        public double getDouble() throws IOException
        {
            return buffer.getDouble();
        }

        @Override
        public void get( byte[] bytes, int length ) throws IOException
        {
            buffer.get( bytes, 0, length );
        }

        @Override
        public WritableChannel put( byte value ) throws IOException
        {
            buffer.put( value );
            return this;
        }

        @Override
        public WritableChannel putShort( short value ) throws IOException
        {
            buffer.putShort( value );
            return this;
        }

        @Override
        public WritableChannel putInt( int value ) throws IOException
        {
            buffer.putInt( value );
            return this;
        }

        @Override
        public WritableChannel putLong( long value ) throws IOException
        {
            buffer.putLong( value );
            return this;
        }

        @Override
        public WritableChannel putFloat( float value ) throws IOException
        {
            buffer.putFloat( value );
            return this;
        }

        @Override
        public WritableChannel putDouble( double value ) throws IOException
        {
            buffer.putDouble( value );
            return this;
        }

        @Override
        public WritableChannel put( byte[] value, int offset, int length ) throws IOException
        {
            buffer.put( value, 0, length );
            return this;
        }

        @Override
        public void close() throws IOException
        {

        }

        void flip()
        {
            buffer.flip();
        }
    }
}
