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
package org.neo4j.adversaries.pagecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A read {@linkplain PageCursor page cursor} that wraps another page cursor and an {@linkplain Adversary adversary}
 * to provide a misbehaving page cursor implementation for testing.
 * <p>
 * Depending on the adversary each read operation can throw either {@link RuntimeException} like
 * {@link SecurityException} or {@link IOException} like {@link FileNotFoundException}.
 * <p>
 * Depending on the adversary each read operation can produce an inconsistent read and require caller to retry using
 * while loop with {@link PageCursor#shouldRetry()} as a condition.
 * <p>
 * Inconsistent reads are injected by first having a retry-round (the set of operations on the cursor up until the
 * {@link #shouldRetry()} call) that counts the number of operations performed on the cursor, and otherwise delegates
 * the read operations to the real page cursor without corrupting them. Then the {@code shouldRetry} will choose a
 * random operation, and from that point on in the next retry-round, all read operations will return random data. The
 * {@code shouldRetry} method returns {@code true} for "yes, you should retry" and the round with the actual read
 * inconsistencies begins. After that round, the client will be told to retry again, and in this third round there will
 * be no inconsistencies, and there will be no need to retry unless the real page cursor says so.
 * <p>
 * Write operations will always throw an {@link IllegalStateException} because this is a read cursor.
 * See {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_LOCK} flag.
 */
@SuppressWarnings( "unchecked" )
class AdversarialReadPageCursor implements PageCursor
{
    private final PageCursor delegate;
    private final Adversary adversary;

    private boolean currentReadIsPreparingInconsistent;
    private boolean currentReadIsInconsistent;
    private int callCounter;

    // This field for meant to be inspected with the debugger.
    @SuppressWarnings( "MismatchedQueryAndUpdateOfCollection" )
    private List<Object> inconsistentReadHistory;

    AdversarialReadPageCursor( PageCursor delegate, Adversary adversary )
    {
        this.delegate = Objects.requireNonNull( delegate );
        this.adversary = Objects.requireNonNull( adversary );
    }


    @Override
    public byte getByte()
    {
        return inconsistently( delegate.getByte() ).byteValue();
    }

    private <T extends Number> Number inconsistently( T value )
    {
        if ( currentReadIsPreparingInconsistent )
        {
            callCounter++;
            return value;
        }
        if ( currentReadIsInconsistent && (--callCounter) <= 0 )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            long x = value.longValue();
            if ( x != 0 & rng.nextBoolean() )
            {
                x = ~x;
            }
            else
            {
                x = rng.nextLong();
            }
            inconsistentReadHistory.add( new NumberValue( value.getClass(), x, delegate.getOffset() ) );
            return x;
        }
        return value;
    }

    private void inconsistently( byte[] data )
    {
        inconsistently( data, 0, data.length );
    }


    private void inconsistently( byte[] data, int from, int to )
    {
        if ( currentReadIsPreparingInconsistent )
        {
            callCounter++;
        }
        else if ( currentReadIsInconsistent )
        {
            ThreadLocalRandom.current().nextBytes( data );
            inconsistentReadHistory.add( Arrays.copyOfRange( data, from, to ) );
        }
    }

    @Override
    public byte getByte( int offset )
    {
        return inconsistently( delegate.getByte( offset ) ).byteValue();
    }

    @Override
    public void putByte( byte value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putByte( int offset, byte value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public long getLong()
    {
        return inconsistently( delegate.getLong() ).longValue();
    }

    @Override
    public long getLong( int offset )
    {
        return inconsistently( delegate.getLong( offset ) ).longValue();
    }

    @Override
    public void putLong( long value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putLong( int offset, long value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public int getInt()
    {
        return inconsistently( delegate.getInt() ).intValue();
    }

    @Override
    public int getInt( int offset )
    {
        return inconsistently( delegate.getInt( offset ) ).intValue();
    }

    @Override
    public void putInt( int value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putInt( int offset, int value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public long getUnsignedInt()
    {
        return inconsistently( delegate.getUnsignedInt() ).longValue();
    }

    @Override
    public long getUnsignedInt( int offset )
    {
        return inconsistently( delegate.getUnsignedInt( offset ) ).longValue();
    }

    @Override
    public void getBytes( byte[] data )
    {
        delegate.getBytes( data );
        inconsistently( data );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
        inconsistently( data, arrayOffset, length );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putBytes( byte[] data )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public short getShort()
    {
        return inconsistently( delegate.getShort() ).shortValue();
    }

    @Override
    public short getShort( int offset )
    {
        return inconsistently( delegate.getShort( offset ) ).shortValue();
    }

    @Override
    public void putShort( short value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putShort( int offset, short value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void setOffset( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.setOffset( offset );
    }

    @Override
    public int getOffset()
    {
        return delegate.getOffset();
    }

    @Override
    public long getCurrentPageId()
    {
        return delegate.getCurrentPageId();
    }

    @Override
    public int getCurrentPageSize()
    {
        return delegate.getCurrentPageSize();
    }

    @Override
    public File getCurrentFile()
    {
        return delegate.getCurrentFile();
    }

    @Override
    public void rewind()
    {
        delegate.rewind();
    }

    @Override
    public boolean next() throws IOException
    {
        currentReadIsPreparingInconsistent = adversary.injectFailureOrMischief( FileNotFoundException.class, IOException.class,
                SecurityException.class, IllegalStateException.class );
        callCounter = 0;
        return delegate.next();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        currentReadIsPreparingInconsistent = adversary.injectFailureOrMischief( FileNotFoundException.class, IOException.class,
                SecurityException.class, IllegalStateException.class );
        callCounter = 0;
        return delegate.next( pageId );
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class,
                IllegalStateException.class );
        if ( currentReadIsPreparingInconsistent )
        {
            currentReadIsPreparingInconsistent = false;
            currentReadIsInconsistent = true;
            callCounter = ThreadLocalRandom.current().nextInt( callCounter + 1 );
            inconsistentReadHistory = new ArrayList<>();
            delegate.shouldRetry();
            delegate.setOffset( 0 );
            return true;
        }
        if ( currentReadIsInconsistent )
        {
            currentReadIsInconsistent = false;
            delegate.shouldRetry();
            delegate.setOffset( 0 );
            return true;
        }
        return delegate.shouldRetry();
    }

    private static class NumberValue
    {
        private final Class<? extends Number> type;
        private final Long value;
        private final int offset;
        private final Exception trace;

        public NumberValue( Class<? extends Number> type, Long value, int offset )
        {
            this.type = type;
            this.value = value;
            this.offset = offset;
            trace = new Exception( toString() );
            trace.fillInStackTrace();
        }

        @Override
        public String toString()
        {
            String typeName = type.getCanonicalName();
            switch ( typeName )
            {
            case "java.lang.Byte": return "(byte)" + value.byteValue() + " at offset " + offset;
            case "java.lang.Short": return "(short)" + value.shortValue() + " at offset " + offset;
            case "java.lang.Integer": return "(int)" + value.intValue() + " at offset " + offset;
            case "java.lang.Long": return "(long)" + value + " at offset " + offset;
            }
            return "(" + typeName + ")" + value + " at offset " + offset;
        }

        public void printStackTrace()
        {
            trace.printStackTrace();
        }
    }
}
