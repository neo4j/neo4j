/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.adversaries.pagecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;
import org.neo4j.util.FeatureToggles;

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
 * See {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_READ_LOCK} flag.
 */
@SuppressWarnings( "unchecked" )
class AdversarialReadPageCursor extends DelegatingPageCursor
{
    private static final boolean enableInconsistencyTracing = FeatureToggles.flag( AdversarialReadPageCursor.class, "enableInconsistencyTracing", false );

    private static class State implements Adversary
    {
        private final Adversary adversary;

        private boolean currentReadIsPreparingInconsistent;
        private boolean currentReadIsInconsistent;
        private int callCounter;

        // This field for meant to be inspected with the debugger.
        @SuppressWarnings( "MismatchedQueryAndUpdateOfCollection" )
        private List<Object> inconsistentReadHistory;

        private State( Adversary adversary )
        {
            this.adversary = adversary;
            inconsistentReadHistory = new ArrayList<>( 32 );
        }

        private <T extends Number> Number inconsistently( T value, PageCursor delegate )
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
                inconsistentReadHistory.add( new NumberValue( value.getClass(), x, delegate.getOffset(), value ) );
                return x;
            }
            return value;
        }

        private void inconsistently( byte[] data, int arrayOffset, int length )
        {
            if ( currentReadIsPreparingInconsistent )
            {
                callCounter++;
            }
            else if ( currentReadIsInconsistent )
            {
                byte[] gunk = new byte[length];
                ThreadLocalRandom.current().nextBytes( gunk );
                System.arraycopy( gunk, 0, data, arrayOffset, length );
                inconsistentReadHistory.add( Arrays.copyOf( data, data.length ) );
            }
        }

        private void reset( boolean currentReadIsPreparingInconsistent )
        {
            callCounter = 0;
            this.currentReadIsPreparingInconsistent = currentReadIsPreparingInconsistent;
        }

        @Override
        public void injectFailure( Class<? extends Throwable>... failureTypes )
        {
            adversary.injectFailure( failureTypes );
        }

        @Override
        public boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
        {
            return adversary.injectFailureOrMischief( failureTypes );
        }

        private boolean hasPreparedInconsistentRead()
        {
            if ( currentReadIsPreparingInconsistent )
            {
                currentReadIsPreparingInconsistent = false;
                currentReadIsInconsistent = true;
                callCounter = ThreadLocalRandom.current().nextInt( callCounter + 1 );
                inconsistentReadHistory = new ArrayList<>();
                return true;
            }
            return false;
        }

        private boolean hasInconsistentRead()
        {
            if ( currentReadIsInconsistent )
            {
                currentReadIsInconsistent = false;
                return true;
            }
            return false;
        }

        public boolean isInconsistent()
        {
            if ( currentReadIsPreparingInconsistent )
            {
                callCounter++;
            }
            return currentReadIsInconsistent;
        }
    }

    private AdversarialReadPageCursor linkedCursor;
    private final State state;

    AdversarialReadPageCursor( PageCursor delegate, Adversary adversary )
    {
        super( delegate );
        this.state = new State( Objects.requireNonNull( adversary ) );
    }

    private AdversarialReadPageCursor( PageCursor delegate, State state )
    {
        super( delegate );
        this.state = state;
    }

    @Override
    public byte getByte()
    {
        return inconsistently( delegate.getByte() ).byteValue();
    }

    private <T extends Number> Number inconsistently( T value )
    {
        return state.inconsistently( value, delegate );
    }

    private void inconsistently( byte[] data, int arrayOffset, int length )
    {
        state.inconsistently( data, arrayOffset, length );
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
    public void getBytes( byte[] data )
    {
        delegate.getBytes( data );
        inconsistently( data, 0, data.length );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
        inconsistently( data, arrayOffset, length );
    }

    @Override
    public void putBytes( byte[] data )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
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
        state.injectFailure( IndexOutOfBoundsException.class );
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
        prepareNext();
        return delegate.next();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        prepareNext();
        return delegate.next( pageId );
    }

    private void prepareNext()
    {
        boolean currentReadIsPreparingInconsistent =
                state.injectFailureOrMischief( FileNotFoundException.class, IOException.class, SecurityException.class, IllegalStateException.class );
        state.reset( currentReadIsPreparingInconsistent );
    }

    @Override
    public void close()
    {
        delegate.close();
        linkedCursor = null;
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        state.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class, IllegalStateException.class );
        if ( state.hasPreparedInconsistentRead() )
        {
            resetDelegate();
            return true;
        }
        if ( state.hasInconsistentRead() )
        {
            resetDelegate();
            return true;
        }
        boolean retry = delegate.shouldRetry();
        return retry || (linkedCursor != null && linkedCursor.shouldRetry());
    }

    private void resetDelegate() throws IOException
    {
        delegate.shouldRetry();
        delegate.setOffset( 0 );
        delegate.checkAndClearBoundsFlag();
        delegate.clearCursorException();
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        state.injectFailure( IndexOutOfBoundsException.class );
        if ( !state.isInconsistent() )
        {
            while ( targetCursor instanceof DelegatingPageCursor )
            {
                targetCursor = ((DelegatingPageCursor) targetCursor).unwrap();
            }
            return delegate.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
        }
        return lengthInBytes;
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        return delegate.checkAndClearBoundsFlag() || (linkedCursor != null && linkedCursor.checkAndClearBoundsFlag());
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        delegate.checkAndClearCursorException();
    }

    @Override
    public void raiseOutOfBounds()
    {
        delegate.raiseOutOfBounds();
    }

    @Override
    public void setCursorException( String message )
    {
        delegate.setCursorException( message );
    }

    @Override
    public void clearCursorException()
    {
        delegate.clearCursorException();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId ) throws IOException
    {
        return linkedCursor = new AdversarialReadPageCursor( delegate.openLinkedCursor( pageId ), state );
    }

    @Override
    public void zapPage()
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public boolean isWriteLocked()
    {
        return delegate.isWriteLocked();
    }

    @Override
    public String toString()
    {
        State s = this.state;
        StringBuilder sb = new StringBuilder();
        for ( Object o : s.inconsistentReadHistory )
        {
            sb.append( o.toString() ).append( '\n' );
            if ( o instanceof NumberValue )
            {
                NumberValue v = (NumberValue) o;
                v.printStackTrace( sb );
            }
        }
        return sb.toString();
    }

    private static class NumberValue
    {
        private final Class<? extends Number> type;
        private final long value;
        private final int offset;
        private final Number insteadOf;
        private Exception trace;

        NumberValue( Class<? extends Number> type, long value, int offset, Number insteadOf )
        {
            this.type = type;
            this.value = value;
            this.offset = offset;
            this.insteadOf = insteadOf;
            if ( enableInconsistencyTracing )
            {
                trace = new Exception()
                {
                    @Override
                    public String getMessage()
                    {
                        return NumberValue.this.toString();
                    }
                };
            }
        }

        @Override
        public String toString()
        {
            String typeName = type.getCanonicalName();
            switch ( typeName )
            {
            case "java.lang.Byte":
                return "(byte)" + value + " at offset " + offset + " (instead of " + insteadOf + ")";
            case "java.lang.Short":
                return "(short)" + value + " at offset " + offset + " (instead of " + insteadOf + ")";
            case "java.lang.Integer":
                return "(int)" + value + " at offset " + offset + " (instead of " + insteadOf + ")";
            case "java.lang.Long":
                return "(long)" + value + " at offset " + offset + " (instead of " + insteadOf + ")";
            default:
                return "(" + typeName + ")" + value + " at offset " + offset + " (instead of " + insteadOf + ")";
            }
        }

        public void printStackTrace( StringBuilder sb )
        {
            StringWriter w = new StringWriter();
            PrintWriter pw = new PrintWriter( w );
            if ( trace != null )
            {
                trace.printStackTrace( pw );
            }
            pw.flush();
            sb.append( w );
            sb.append( '\n' );
        }
    }
}
