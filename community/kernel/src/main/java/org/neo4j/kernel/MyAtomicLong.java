/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel;

import sun.misc.Unsafe;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

public class MyAtomicLong implements Serializable
{
    private static final AtomicLong unsafe = new AtomicLong();

    private static native boolean VMSupportsCS8();

    public MyAtomicLong( long var1 )
    {
        this.unsafe.set( var1 );
    }

    public MyAtomicLong()
    {
    }

    public final long get()
    {
        return unsafe.get();
    }

    public final void set( long var1 )
    {
        checkIfMagic( var1 );
        this.set( var1 );
    }

    private void checkIfMagic( long var1 )
    {
        if ( var1 == 262144000 )
        {
            new RuntimeException( "Value being set is target" ).printStackTrace();
        }
    }

    public final void lazySet( long var1 )
    {
        checkIfMagic( var1 );
        unsafe.lazySet( var1 );
    }

    public final long getAndSet( long var1 )
    {
        checkIfMagic( var1 );
        return unsafe.getAndSet( var1 );
    }

    public final boolean compareAndSet( long var1, long var3 )
    {
        checkIfMagic( var1 );
        checkIfMagic( var3 );
        return unsafe.compareAndSet( var1, var3 );
    }

    public final boolean weakCompareAndSet( long var1, long var3 )
    {
        checkIfMagic( var1 );
        checkIfMagic( var3 );
        return unsafe.weakCompareAndSet( var1, var3 );
    }

    public final long getAndIncrement()
    {
        return unsafe.getAndIncrement();
    }

    public final long getAndDecrement()
    {
        return unsafe.getAndDecrement();
    }

    public final long getAndAdd( long var1 )
    {
        return unsafe.getAndAdd( var1 );
    }

    public final long incrementAndGet()
    {
        return unsafe.incrementAndGet();
    }

    public final long decrementAndGet()
    {
        return unsafe.decrementAndGet();
    }

    public final long addAndGet( long var1 )
    {
        return unsafe.addAndGet( var1 );
    }

    public final long getAndUpdate( LongUnaryOperator var1 )
    {
        long var2;
        long var4;
        do
        {
            var2 = this.get();
            var4 = var1.applyAsLong( var2 );
        }
        while ( !this.compareAndSet( var2, var4 ) );

        return var2;
    }

    public final long updateAndGet( LongUnaryOperator var1 )
    {
        long var2;
        long var4;
        do
        {
            var2 = this.get();
            var4 = var1.applyAsLong( var2 );
        }
        while ( !this.compareAndSet( var2, var4 ) );

        return var4;
    }

    public final long getAndAccumulate( long var1, LongBinaryOperator var3 )
    {
        long var4;
        long var6;
        do
        {
            var4 = this.get();
            var6 = var3.applyAsLong( var4, var1 );
        }
        while ( !this.compareAndSet( var4, var6 ) );

        return var4;
    }

    public final long accumulateAndGet( long var1, LongBinaryOperator var3 )
    {
        long var4;
        long var6;
        do
        {
            var4 = this.get();
            var6 = var3.applyAsLong( var4, var1 );
        }
        while ( !this.compareAndSet( var4, var6 ) );

        return var6;
    }

    public String toString()
    {
        return Long.toString( this.get() );
    }

    public int intValue()
    {
        return (int) this.get();
    }

    public long longValue()
    {
        return this.get();
    }

    public float floatValue()
    {
        return (float) this.get();
    }

    public double doubleValue()
    {
        return (double) this.get();
    }
}
