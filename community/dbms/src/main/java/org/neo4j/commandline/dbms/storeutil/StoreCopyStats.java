/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.commandline.dbms.storeutil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Value;

import static java.lang.Math.max;
import static java.lang.String.format;

class StoreCopyStats
{
    private static final int MAX_LOG_LENGTH = 120;

    private final long startTime;
    private final Log log;
    final LongAdder count = new LongAdder();
    final LongAdder unused = new LongAdder();
    final LongAdder removed = new LongAdder();

    StoreCopyStats( Log log )
    {
        this.log = log;
        startTime = System.nanoTime();
    }

    void addCorruptToken( String type, int id )
    {
        log.error( "%s(%d): Missing token name", type, id );
    }

    void brokenPropertyToken( String type, PrimitiveRecord record, Value newPropertyValue, int keyIndexId )
    {
        String value = newPropertyValue.toString();
        log.error( "%s(%d): Ignoring property with missing token(%d). Value of the property is %s.",
                type, record.getId(), keyIndexId, trimToMaxLength( value ) );
    }

    void brokenPropertyChain( String type, PrimitiveRecord record, Exception e )
    {
        log.error( format( "%s(%d): Ignoring broken property chain.", type, record.getId() ), e );
    }

    void brokenRecord( String type, long id, Exception e )
    {
        log.error( format( "%s(%d): Ignoring broken record.", type, id ), e );
    }

    void printSummary()
    {
        long seconds = TimeUnit.NANOSECONDS.toSeconds( System.nanoTime() - startTime );
        long count = this.count.sum();
        long unused = this.unused.sum();
        long removed = this.removed.sum();

        log.info( "Import summary: Copying of %d records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)",
                count, seconds, count / max( 1L, seconds ), unused, percent( unused, count ), removed, percent( removed, count ));
    }

    void invalidIndex( IndexDescriptor indexDescriptor, Exception e )
    {
        log.error( format( "Unable to format statement for index '%s'%n", indexDescriptor.getName() ), e );
    }

    void invalidConstraint( ConstraintDescriptor constraintDescriptor, Exception e )
    {
        log.error( format( "Unable to format statement for constraint '%s'%n", constraintDescriptor.getName() ), e );
    }

    private static int percent( long part, long total )
    {
        if ( total == 0L )
        {
            return 0;
        }
        return (int) (100 * ((double) part) / total);
    }

    private static String trimToMaxLength( String value )
    {
        return value.length() <= MAX_LOG_LENGTH ? value : value.substring( 0, MAX_LOG_LENGTH ) + "..";
    }
}
