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
package org.neo4j.kernel.impl.index.schema;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Random;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;

public class TemporalIndexPopulationStressTest extends IndexPopulationStressTest
{
    private static final ZoneId[] zoneIds = ZoneId.getAvailableZoneIds().stream().map( ZoneId::of ).toArray( ZoneId[]::new );
    private static final int MAX_OFFSET = ZoneOffset.MAX.getTotalSeconds();
    private static final int MIN_OFFSET = ZoneOffset.MIN.getTotalSeconds();

    @Override
    IndexProvider newProvider( IndexDirectoryStructure.Factory directory )
    {
        return new TemporalIndexProvider( rules.pageCache(),
                                          rules.fileSystem(),
                                          directory,
                                          IndexProvider.Monitor.EMPTY,
                                          RecoveryCleanupWorkCollector.immediate(),
                                          false );
    }

    @Override
    Value randomValue( Random random )
    {
        switch ( random.nextInt( 6 ) )
        {
        case 0:
            return DateValue.epochDate( random.nextInt( 1_000_000 ) );

        case 1:
            return LocalDateTimeValue.localDateTime( epochSecond( random ), nanosOfSecond( random ) );

        case 2:
            ZoneId zone = random.nextBoolean() ? randomZoneOffset( random ) : randomZoneId( random );
            return DateTimeValue.datetime( epochSecond( random ), nanosOfSecond( random ), zone );

        case 3:
            return LocalTimeValue.localTime( secondsOfDay( random ) );

        case 4:
            return TimeValue.time( secondsOfDay( random ), randomZoneOffset( random ) );

        case 5:
            return DurationValue.duration( random.nextInt( 1_000_000 ), random.nextInt( 1_000 ), random.nextInt( 1_000 ), random.nextInt( 1_000 ) );

        default:
            throw new IllegalStateException( "Managed to break java.util.Random" );
        }
    }

    private int secondsOfDay( Random random )
    {
        return random.nextInt( 1_000_000_000 );
    }

    private int epochSecond( Random random )
    {
        return random.nextInt( 1_000_000_000 );
    }

    private ZoneOffset randomZoneOffset( Random random )
    {
        return ZoneOffset.ofTotalSeconds( random.nextInt( MAX_OFFSET - MIN_OFFSET ) + MIN_OFFSET );
    }

    private ZoneId randomZoneId( Random random )
    {
        return zoneIds[random.nextInt( zoneIds.length )];
    }

    private long nanosOfSecond( Random random )
    {
        return random.nextInt( 1_000_000_000 );
    }
}
