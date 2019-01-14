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

import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenericKeyStateFormatTest extends FormatCompatibilityVerifier
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private static final int ENTITY_ID = 19570320;
    private static final int NUMBER_OF_SLOTS = 2;
    private List<Value> values;

    @Before
    public void setup()
    {
        values = new ArrayList<>();
        // ZONED_DATE_TIME_ARRAY
        values.add( Values.dateTimeArray( new ZonedDateTime[]{
                ZonedDateTime.of( 2018, 10, 9, 8, 7, 6, 5, ZoneId.of( "UTC" ) ),
                ZonedDateTime.of( 2017, 9, 8, 7, 6, 5, 4, ZoneId.of( "UTC" ) )
        } ) );
        // LOCAL_DATE_TIME_ARRAY
        values.add( Values.localDateTimeArray( new LocalDateTime[]{
                LocalDateTime.of( 2018, 10, 9, 8, 7, 6, 5 ),
                LocalDateTime.of( 2018, 10, 9, 8, 7, 6, 5 )
        } ) );
        // DATE_ARRAY
        values.add( Values.dateArray( new LocalDate[]{
                LocalDate.of( 1, 12, 28 ),
                LocalDate.of( 1, 12, 28 )
        } ) );
        // ZONED_TIME_ARRAY
        values.add( Values.timeArray( new OffsetTime[]{
                OffsetTime.of( 19, 8, 7, 6, ZoneOffset.UTC ),
                OffsetTime.of( 19, 8, 7, 6, ZoneOffset.UTC )
        } ) );
        // LOCAL_TIME_ARRAY
        values.add( Values.localTimeArray( new LocalTime[]{
                LocalTime.of( 19, 28 ),
                LocalTime.of( 19, 28 )
        } ) );
        // DURATION_ARRAY
        values.add( Values.durationArray( new DurationValue[]{
                DurationValue.duration( 99, 10, 10, 10 ),
                DurationValue.duration( 99, 10, 10, 10 )
        } ) );
        // TEXT_ARRAY
        values.add( Values.of( new String[]{"someString1", "someString2"} ) );
        // BOOLEAN_ARRAY
        values.add( Values.of( new boolean[]{true, true} ) );
        // NUMBER_ARRAY (byte, short, int, long, float, double)
        values.add( Values.of( new byte[]{(byte) 1, (byte) 12} ) );
        values.add( Values.of( new short[]{314, 1337} ) );
        values.add( Values.of( new int[]{3140, 13370} ) );
        values.add( Values.of( new long[]{31400, 133700} ) );
        values.add( Values.of( new float[]{0.5654f, 13432.14f} ) );
        values.add( Values.of( new double[]{432453254.43243, 4354.7888} ) );
        values.add( Values.of( new char[]{'a', 'z'} ) );
        // ZONED_DATE_TIME
        values.add( DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "UTC" ) );
        // LOCAL_DATE_TIME
        values.add( LocalDateTimeValue.localDateTime( 2018, 3, 1, 13, 50, 42, 1337 ) );
        // DATE
        values.add( DateValue.epochDate( 2 ) );
        // ZONED_TIME
        values.add( TimeValue.time( 43_200_000_000_000L, ZoneOffset.UTC ) );
        // LOCAL_TIME
        values.add( LocalTimeValue.localTime( 100000 ) );
        // DURATION
        values.add( DurationValue.duration( 10, 20, 30, 40 ) );
        // TEXT
        values.add( Values.of( "string1" ) );
        // BOOLEAN
        values.add( Values.of( true ) );
        // NUMBER (byte, short, int, long, float, double)
        values.add( Values.of( Byte.MAX_VALUE ) );
        values.add( Values.of( Short.MAX_VALUE ) );
        values.add( Values.of( Integer.MAX_VALUE ) );
        values.add( Values.of( Long.MAX_VALUE ) );
        values.add( Values.of( Float.MAX_VALUE ) );
        values.add( Values.of( Double.MAX_VALUE ) );
        values.add( Values.of( Character.MAX_VALUE ) );
        // GEOMETRY
        values.add( Values.pointValue( CoordinateReferenceSystem.WGS84, 12.78, 56.7 ) );
        values.add( Values.pointArray( new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.WGS84, 12.7566548, 56.7163465 ),
                Values.pointValue( CoordinateReferenceSystem.WGS84, 12.13413478, 56.1343457 )
        } ) );
        values.add( Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 666 ) );
        values.add( Values.pointArray( new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.7566548, 56.7163465, 666 ),
                Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.13413478, 56.1343457, 555 )
        } ) );
        values.add( Values.pointValue( CoordinateReferenceSystem.Cartesian, 0.0000043, -0.0000000012341025786543 ) );
        values.add( Values.pointArray( new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.Cartesian, 0.0000043, -0.0000000012341025786543 ),
                Values.pointValue( CoordinateReferenceSystem.Cartesian, 0.2000043, -0.0300000012341025786543 )
        } ) );
        values.add( Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 0.0000043, -0.0000000012341025786543, 666 ) );
        values.add( Values.pointArray( new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 0.0000043, -0.0000000012341025786543, 666 ),
                Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 0.2000043, -0.0300000012341025786543, 555 )
        } ) );
    }

    @Override
    protected String zipName()
    {
        return "current-generic-key-state-format.zip";
    }

    @Override
    protected String storeFileName()
    {
        return "generic-key-state-store";
    }

    @Override
    protected void createStoreFile( File storeFile ) throws IOException
    {
        withCursor( storeFile, true, c -> {
            putFormatVersion( c );
            putData( c );
        } );
    }

    @Override
    protected void verifyFormat( File storeFile ) throws FormatViolationException, IOException
    {
        AtomicReference<FormatViolationException> exception = new AtomicReference<>();
        withCursor( storeFile, false, c ->
        {
            int major = c.getInt();
            int minor = c.getInt();
            GenericLayout layout = getLayout();
            if ( major != layout.majorVersion() || minor != layout.minorVersion() )
            {
                exception.set( new FormatViolationException( String.format( "Read format version %d.%d, but layout has version %d.%d",
                        major, minor, layout.majorVersion(), layout.minorVersion() ) ) );
            }
        } );
        if ( exception.get() != null )
        {
            throw exception.get();
        }
    }

    @Override
    protected void verifyContent( File storeFile ) throws IOException
    {
        withCursor( storeFile, false, c ->
        {
            readFormatVersion( c );
            verifyData( c );
        } );
    }

    private void putFormatVersion( PageCursor cursor )
    {
        GenericLayout layout = getLayout();
        int major = layout.majorVersion();
        cursor.putInt( major );
        int minor = layout.minorVersion();
        cursor.putInt( minor );
    }

    private void readFormatVersion( PageCursor c )
    {
        c.getInt(); // Major version
        c.getInt(); // Minor version
    }

    private void putData( PageCursor c )
    {
        GenericLayout layout = getLayout();
        GenericKey key = layout.newKey();
        for ( Value value : values )
        {
            initializeFromValue( key, value );
            c.putInt( key.size() );
            layout.writeKey( c, key );
        }
    }

    private void initializeFromValue( GenericKey key, Value value )
    {
        key.initialize( ENTITY_ID );
        for ( int i = 0; i < NUMBER_OF_SLOTS; i++ )
        {
            key.initFromValue( i, value, NativeIndexKey.Inclusion.NEUTRAL );
        }
    }

    private void verifyData( PageCursor c )
    {
        GenericLayout layout = getLayout();
        GenericKey readCompositeKey = layout.newKey();
        GenericKey comparison = layout.newKey();
        for ( Value value : values )
        {
            int keySize = c.getInt();
            layout.readKey( c, readCompositeKey, keySize );
            for ( Value readValue : readCompositeKey.asValues() )
            {
                initializeFromValue( comparison, value );
                assertEquals( 0, layout.compare( readCompositeKey, comparison ), detailedFailureMessage( readCompositeKey, comparison ) );
                if ( readValue != Values.NO_VALUE )
                {
                    assertEquals( value, readValue, "expected read value to be " + value + ", but was " + readValue );
                }
            }
        }
    }

    private String detailedFailureMessage( GenericKey actualKey, GenericKey expectedKey )
    {
        return "expected " + expectedKey.toDetailedString() + ", but was " + actualKey.toDetailedString();
    }

    private GenericLayout getLayout()
    {
        return new GenericLayout( NUMBER_OF_SLOTS,
                new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() ) );
    }

    private void withCursor( File storeFile, boolean create, Consumer<PageCursor> cursorConsumer ) throws IOException
    {
        OpenOption[] openOptions = create ?
                                   new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE} :
                                   new OpenOption[]{StandardOpenOption.WRITE};
        try ( PageCache pageCache = pageCacheRule.getPageCache( globalFs.get() );
              PagedFile pagedFile = pageCache.map( storeFile, pageCache.pageSize(), openOptions );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            cursorConsumer.accept( cursor );
        }
    }
}
