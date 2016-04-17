/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class RecordFormatSelectorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void selectSpecifiedRecordFormat() throws Exception
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), HighLimit.NAME ) );
        RecordFormats formatSelector = RecordFormatSelector.select( config,
                StandardV3_0.RECORD_FORMATS, NullLogService.getInstance() );
        assertEquals( HighLimit.RECORD_FORMATS.storeVersion(), formatSelector.storeVersion() );
    }

    @Test
    public void selectDefaultFormatByDefault() throws Exception
    {
        Config config = Config.empty();
        RecordFormats formatSelector = RecordFormatSelector.select( config,
                StandardV3_0.RECORD_FORMATS, NullLogService.getInstance() );
        assertEquals( StandardV3_0.RECORD_FORMATS.storeVersion(), formatSelector.storeVersion() );
    }

    @Test
    public void shouldNotResolveNoneExistingRecordFormat() throws Exception
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), "notAValidRecordFormat" ) );
        expectedException.expect( IllegalArgumentException.class );

        RecordFormatSelector.select( config, StandardV3_0.RECORD_FORMATS, NullLogService.getInstance() );
    }

    @Test
    public void autoSelectStandardFormat()
    {
        RecordFormats recordFormats = RecordFormatSelector.autoSelectFormat();
        assertEquals( "Autoselectable format should be equal to default format.", recordFormats,
                StandardV3_0.RECORD_FORMATS );
    }

    @Test
    public void autoselectCommunityFormat()
    {
        RecordFormats recordFormats = RecordFormatSelector.autoSelectFormat( Config.empty(), NullLogService.getInstance() );
        assertEquals( "autoselect should select specified format.", recordFormats, StandardV3_0.RECORD_FORMATS );
    }

    @Test
    public void overrideWithNonExistingFormatFailure()
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), "notAValidRecordFormat" ) );
        expectedException.expect( IllegalArgumentException.class );

        RecordFormatSelector.autoSelectFormat( config, NullLogService.getInstance() );
    }
}
