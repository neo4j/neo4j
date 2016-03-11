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

import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV3_0;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.record_format;

public class InternalRecordFormatSelectorTest
{
    @Test
    public void shouldResolveHighLimitsRecordFormat() throws Exception
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), HighLimit.NAME ) );
        RecordFormats formatSelector = InternalRecordFormatSelector.select( config, NullLogService.getInstance() );
        assertEquals( HighLimit.RECORD_FORMATS.storeVersion(), formatSelector.storeVersion() );
    }

    @Test
    public void shouldResolveCommunityRecordFormat() throws Exception
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), LowLimitV3_0.NAME ) );
        RecordFormats formatSelector = InternalRecordFormatSelector.select( config, NullLogService.getInstance() );
        assertEquals( LowLimitV3_0.RECORD_FORMATS.storeVersion(), formatSelector.storeVersion() );
    }

    @Test
    public void shouldResolveNoRecordFormatToHighLimitDefault() throws Exception
    {
        Config config = Config.empty();
        RecordFormats formatSelector = InternalRecordFormatSelector.select( config, NullLogService.getInstance() );
        assertEquals( HighLimit.RECORD_FORMATS.storeVersion(), formatSelector.storeVersion() );
    }

    @Test
    public void shouldNotResolveNoneExistingRecordFormat() throws Exception
    {
        Config config = new Config( MapUtil.stringMap( record_format.name(), "notAValidRecordFormat" ) );
        try
        {
            RecordFormats formatSelector = InternalRecordFormatSelector.select( config, NullLogService.getInstance() );
            assertNotNull( formatSelector );
            fail( "Should not be possible to specify non-existing format" );
        }
        catch ( IllegalArgumentException ignored )
        {
            // Success
        }
    }
}
