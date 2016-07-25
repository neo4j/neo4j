/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.getThresholdByType;

public class LogPruneStrategyFactoryTest
{
    @Test
    public void testLogPruneThresholdsByType() throws Exception
    {
        FileSystemAbstraction fsa = Mockito.mock( FileSystemAbstraction.class );

        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "files", 25 ), "" ), instanceOf( FileCountThreshold.class ) );
        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "size", 16000 ), "" ), instanceOf( FileSizeThreshold.class ) );
        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "txs", 4000 ), "" ), instanceOf( EntryCountThreshold.class ) );
        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "entries", 4000 ), "" ), instanceOf( EntryCountThreshold.class ) );
        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "hours", 100 ), "" ), instanceOf( EntryTimespanThreshold.class ) );
        assertThat( getThresholdByType( fsa, new ThresholdConfigValue( "days", 100_000 ), "" ), instanceOf( EntryTimespanThreshold.class) );
    }
}
