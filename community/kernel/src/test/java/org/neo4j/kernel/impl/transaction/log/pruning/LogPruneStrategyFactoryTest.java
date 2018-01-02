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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class LogPruneStrategyFactoryTest
{

    @Test
    public void testLogPruneThresholdsByType() throws Exception
    {
        assertThat( getPruneStrategy( "files", "25", "25 files" ), instanceOf( FileCountThreshold.class ) );
        assertThat( getPruneStrategy( "size", "16G", "16G size" ), instanceOf( FileSizeThreshold.class ) );
        assertThat( getPruneStrategy( "txs", "4G", "4G txs" ), instanceOf( TransactionCountThreshold.class ) );
        assertThat( getPruneStrategy( "hours", "100", "100 hours" ), instanceOf( TransactionTimespanThreshold.class ) );
        assertThat( getPruneStrategy( "days", "100k", "100k days" ),
                    instanceOf( TransactionTimespanThreshold.class) );
    }

    private Threshold getPruneStrategy(String type, String value, String configValue)
    {
        FileSystemAbstraction fileSystem = Mockito.mock( FileSystemAbstraction.class );
        return LogPruneStrategyFactory.getThresholdByType( fileSystem, type, value, configValue );
    }
}
