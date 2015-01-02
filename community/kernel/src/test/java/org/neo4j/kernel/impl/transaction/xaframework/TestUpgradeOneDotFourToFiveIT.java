/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Unzip;

import static org.junit.Assert.fail;

import static org.neo4j.kernel.CommonFactories.defaultFileSystemAbstraction;
import static org.neo4j.kernel.impl.transaction.xaframework.InjectedTransactionValidator.ALLOW_ALL;

import static org.mockito.Mockito.mock;

public class TestUpgradeOneDotFourToFiveIT
{
    @Test( expected=IllegalLogFormatException.class )
    public void cannotRecoverNoncleanShutdownDbWithOlderLogFormat() throws Exception
    {
        File storeDir = Unzip.unzip( getClass(), "non-clean-1.4.2-db.zip" );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        XaLogicalLog log = new XaLogicalLog( resourceFile( storeDir ), null, null, null,
                defaultFileSystemAbstraction(), new Monitors(), new DevNullLoggingService(), LogPruneStrategies.NO_PRUNING,
                TransactionStateFactory.noStateFactory( new DevNullLoggingService() ), kernelHealth, 25 * 1024 * 1024, ALLOW_ALL );
        log.open();
        fail( "Shouldn't be able to start" );
    }

    protected File resourceFile( File storeDir )
    {
        return new File( storeDir, "nioneo_logical.log" );
    }
}
