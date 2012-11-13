/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.fail;
import static org.neo4j.kernel.CommonFactories.defaultFileSystemAbstraction;
import static org.neo4j.kernel.CommonFactories.defaultLogBufferFactory;
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;

public class TestUpgradeOneDotFourToFiveIT
{
    private static final File PATH = new File( "target/test-data/upgrade-1.4-5" );

    @BeforeClass
    public static void doBefore() throws Exception
    {
        deleteRecursively( PATH );
    }

    @Test( expected=IllegalLogFormatException.class )
    public void cannotRecoverNoncleanShutdownDbWithOlderLogFormat() throws Exception
    {
        copyRecursively( new File( TestUpgradeOneDotFourToFiveIT.class.getResource( "non-clean-1.4.2-db/neostore" ).getFile() ).getParentFile(), PATH );
//        Map<Object, Object> config = new HashMap<Object, Object>();
//        config.put( "store_dir", PATH.getAbsolutePath() );
//        config.put( StringLogger.class, StringLogger.DEV_NULL );
//        config.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
//        config.put( LogBufferFactory.class, CommonFactories.defaultLogBufferFactory() );
        
        XaLogicalLog log = new XaLogicalLog( resourceFile(), null, null, null, defaultLogBufferFactory(), defaultFileSystemAbstraction(), DEV_NULL,
                LogPruneStrategies.NO_PRUNING, TransactionStateFactory.NO_STATE_FACTORY );
        log.open();
        fail( "Shouldn't be able to start" );
    }

    protected String resourceFile()
    {
        return new File( PATH, "nioneo_logical.log" ).getAbsolutePath();
    }
}
