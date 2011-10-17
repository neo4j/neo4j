/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;

public class TestUpgradeOneDotFourToFive
{
    private static final File PATH = new File( "target/test-data/upgrade-1.4-5" );
    
    @BeforeClass
    public static void doBefore() throws Exception
    {
        deleteRecursively( PATH );
    }
    
    @Test
    public void cannotRecoverNoncleanShutdownDbWithOlderLogFormat() throws Exception
    {
        copyRecursively( new File( TestUpgradeOneDotFourToFive.class.getResource( "non-clean-1.4.2-db/neostore" ).getFile() ).getParentFile(), PATH );
        try
        {
            new EmbeddedGraphDatabase( PATH.getAbsolutePath() );
            fail( "Shouldn't be able to start" );
        }
        catch ( TransactionFailureException e )
        {
            assertTrue( IllegalLogFormatException.class.isAssignableFrom( e.getCause().getCause().getClass() ) );
        }
    }
}
