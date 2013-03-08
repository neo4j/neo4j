/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;

import javax.transaction.SystemException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class TxManagerTest
{
    @Test
    public void settingTmNotOkShouldAttachCauseToSubsequentErrors() throws Exception
    {
        // Given
        XaDataSourceManager mockXaManager = mock( XaDataSourceManager.class );
        TxManager txm = new TxManager( new File( "/dev/null" ), mockXaManager, new KernelPanicEventGenerator(
                new KernelEventHandlers() ), StringLogger.DEV_NULL, fs.get(), null );
        txm.doRecovery(); // Make the txm move to an ok state

        String msg = "These kinds of throwables, breaking our transaction managers, are why we can't have nice things.";

        // When
        txm.setTmNotOk( new Throwable( msg ) );

        // Then
        try
        {
            txm.begin();
            fail( "Should have thrown SystemException." );
        }
        catch ( SystemException topLevelException )
        {
            assertThat( "TM should forward a cause.", topLevelException.getCause(), is( Throwable.class ) );
            assertThat( "Cause should be the original cause", topLevelException.getCause().getMessage(), is( msg ) );
        }

    }

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
