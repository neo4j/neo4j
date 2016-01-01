/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.Map;

import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.Configuration;

public class StoreFactoryTest
{
    @Test
    public void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup() throws Exception
    {
        // Given
        StoreFactory storeFactory = createStoreFactory();

        // When
        NeoStore neostore = storeFactory.createNeoStore( new File( "neostore" ) );

        // Then
        assertThat( neostore.getUpgradeId(), equalTo( neostore.getRandomNumber() ) );
        assertThat( neostore.getUpgradeTime(), equalTo( neostore.getCreationTime() ) );
    }

    private static StoreFactory createStoreFactory()
    {
        Map<String, String> configParams = stringMap(
                Configuration.read_only.name(), "false",
                Configuration.backup_slave.name(), "false" );

        return new StoreFactory( new Config( configParams ), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new EphemeralFileSystemAbstraction(), StringLogger.DEV_NULL,
                mock( RemoteTxHook.class ) );
    }
}
