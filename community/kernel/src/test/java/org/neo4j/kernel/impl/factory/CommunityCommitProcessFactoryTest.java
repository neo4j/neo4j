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
package org.neo4j.kernel.impl.factory;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CommunityCommitProcessFactoryTest
{
    @Test
    public void createReadOnlyCommitProcess()
    {
        CommunityCommitProcessFactory factory = new CommunityCommitProcessFactory();

        Config config = new Config( stringMap( GraphDatabaseSettings.read_only.name(), "true" ) );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( TransactionRepresentationStoreApplier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( IndexUpdatesValidator.class ), config );

        assertThat( commitProcess, instanceOf( ReadOnlyTransactionCommitProcess.class ) );
    }

    @Test
    public void createRegularCommitProcess()
    {
        CommunityCommitProcessFactory factory = new CommunityCommitProcessFactory();

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( TransactionRepresentationStoreApplier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( IndexUpdatesValidator.class ), new Config() );

        assertThat( commitProcess, instanceOf( TransactionRepresentationCommitProcess.class ) );
    }
}
