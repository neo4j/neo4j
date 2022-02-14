/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.index.lucene;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.index.schema.BuiltInDelegatingIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.RangeIndexProviderFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.FailureType.INITIAL_STATE;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.INITIAL_STATE_FAILURE_MESSAGE;

@TestDirectoryExtension
class ConstraintIndexFailureIT
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldFailToValidateConstraintsIfUnderlyingIndexIsFailed() throws Exception
    {
        // given a perfectly normal constraint
        Path dir = directory.homePath();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( dir )
                // use delegating index provider with custom descriptor, so it can be replaced with failing provider
                .addExtension( new BuiltInDelegatingIndexProviderFactory( new RangeIndexProviderFactory(), DESCRIPTOR ) )
                .noOpSystemGraphInitializer()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( TransactionImpl tx = (TransactionImpl) db.beginTx() )
        {
            IndexingTestUtil.createNodePropUniqueConstraintWithSpecifiedProvider( tx, DESCRIPTOR, label( "Label1" ), "key1" );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }

        // Remove the indexes offline and start up with an index provider which reports FAILED as initial state. An ordeal, I know right...
        FileUtils.deleteDirectory( IndexDirectoryStructure.baseSchemaIndexFolder( dir ) );
        managementService = new TestDatabaseManagementServiceBuilder( dir )
                .addExtension( new FailingGenericNativeIndexProviderFactory( new RangeIndexProviderFactory(), INITIAL_STATE ) )
                .noOpSystemGraphInitializer()
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        // when
        try ( Transaction tx = db.beginTx() )
        {
            var e = assertThrows( ConstraintViolationException.class, () -> tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" ) );
            assertThat( e.getCause() ).isInstanceOf( UnableToValidateConstraintException.class );
            assertThat( e.getCause().getCause().getMessage() ).contains( "The index is in a failed state:" ).contains( INITIAL_STATE_FAILURE_MESSAGE );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
