/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.schema;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.kernel.api.security.AnonymousContext.read;
import static org.neo4j.values.storable.BooleanValue.FALSE;

@DbmsExtension
class SchemaReadIT
{
    private static final String INDEX_NAME = "testIndex";
    private static final String PROPERTY_NAME = "property";

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private Kernel kernel;

    @Test
    void trackPageCacheAccessOnIndexNodeCount() throws KernelException
    {
        var label = Label.label( "foo" );
        createIndex( label );
        try ( var tx = kernel.beginTransaction( IMPLICIT, read() ) )
        {
            var cursorTracer = tx.pageCursorTracer();
            var indexDescriptor = tx.schemaRead().indexGetForName( INDEX_NAME );

            assertThat( cursorTracer.faults() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.pins() ).isZero();

            tx.schemaRead().nodesCountIndexed( indexDescriptor, 0, 0, FALSE );

            assertThat( cursorTracer.faults() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.pins() ).isOne();
        }
    }

    private void createIndex( Label label )
    {
        try ( var transaction = db.beginTx() )
        {
            transaction.schema().indexFor( label ).on( PROPERTY_NAME ).withName( INDEX_NAME ).create();
            transaction.commit();
        }
        try ( var transaction = db.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, HOURS );
        }
    }
}
