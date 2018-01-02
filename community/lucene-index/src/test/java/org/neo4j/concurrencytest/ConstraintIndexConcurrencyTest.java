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
package org.neo4j.concurrencytest;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.kernel.api.properties.Property.property;

public class ConstraintIndexConcurrencyTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Test
    public void shouldNotAllowConcurrentViolationOfConstraint() throws Exception
    {
        // Given
        GraphDatabaseAPI graphDb = db.getGraphDatabaseAPI();

        Supplier<Statement> statementSupplier = graphDb.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class );

        Label label = label( "Foo" );
        String propertyKey = "bar";
        String conflictingValue = "baz";

        // a constraint
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }

        // When
        try ( Transaction tx = graphDb.beginTx() )
        {
            // create a statement and perform a lookup
            Statement statement = statementSupplier.get();
            int labelId = statement.readOperations().labelGetForName( label.name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
            statement.readOperations().nodesGetFromIndexSeek( new IndexDescriptor( labelId, propertyKeyId ),
                    "The value is irrelevant, we just want to perform some sort of lookup against this index" );

            // then let another thread come in and create a node
            threads.execute( createNode( label, propertyKey, conflictingValue ), graphDb ).get();

            // before we create a node with the same property ourselves - using the same statement that we have
            // already used for lookup against that very same index
            long node = statement.dataWriteOperations().nodeCreate();
            statement.dataWriteOperations().nodeAddLabel( node, labelId );
            try
            {
                statement.dataWriteOperations().nodeSetProperty( node, property( propertyKeyId, conflictingValue ) );

                fail( "exception expected" );
            }
            // Then
            catch ( UniquePropertyConstraintViolationKernelException e )
            {
                assertEquals( labelId, e.labelId() );
                assertEquals( propertyKeyId, e.propertyKeyId() );
                assertEquals( conflictingValue, e.propertyValue() );
            }

            tx.success();
        }
    }

    private static Function<GraphDatabaseService,Void> createNode(
            final Label label, final String key, final Object value )
    {
        return new Function<GraphDatabaseService,Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    graphDb.createNode( label ).setProperty( key, value );
                    tx.success();
                }
                return null;
            }
        };
    }
}
