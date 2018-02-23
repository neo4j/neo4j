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

import java.util.function.Supplier;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

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
        try ( Transaction tx = graphDb.beginTx();
                Statement statement = statementSupplier.get() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
            IndexDescriptor index = IndexDescriptorFactory.uniqueForLabel( labelId, propertyKeyId );
            statement.readOperations().indexQuery( index, IndexQuery.exact( index.schema().getPropertyId(),
                    "The value is irrelevant, we just want to perform some sort of lookup against this index" ) );

            // then let another thread come in and create a node
            threads.execute( db ->
            {
                try ( Transaction transaction = db.beginTx() )
                {
                    db.createNode( label ).setProperty( propertyKey, conflictingValue );
                    transaction.success();
                }
                return null;
            }, graphDb ).get();

            // before we create a node with the same property ourselves - using the same statement that we have
            // already used for lookup against that very same index
            long node = statement.dataWriteOperations().nodeCreate();
            statement.dataWriteOperations().nodeAddLabel( node, labelId );
            try
            {
                statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( conflictingValue ) );

                fail( "exception expected" );
            }
            // Then
            catch ( UniquePropertyValueValidationException e )
            {
                assertEquals( ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKeyId ), e.constraint() );
                IndexEntryConflictException conflict = Iterators.single( e.conflicts().iterator() );
                assertEquals( Values.stringValue( conflictingValue ), conflict.getSinglePropertyValue() );
            }

            tx.success();
        }
    }
}
