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
package org.neo4j.procedure.builtin;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.procedure.builtin.SchemaStatementProcedure.createSchemaStatementResults;

class SchemaStatementProcedureTest
{
    private static final String CONSTRAINT_NAME = "constraint name";
    private static final String NAME_WITH_BACKTICKS = "``ugly`name`";
    private static final String ESCAPED_NAME_WITH_BACKTICKS = "`````ugly``name```";
    private static final String LABEL_WITH_BACKTICKS = "```label";
    private static final String ESCAPED_LABEL_WITH_BACKTICKS = "```````label`";
    private static final String PROPERTY_KEY_WITH_BACKTICKS = "prop`";
    private static final String ESCAPED_PROPERTY_KEY_WITH_BACKTICKS = "`prop```";

    @Test
    void schemaStatementsMustIncludeOnlineIndexes() throws IndexNotFoundKernelException, ProcedureException
    {
        IndexDescriptor index = someIndex();
        InternalIndexState indexState = InternalIndexState.ONLINE;
        SchemaReadCore schemaReadCore = getSchemaReadCore( index, indexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        assertEquals( 1, result.size() );
    }

    @Test
    void schemaStatementsMustNotIncludePopulatingIndexes() throws ProcedureException, IndexNotFoundKernelException
    {
        IndexDescriptor index = someIndex();
        InternalIndexState indexState = InternalIndexState.POPULATING;
        SchemaReadCore schemaReadCore = getSchemaReadCore( index, indexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        assertEquals( 0, result.size() );
    }

    @Test
    void schemaStatementsMustNotIncludeFailedIndexes() throws IndexNotFoundKernelException, ProcedureException
    {
        IndexDescriptor index = someIndex();
        InternalIndexState indexState = InternalIndexState.FAILED;
        SchemaReadCore schemaReadCore = getSchemaReadCore( index, indexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        assertEquals( 0, result.size() );
    }

    @Test
    void schemaStatementsMustNotIncludeOrphanedIndexes() throws IndexNotFoundKernelException, ProcedureException
    {
        IndexDescriptor index = someOrphanedIndex();
        InternalIndexState indexState = InternalIndexState.ONLINE;
        SchemaReadCore schemaReadCore = getSchemaReadCore( index, indexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        assertEquals( 0, result.size() );
    }

    @Test
    void schemaStatementsMustOnlyIncludeIndexBackedConstraintNotActualIndex() throws IndexNotFoundKernelException, ProcedureException
    {
        IndexDescriptor index = someOrphanedIndex();
        ConstraintDescriptor constraint = indexBackedConstraint( index );
        index = indexBoundToConstraint( index, constraint );
        InternalIndexState internalIndexState = InternalIndexState.ONLINE;
        SchemaReadCore schemaReadCore = getSchemaReadCore( constraint, index, internalIndexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        Iterator<BuiltInProcedures.SchemaStatementResult> iter = result.iterator();
        assertTrue( iter.hasNext() );
        BuiltInProcedures.SchemaStatementResult next = iter.next();
        assertEquals( SchemaStatementProcedure.SchemaRuleType.CONSTRAINT.name(), next.type );
        assertEquals( CONSTRAINT_NAME, next.name );
        assertFalse( iter.hasNext() );
    }

    @Test
    void schemaStatementsMustNotIncludeIndexBackedConstraintsWithFailedIndexIndex() throws IndexNotFoundKernelException, ProcedureException
    {
        IndexDescriptor index = someOrphanedIndex();
        ConstraintDescriptor constraint = indexBackedConstraint( index );
        index = indexBoundToConstraint( index, constraint );
        InternalIndexState internalIndexState = InternalIndexState.FAILED;
        SchemaReadCore schemaReadCore = getSchemaReadCore( constraint, index, internalIndexState );
        TokenRead tokenRead = mock( TokenRead.class );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        assertEquals( 0, result.size() );
    }

    @Test
    void schemaStatementsShouldHandleIndexWithBackticks()
            throws IndexNotFoundKernelException, ProcedureException, LabelNotFoundKernelException, PropertyKeyIdNotFoundKernelException
    {
        IndexDescriptor index = forSchema( forLabel( 1, 1 ) ).withName( NAME_WITH_BACKTICKS ).materialise( 1 );
        InternalIndexState internalIndexState = InternalIndexState.ONLINE;
        SchemaReadCore schemaReadCore = getSchemaReadCore( index, internalIndexState );
        TokenRead tokenRead = mock( TokenRead.class );
        when( tokenRead.nodeLabelName( 1 )).thenReturn( LABEL_WITH_BACKTICKS );
        when( tokenRead.propertyKeyName( 1 )).thenReturn( PROPERTY_KEY_WITH_BACKTICKS );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        Iterator<BuiltInProcedures.SchemaStatementResult> iter = result.iterator();
        assertTrue( iter.hasNext() );
        BuiltInProcedures.SchemaStatementResult next = iter.next();
        assertEquals( NAME_WITH_BACKTICKS, next.name );
        assertEquals( format( "CALL db.createIndex('%s', ['%s'], ['%s'], 'Undecided-0', {})",
                NAME_WITH_BACKTICKS, LABEL_WITH_BACKTICKS, PROPERTY_KEY_WITH_BACKTICKS), next.createStatement );
        assertEquals( format( "DROP INDEX %s", ESCAPED_NAME_WITH_BACKTICKS ), next.dropStatement );
        assertFalse( iter.hasNext() );
    }

    @Test
    void schemaStatementsShouldHandleConstraintWithBackticks()
            throws IndexNotFoundKernelException, ProcedureException, LabelNotFoundKernelException, PropertyKeyIdNotFoundKernelException
    {
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( forLabel( 1, 1 ) ).withName( NAME_WITH_BACKTICKS );
        SchemaReadCore schemaReadCore = getSchemaReadCore( constraint );
        TokenRead tokenRead = mock( TokenRead.class );
        when( tokenRead.nodeLabelName( 1 )).thenReturn( LABEL_WITH_BACKTICKS );
        when( tokenRead.propertyKeyName( 1 )).thenReturn( PROPERTY_KEY_WITH_BACKTICKS );

        Collection<BuiltInProcedures.SchemaStatementResult> result = createSchemaStatementResults( schemaReadCore, tokenRead );
        Iterator<BuiltInProcedures.SchemaStatementResult> iter = result.iterator();
        assertTrue( iter.hasNext() );
        BuiltInProcedures.SchemaStatementResult next = iter.next();
        assertEquals( NAME_WITH_BACKTICKS, next.name );
        assertEquals( format( "CREATE CONSTRAINT %s ON (a:%s) ASSERT exists(a.%s)",
                ESCAPED_NAME_WITH_BACKTICKS, ESCAPED_LABEL_WITH_BACKTICKS, ESCAPED_PROPERTY_KEY_WITH_BACKTICKS), next.createStatement );
        assertEquals( format( "DROP CONSTRAINT %s", ESCAPED_NAME_WITH_BACKTICKS ), next.dropStatement );
        assertEquals( NAME_WITH_BACKTICKS, next.name );
        assertFalse( iter.hasNext() );
    }

    private IndexDescriptor indexBoundToConstraint( IndexDescriptor index, ConstraintDescriptor constraint )
    {
        index = index.withOwningConstraintId( constraint.getId() );
        return index.withName( constraint.getName() );
    }

    private ConstraintDescriptor indexBackedConstraint( IndexDescriptor backingIndex )
    {
        return ConstraintDescriptorFactory.uniqueForSchema( backingIndex.schema() )
                .withOwnedIndexId( backingIndex.getId() )
                .withId( backingIndex.getId() + 1 )
                .withName( CONSTRAINT_NAME );
    }

    private IndexDescriptor someIndex()
    {
        return forSchema( forLabel( 1, 1 ) ).withName( "index" ).materialise( 1 );
    }

    private IndexDescriptor someOrphanedIndex()
    {
        IndexDescriptor index = uniqueForSchema( forLabel( 1, 1 ) ).withName( "index" ).materialise( 1 );
        assertTrue( index.isUnique() );
        assertFalse( index.getOwningConstraintId().isPresent() );
        return index;
    }

    private SchemaReadCore getSchemaReadCore( IndexDescriptor index, InternalIndexState indexState ) throws IndexNotFoundKernelException
    {
        return getSchemaReadCore( null, index, indexState );
    }

    private SchemaReadCore getSchemaReadCore( ConstraintDescriptor constraint ) throws IndexNotFoundKernelException
    {
        return getSchemaReadCore( constraint, null, null );
    }

    private SchemaReadCore getSchemaReadCore( ConstraintDescriptor constraint, IndexDescriptor index, InternalIndexState indexState )
            throws IndexNotFoundKernelException
    {
        SchemaReadCore schemaReadCore = mock( SchemaReadCore.class );
        when( schemaReadCore.indexesGetAll() ).thenReturn( iterator() );
        when( schemaReadCore.constraintsGetAll() ).thenReturn( iterator() );

        if ( index != null )
        {
            when( schemaReadCore.indexesGetAll() ).thenReturn( singleton( index ).iterator() );
            when( schemaReadCore.indexGetForName( index.getName() ) ).thenReturn( index );
            when( schemaReadCore.indexGetState( index ) ).thenReturn( indexState );
        }

        if ( constraint != null )
        {
            when( schemaReadCore.constraintsGetAll() ).thenReturn( singleton( constraint ).iterator() );
        }
        return schemaReadCore;
    }
}
