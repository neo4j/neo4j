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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.LazyIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class OnlineIndexUpdatesValidatorTest
{
    private final NeoStores neoStores = mock( NeoStores.class );

    private final IndexingService indexingService = mock( IndexingService.class );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final PropertyLoader propertyLoader = mock( PropertyLoader.class );

    private final PrimitiveLongObjectMap<NodeCommand> emptyNodeCommands = Primitive.longObjectMap();
    private final PrimitiveLongObjectMap<List<PropertyCommand>> emptyPropCommands = Primitive.longObjectMap();

    @Test
    public void shouldValidateIndexesOnNodeCommands() throws IOException
    {
        // Given
        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        NodeRecord before = new NodeRecord( 11 );
        NodeRecord after = new NodeRecord( 12 );
        after.setId( 5 );
        NodeCommand command = new NodeCommand().init( before, after );

        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.<Command>asList( command ) );

        // When
        ValidatedIndexUpdates updates = validator.validate( tx );

        // Then
        assertNotSame( updates, ValidatedIndexUpdates.NONE );

        LazyIndexUpdates expectedUpdates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                emptyPropCommands, groupedById( command ) );

        verify( indexingService ).validate( eq( expectedUpdates ), eq( ONLINE ) );
    }

    @Test
    public void shouldValidateIndexUpdatesOnPropertyCommandsWhenThePropertyIsOnANode() throws IOException
    {
        // Given
        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        PropertyRecord before = new PropertyRecord( 11 );
        PropertyRecord after = new PropertyRecord( 12 );
        after.setNodeId( 42 );
        PropertyCommand command = new PropertyCommand().init( before, after );

        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.<Command>asList( command ) );

        // When
        ValidatedIndexUpdates updates = validator.validate( tx );

        // Then
        assertNotSame( updates, ValidatedIndexUpdates.NONE );

        LazyIndexUpdates expectedUpdates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                groupedByNodeId( command ), emptyNodeCommands );

        verify( indexingService ).validate( expectedUpdates, ONLINE );
    }

    @Test
    public void shouldNotUpdateIndexesOnPropertyCommandsWhenThePropertyIsNotOnANode() throws Exception
    {
        // Given
        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        PropertyRecord before = new PropertyRecord( 11 );
        PropertyRecord after = new PropertyRecord( 12 );
        PropertyCommand command = new PropertyCommand().init( before, after );

        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.<Command>asList( command ) );

        // When
        ValidatedIndexUpdates updates = validator.validate( tx );

        // Then
        assertSame( updates, ValidatedIndexUpdates.NONE );
        verify( indexingService, never() ).validate( any( LazyIndexUpdates.class ), any( IndexUpdateMode.class ) );
    }

    @Test
    public void shouldRethrowExceptionThrownByIndexUpdatersValidationProcedure() throws IOException
    {
        // Given
        long node = 1;
        int label = 2;
        int property = 2;

        NodePropertyCommands commands = createNodeWithLabelAndPropertyCommands( node, label, property );
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        LazyIndexUpdates updates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                commands.property(), commands.node() );

        IndexCapacityExceededException error = new IndexCapacityExceededException( 100, 100 );
        doThrow( new UnderlyingStorageException( error ) ).when( indexingService ).validate( updates, ONLINE );

        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        try
        {
            // When
            validator.validate( tx );
            fail( "Should have thrown " + UnderlyingStorageException.class.getSimpleName() );
        }
        catch ( UnderlyingStorageException e )
        {
            // Then
            assertSame( error, e.getCause() );
        }
    }

    @Test
    public void shouldValidateIndexUpdatesWhenNodeCreated() throws IOException
    {
        // Given
        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        NodeCommand createNode1 = createNodeWithLabel( 1, 1 );
        NodeCommand createNode2 = createNodeWithLabel( 2, 2 );

        PropertyCommand addProperty1ToNode1 = addProperty( 1, 1 );
        PropertyCommand addProperty2ToNode1 = addProperty( 1, 2 );

        PropertyCommand addProperty1ToNode2 = addProperty( 2, 2 );

        TransactionRepresentation tx = new PhysicalTransactionRepresentation( asList(
                createNode1, addProperty1ToNode1, addProperty2ToNode1,
                createNode2, addProperty1ToNode2
        ) );

        // When
        ValidatedIndexUpdates validatedUpdates = validator.validate( tx );

        // Then
        assertNotSame( ValidatedIndexUpdates.NONE, validatedUpdates );

        LazyIndexUpdates expectedUpdates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                groupedByNodeId( addProperty1ToNode1, addProperty2ToNode1, addProperty1ToNode2 ),
                groupedById( createNode1, createNode2 ) );

        verify( indexingService ).validate( expectedUpdates, ONLINE );
    }

    @Test
    public void shouldValidateIndexUpdatesWhenNodeDeleted() throws IOException
    {
        // Given
        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        NodeCommand deleteNode1 = deleteNode( 1 );
        NodeCommand deleteNode2 = deleteNode( 1 );
        TransactionRepresentation tx = new PhysicalTransactionRepresentation(
                Arrays.<Command>asList( deleteNode1, deleteNode2 )
        );

        // When
        ValidatedIndexUpdates validatedUpdates = validator.validate( tx );

        // Then
        assertNotSame( ValidatedIndexUpdates.NONE, validatedUpdates );

        LazyIndexUpdates expectedUpdates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                emptyPropCommands, groupedById( deleteNode1, deleteNode2 ) );

        verify( indexingService ).validate( expectedUpdates, ONLINE );
    }

    private IndexUpdatesValidator newIndexUpdatesValidatorWithMockedDependencies()
    {
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
        return new OnlineIndexUpdatesValidator( neoStores, null, propertyLoader, indexingService, ONLINE );
    }

    private static NodePropertyCommands createNodeWithLabelAndPropertyCommands( long nodeId, int label, int property )
    {
        NodeCommand nodeCommand = createNodeWithLabel( nodeId, label );
        PropertyCommand propCommand = addProperty( nodeId, property );
        return new NodePropertyCommands( nodeCommand, propCommand );
    }

    private static NodeCommand createNodeWithLabel( long nodeId, int label )
    {
        NodeRecord before = new NodeRecord( nodeId, false, false, NO_NEXT_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue(), NO_LABELS_FIELD.intValue() );
        NodeRecord after = new NodeRecord( nodeId, true, false, NO_NEXT_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue(), NO_LABELS_FIELD.intValue() );
        new InlineNodeLabels( after.getLabelField(), after ).put( new long[]{label}, null, null );
        return new NodeCommand().init( before, after );
    }

    private static NodeCommand deleteNode( long nodeId )
    {
        NodeRecord before = new NodeRecord( nodeId, true, false, 42, 42, NO_LABELS_FIELD.intValue() );
        new InlineNodeLabels( before.getLabelField(), before ).put( new long[]{42}, null, null );
        NodeRecord after = new NodeRecord( nodeId, false, false, NO_NEXT_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue(), NO_LABELS_FIELD.intValue() );
        return new NodeCommand().init( before, after );
    }

    private static PropertyCommand addProperty( long nodeId, int property )
    {
        PropertyRecord before = new PropertyRecord( 1 );
        before.setInUse( false );
        PropertyRecord after = new PropertyRecord( 2 );
        after.setInUse( true );
        after.setCreated();
        after.setNodeId( nodeId );
        PropertyBlock block = new PropertyBlock();
        block.setSingleBlock( 42 );
        block.setKeyIndexId( property );
        after.setPropertyBlock( block );
        return new PropertyCommand().init( before, after );
    }

    private static PrimitiveLongObjectMap<NodeCommand> groupedById( NodeCommand... commands )
    {
        PrimitiveLongObjectMap<NodeCommand> result = Primitive.longObjectMap( commands.length );
        for ( NodeCommand command : commands )
        {
            result.put( command.getAfter().getId(), command );
        }
        return result;
    }

    private static PrimitiveLongObjectMap<List<PropertyCommand>> groupedByNodeId( PropertyCommand... commands )
    {
        PrimitiveLongObjectMap<List<PropertyCommand>> result = Primitive.longObjectMap( commands.length );
        for ( PropertyCommand command : commands )
        {
            List<PropertyCommand> propCommands = result.get( command.getNodeId() );
            if ( propCommands == null )
            {
                result.put( command.getNodeId(), propCommands = new ArrayList<>() );
            }
            propCommands.add( command );
        }
        return result;
    }

    private static class NodePropertyCommands extends AbstractCollection<Command>
    {
        final NodeCommand nodeCommand;
        final PropertyCommand propCommand;

        NodePropertyCommands( NodeCommand nodeCommand, PropertyCommand propCommand )
        {
            this.nodeCommand = nodeCommand;
            this.propCommand = propCommand;
        }

        @Override
        public Iterator<Command> iterator()
        {
            return IteratorUtil.iterator( nodeCommand, propCommand );
        }

        @Override
        public int size()
        {
            return IteratorUtil.count( iterator() );
        }

        PrimitiveLongObjectMap<List<PropertyCommand>> property()
        {
            return groupedByNodeId( propCommand );
        }

        PrimitiveLongObjectMap<NodeCommand> node()
        {
            return groupedById( nodeCommand );
        }
    }
}
