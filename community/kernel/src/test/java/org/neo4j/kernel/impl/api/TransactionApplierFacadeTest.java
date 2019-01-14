/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.Command;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionApplierFacadeTest
{
    private TransactionApplierFacade facade;
    private TransactionApplier txApplier1;
    private TransactionApplier txApplier2;
    private TransactionApplier txApplier3;

    @Before
    public void setUp()
    {
        txApplier1 = mock( TransactionApplier.class );
        txApplier2 = mock( TransactionApplier.class );
        txApplier3 = mock( TransactionApplier.class );

        facade = new TransactionApplierFacade( txApplier1, txApplier2, txApplier3 );

    }

    @Test
    public void testClose() throws Exception
    {
        // WHEN
        facade.close();

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2 ,txApplier3 );

        // Verify reverse order
        inOrder.verify( txApplier3 ).close();
        inOrder.verify( txApplier2 ).close();
        inOrder.verify( txApplier1 ).close();
    }

    @Test
    public void testVisit() throws Exception
    {
        Command cmd = mock( Command.class );

        // WHEN
        boolean result = facade.visit( cmd );

        // THEN
        InOrder inOrder = inOrder( cmd );

        inOrder.verify( cmd ).handle( txApplier1 );
        inOrder.verify( cmd ).handle( txApplier2 );
        inOrder.verify( cmd ).handle( txApplier3 );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitNodeCommand() throws Exception
    {
        Command.NodeCommand cmd = mock( Command.NodeCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitNodeCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitNodeCommand( cmd );
        inOrder.verify( txApplier2 ).visitNodeCommand( cmd );
        inOrder.verify( txApplier3 ).visitNodeCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitRelationshipCommand() throws Exception
    {
        Command.RelationshipCommand cmd = mock( Command.RelationshipCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitRelationshipCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitRelationshipCommand( cmd );
        inOrder.verify( txApplier2 ).visitRelationshipCommand( cmd );
        inOrder.verify( txApplier3 ).visitRelationshipCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitPropertyCommand() throws Exception
    {
        Command.PropertyCommand cmd = mock( Command.PropertyCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitPropertyCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitPropertyCommand( cmd );
        inOrder.verify( txApplier2 ).visitPropertyCommand( cmd );
        inOrder.verify( txApplier3 ).visitPropertyCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitRelationshipGroupCommand() throws Exception
    {
        Command.RelationshipGroupCommand cmd = mock( Command.RelationshipGroupCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitRelationshipGroupCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitRelationshipGroupCommand( cmd );
        inOrder.verify( txApplier2 ).visitRelationshipGroupCommand( cmd );
        inOrder.verify( txApplier3 ).visitRelationshipGroupCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitRelationshipTypeTokenCommand() throws Exception
    {
        Command.RelationshipTypeTokenCommand cmd = mock( Command.RelationshipTypeTokenCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitRelationshipTypeTokenCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitRelationshipTypeTokenCommand( cmd );
        inOrder.verify( txApplier2 ).visitRelationshipTypeTokenCommand( cmd );
        inOrder.verify( txApplier3 ).visitRelationshipTypeTokenCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitLabelTokenCommand() throws Exception
    {
        Command.LabelTokenCommand cmd = mock( Command.LabelTokenCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitLabelTokenCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitLabelTokenCommand( cmd );
        inOrder.verify( txApplier2 ).visitLabelTokenCommand( cmd );
        inOrder.verify( txApplier3 ).visitLabelTokenCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitPropertyKeyTokenCommand() throws Exception
    {
        // Make sure it just calls through to visit
        Command.PropertyKeyTokenCommand cmd = mock( Command.PropertyKeyTokenCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitPropertyKeyTokenCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitPropertyKeyTokenCommand( cmd );
        inOrder.verify( txApplier2 ).visitPropertyKeyTokenCommand( cmd );
        inOrder.verify( txApplier3 ).visitPropertyKeyTokenCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitSchemaRuleCommand() throws Exception
    {
// Make sure it just calls through to visit
        Command.SchemaRuleCommand cmd = mock( Command.SchemaRuleCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitSchemaRuleCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitSchemaRuleCommand( cmd );
        inOrder.verify( txApplier2 ).visitSchemaRuleCommand( cmd );
        inOrder.verify( txApplier3 ).visitSchemaRuleCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitNeoStoreCommand() throws Exception
    {
// Make sure it just calls through to visit
        Command.NeoStoreCommand cmd = mock( Command.NeoStoreCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitNeoStoreCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitNeoStoreCommand( cmd );
        inOrder.verify( txApplier2 ).visitNeoStoreCommand( cmd );
        inOrder.verify( txApplier3 ).visitNeoStoreCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexAddNodeCommand() throws Exception
    {
        IndexCommand.AddNodeCommand cmd = mock( IndexCommand.AddNodeCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexAddNodeCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexAddNodeCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexAddNodeCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexAddNodeCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexAddRelationshipCommand() throws Exception
    {
        IndexCommand.AddRelationshipCommand cmd = mock( IndexCommand.AddRelationshipCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexAddRelationshipCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexAddRelationshipCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexAddRelationshipCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexAddRelationshipCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexRemoveCommand() throws Exception
    {
        IndexCommand.RemoveCommand cmd = mock( IndexCommand.RemoveCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexRemoveCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexRemoveCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexRemoveCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexRemoveCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexDeleteCommand() throws Exception
    {
        IndexCommand.DeleteCommand cmd = mock( IndexCommand.DeleteCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexDeleteCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexDeleteCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexDeleteCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexDeleteCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexCreateCommand() throws Exception
    {
        IndexCommand.CreateCommand cmd = mock( IndexCommand.CreateCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexCreateCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexCreateCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexCreateCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexCreateCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitIndexDefineCommand() throws Exception
    {
        IndexDefineCommand cmd = mock( IndexDefineCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitIndexDefineCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitIndexDefineCommand( cmd );
        inOrder.verify( txApplier2 ).visitIndexDefineCommand( cmd );
        inOrder.verify( txApplier3 ).visitIndexDefineCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitNodeCountsCommand() throws Exception
    {
        Command.NodeCountsCommand cmd = mock( Command.NodeCountsCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitNodeCountsCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitNodeCountsCommand( cmd );
        inOrder.verify( txApplier2 ).visitNodeCountsCommand( cmd );
        inOrder.verify( txApplier3 ).visitNodeCountsCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }

    @Test
    public void testVisitRelationshipCountsCommand() throws Exception
    {
        Command.RelationshipCountsCommand cmd = mock( Command.RelationshipCountsCommand.class );
        when( cmd.handle( any( CommandVisitor.class ) ) ).thenCallRealMethod();

        // WHEN
        boolean result = facade.visitRelationshipCountsCommand( cmd );

        // THEN
        InOrder inOrder = inOrder( txApplier1, txApplier2, txApplier3 );

        inOrder.verify( txApplier1 ).visitRelationshipCountsCommand( cmd );
        inOrder.verify( txApplier2 ).visitRelationshipCountsCommand( cmd );
        inOrder.verify( txApplier3 ).visitRelationshipCountsCommand( cmd );

        inOrder.verifyNoMoreInteractions();

        assertFalse( result );
    }
}
