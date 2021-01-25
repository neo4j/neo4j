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
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.fail;

@ImpermanentDbmsExtension
class ProduceNoopCommandsIT
{
    private static final Label LABEL = Label.label( "Label" );
    private static final Label LABEL2 = Label.label( "Label2" );
    private static final String KEY = "key";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String KEY4 = "key4";
    private static final String KEY5 = "key5";
    private static final String KEY6 = "key6";
    private static final String KEY7 = "key7";
    private static final String KEY8 = "key8";
    private static final RelationshipType TYPE = RelationshipType.withName( "TYPE" );
    private static final RelationshipType TYPE2 = RelationshipType.withName( "TYPE_2" );
    private static final RelationshipType TYPE3 = RelationshipType.withName( "TYPE_3" );

    @Inject
    private GraphDatabaseService db;

    @AfterEach
    void listNoopCommands() throws IOException
    {
        LogicalTransactionStore txStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        try ( TransactionCursor transactions = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            while ( transactions.next() )
            {
                CommittedTransactionRepresentation tx = transactions.get();
                TransactionRepresentation transactionRepresentation = tx.getTransactionRepresentation();
                if ( hasNoOpCommand( transactionRepresentation ) )
                {
                    StringBuilder error = new StringBuilder( "Tx contains no-op commands, " + tx.getStartEntry() );
                    printNoOpCommands( transactionRepresentation, error );
                    error.append( format( "%n%s", tx.getCommitEntry() ) );
                    fail( error.toString() );
                }
            }
        }
    }

    @Test
    void addExistingLabelToNode()
    {
        // given
        long id = node().withLabels( LABEL ).build();

        // when
        onNode( id, ( tx, node ) -> node.addLabel( LABEL ) );
    }

    @Test
    void removeNonExistentLabelFromNode()
    {
        // given
        long id = node().withLabels( LABEL ).build();

        // when
        onNode( id, ( tx, node ) -> node.removeLabel( LABEL2 ) );
    }

    @Test
    void removeAddLabelToNode()
    {
        // given
        long id = node().withLabels( LABEL ).build();

        // when
        onNode( id, ( tx, node ) ->
        {
            node.removeLabel( LABEL );
            node.addLabel( LABEL );
        } );
    }

    @Test
    void setNodePropertyToSameValue()
    {
        // given
        long id = node().withProperty( KEY, 123 ).build();

        // when
        onNode( id, ( tx, node ) -> node.setProperty( KEY, 123 ) );
    }

    @Disabled( "This no-op prevention is still not implemented" )
    @Test
    void removeAndSetNodePropertyToSameValue()
    {
        // given
        long id = node().withProperty( KEY, 123 ).build();

        // when
        onNode( id, ( tx, node ) ->
        {
            node.removeProperty( KEY );
            node.setProperty( KEY, 123 );
        } );
    }

    @Test
    void overwriteNodePropertyInOneEndOfChain()
    {
        // given
        long id = node()
                .withProperty( KEY, 123 )
                .withProperty( KEY2, "123" )
                .withProperty( KEY3, 456 )
                .withProperty( KEY4, "123" )
                .withProperty( KEY5, 789 )
                .withProperty( KEY6, "789" )
                .withProperty( KEY7, 123456 )
                .withProperty( KEY8, "123456" )
                .build();

        // when
        onNode( id, ( tx, node ) -> node.setProperty( KEY6, 123 ) );
    }

    @Test
    void overwriteNodePropertyInAnotherEndOfChain()
    {
        // given
        long id = node()
                .withProperty( KEY, 123 )
                .withProperty( KEY2, "123" )
                .withProperty( KEY3, 456 )
                .withProperty( KEY4, "123" )
                .withProperty( KEY5, 789 )
                .withProperty( KEY6, "789" )
                .withProperty( KEY7, 123456 )
                .withProperty( KEY8, "123456" )
                .build();

        // when
        onNode( id, ( tx, node ) -> node.setProperty( KEY2, 123 ) );
    }

    @Test
    void createRelationshipOnDenseNode()
    {
        // given
        long id = node()
                .withRelationships( TYPE, 30 )
                .withRelationships( TYPE2, 30 )
                .withRelationships( TYPE3, 30 )
                .build();

        // when
        onNode( id, ( tx, node ) -> node.createRelationshipTo( tx.createNode(), TYPE2 ) );
    }

    @Test
    void deleteRelationshipFromNode()
    {
        // given
        long id = node()
                .withRelationships( TYPE, 2 )
                .withRelationships( TYPE2, 2 )
                .withRelationships( TYPE3, 2 )
                .build();

        // when
        onNode( id, ( tx, node ) -> deleteRelationship( node, 2 ) );
    }

    @Test
    void deleteRelationshipFromDenseNode()
    {
        // given
        long id = node()
                .withRelationships( TYPE, 30 )
                .withRelationships( TYPE2, 30 )
                .withRelationships( TYPE3, 30 )
                .build();

        // when
        onNode( id, ( tx, node ) -> deleteRelationship( node, 2 ) );
    }

    @Test
    void createAndDeleteRelationshipOnDenseNode()
    {
        // given
        long id = node()
                .withRelationships( TYPE, 30 )
                .withRelationships( TYPE2, 30 )
                .withRelationships( TYPE3, 30 )
                .build();

        // when
        onNode( id, ( tx, node ) -> node.createRelationshipTo( tx.createNode(), TYPE ).delete() );
    }

    private void deleteRelationship( Node node, int index )
    {
        Iterator<Relationship> relationships = node.getRelationships().iterator();
        for ( int i = 0; i < index - 1; i++ )
        {
            relationships.next();
        }
        relationships.next().delete();
        while ( relationships.hasNext() )
        {
            relationships.next();
        }
    }

    private void printNoOpCommands( TransactionRepresentation transactionRepresentation, StringBuilder error ) throws IOException
    {
        transactionRepresentation.accept( command ->
        {
            if ( command instanceof Command.BaseCommand )
            {
                Command.BaseCommand baseCommand = (Command.BaseCommand) command;
                String toString = baseCommand.toString();
                if ( baseCommand.getBefore().equals( baseCommand.getAfter() ) )
                {
                    toString += "  <---";
                }
                error.append( format( "%n%s", toString ) );
            }
            return false;
        } );
    }

    private boolean hasNoOpCommand( TransactionRepresentation transactionRepresentation ) throws IOException
    {
        MutableBoolean has = new MutableBoolean();
        transactionRepresentation.accept( command ->
        {
            if ( command instanceof Command.BaseCommand )
            {
                Command.BaseCommand baseCommand = (Command.BaseCommand) command;
                if ( baseCommand instanceof Command.PropertyCommand )
                {
                    Command.PropertyCommand propertyCommand = (Command.PropertyCommand) baseCommand;
                    fixPropertyRecord( propertyCommand.getBefore() );
                    fixPropertyRecord( propertyCommand.getAfter() );
                }
                if ( baseCommand.getBefore().equals( baseCommand.getAfter() ) )
                {
                    has.setTrue();
                }
            }
            return false;
        } );
        return has.getValue();
    }

    private static void fixPropertyRecord( PropertyRecord record )
    {
        for ( PropertyBlock block : record )
        {
            for ( long valueBlock : block.getValueBlocks() )
            {
                record.addLoadedBlock( valueBlock );
            }
        }
    }

    private NodeBuilder node()
    {
        return new NodeBuilder( db.beginTx() );
    }

    private void onNode( long node, BiConsumer<Transaction,Node> action )
    {
        try ( Transaction tx = db.beginTx() )
        {
            action.accept( tx, tx.getNodeById( node ) );
            tx.commit();
        }
    }

    private static class NodeBuilder
    {
        private Transaction tx;
        private Node node;

        NodeBuilder( Transaction tx )
        {
            this.tx = tx;
            this.node = tx.createNode();
        }

        NodeBuilder withLabels( Label... labels )
        {
            Stream.of( labels ).forEach( node::addLabel );
            return this;
        }

        NodeBuilder withRelationships( RelationshipType type, int count )
        {
            for ( int i = 0; i < count; i++ )
            {
                node.createRelationshipTo( tx.createNode(), type );
            }
            return this;
        }

        NodeBuilder withProperty( String key, Object value )
        {
            node.setProperty( key, value );
            return this;
        }

        long build()
        {
            tx.commit();
            tx.close();
            return node.getId();
        }
    }
}
