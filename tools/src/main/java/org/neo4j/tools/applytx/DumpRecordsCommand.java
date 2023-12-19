/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.applytx;

import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;

import java.io.PrintStream;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Token;
import org.neo4j.tools.console.input.Command;
import org.neo4j.tools.console.input.ConsoleInput;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.tools.console.input.ConsoleUtil.airlineHelp;

/**
 * Able to dump records and record chains. Works as a {@link ConsoleInput} {@link Command}.
 */
public class DumpRecordsCommand implements Command
{
    public static final String NAME = "dump";

    private interface Action
    {
        void run( StoreAccess store, PrintStream out );
    }

    private final Cli<Action> cli;
    private final Supplier<StoreAccess> store;

    @SuppressWarnings( "unchecked" )
    public DumpRecordsCommand( Supplier<StoreAccess> store )
    {
        this.store = store;
        CliBuilder<Action> builder = Cli.<Action>builder( NAME )
                .withDescription( "Dump record contents" )
                .withCommands( DumpRelationshipTypes.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "node" )
                .withCommands( DumpNodePropertyChain.class, DumpNodeRelationshipChain.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "relationship" )
                .withCommands( DumpRelationshipPropertyChain.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "tokens" )
                .withCommands( DumpRelationshipTypes.class, DumpLabels.class, DumpPropertyKeys.class, Help.class )
                .withDefaultCommand( Help.class );
        this.cli = builder.build();
    }

    @Override
    public void run( String[] args, PrintStream out )
    {
        cli.parse( args ).run( store.get(), out );
    }

    @Override
    public String toString()
    {
        return airlineHelp( cli );
    }

    abstract static class DumpPropertyChain implements Action
    {
        @Arguments( title = "id", description = "Entity id", required = true )
        public long id;

        protected abstract long firstPropId( StoreAccess access );

        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            long propId = firstPropId( store );
            RecordStore<PropertyRecord> propertyStore = store.getPropertyStore();
            PropertyRecord record = propertyStore.newRecord();
            while ( propId != Record.NO_NEXT_PROPERTY.intValue() )
            {
                propertyStore.getRecord( propId, record, NORMAL );
                // We rely on this method having the side-effect of loading the property blocks:
                record.numberOfProperties();
                out.println( record );
                propId = record.getNextProp();
            }
        }
    }

    @io.airlift.airline.Command( name = "properties", description = "Dump property chain for a node" )
    public static class DumpNodePropertyChain extends DumpPropertyChain
    {
        @Override
        protected long firstPropId( StoreAccess access )
        {
            RecordStore<NodeRecord> nodeStore = access.getNodeStore();
            return nodeStore.getRecord( id, nodeStore.newRecord(), NORMAL ).getNextProp();
        }
    }

    @io.airlift.airline.Command( name = "properties", description = "Dump property chain for a relationship" )
    public static class DumpRelationshipPropertyChain extends DumpPropertyChain
    {
        @Override
        protected long firstPropId( StoreAccess access )
        {
            RecordStore<RelationshipRecord> relationshipStore = access.getRelationshipStore();
            return relationshipStore.getRecord( id, relationshipStore.newRecord(), NORMAL ).getNextProp();
        }
    }

    @io.airlift.airline.Command( name = "relationships", description = "Dump relationship chain for a node" )
    public static class DumpNodeRelationshipChain implements Action
    {
        @Arguments( description = "Node id", required = true )
        public long id;

        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            RecordStore<NodeRecord> nodeStore = store.getNodeStore();
            NodeRecord node = nodeStore.getRecord( id, nodeStore.newRecord(), NORMAL );
            if ( node.isDense() )
            {
                RecordStore<RelationshipGroupRecord> relationshipGroupStore = store.getRelationshipGroupStore();
                RelationshipGroupRecord group = relationshipGroupStore.newRecord();
                relationshipGroupStore.getRecord( node.getNextRel(), group, NORMAL );
                do
                {
                    out.println( "group " + group );
                    out.println( "out:" );
                    printRelChain( store, out, group.getFirstOut() );
                    out.println( "in:" );
                    printRelChain( store, out, group.getFirstIn() );
                    out.println( "loop:" );
                    printRelChain( store, out, group.getFirstLoop() );
                    group = group.getNext() != -1 ?
                            relationshipGroupStore.getRecord( group.getNext(), group, NORMAL ) : null;
                } while ( group != null );
            }
            else
            {
                printRelChain( store, out, node.getNextRel() );
            }
        }

        private void printRelChain( StoreAccess access, PrintStream out, long firstRelId )
        {
            for ( long rel = firstRelId; rel != Record.NO_NEXT_RELATIONSHIP.intValue(); )
            {
                RecordStore<RelationshipRecord> relationshipStore = access.getRelationshipStore();
                RelationshipRecord record = relationshipStore.getRecord( rel, relationshipStore.newRecord(), NORMAL );
                out.println( rel + "\t" + record );
                if ( record.getFirstNode() == id )
                {
                    rel = record.getFirstNextRel();
                }
                else
                {
                    rel = record.getSecondNextRel();
                }
            }
        }
    }

    @io.airlift.airline.Command( name = "relationship-type", description = "Dump relationship type tokens" )
    public static class DumpRelationshipTypes implements Action
    {
        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            for ( Token token : ((RelationshipTypeTokenStore)
                    store.getRelationshipTypeTokenStore()).getTokens( Integer.MAX_VALUE ) )
            {
                out.println( token );
            }
        }
    }

    @io.airlift.airline.Command( name = "label", description = "Dump label tokens" )
    public static class DumpLabels implements Action
    {
        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            for ( Token token : ((LabelTokenStore)
                    store.getLabelTokenStore()).getTokens( Integer.MAX_VALUE ) )
            {
                out.println( token );
            }
        }
    }

    @io.airlift.airline.Command( name = "property-key", description = "Dump property key tokens" )
    public static class DumpPropertyKeys implements Action
    {
        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            for ( Token token : ((PropertyKeyTokenStore)
                    store.getPropertyKeyTokenStore()).getTokens( Integer.MAX_VALUE ) )
            {
                out.println( token );
            }
        }
    }

    public static class Help extends io.airlift.airline.Help implements Action
    {
        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            run();
        }
    }
}
