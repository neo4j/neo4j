/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.applytx;

import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;

import java.io.PrintStream;

import org.neo4j.helpers.Provider;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.tools.console.input.Command;
import org.neo4j.tools.console.input.ConsoleInput;

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
    private final Provider<StoreAccess> store;

    @SuppressWarnings( "unchecked" )
    public DumpRecordsCommand( Provider<StoreAccess> store )
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
    public void run( String[] args, PrintStream out ) throws Exception
    {
        cli.parse( args ).run( store.instance(), out );
    }

    @Override
    public String toString()
    {
        return airlineHelp( cli );
    }

    static abstract class DumpPropertyChain implements Action
    {
        @Arguments( title = "id", description = "Entity id", required = true )
        public long id;

        protected abstract long firstPropId( StoreAccess access );

        @Override
        public void run( StoreAccess store, PrintStream out )
        {
            long propId = firstPropId( store );
            while ( propId != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord record = store.getPropertyStore().getRecord( propId );
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
            return access.getNodeStore().getRecord( id ).getNextProp();
        }
    }

    @io.airlift.airline.Command( name = "properties", description = "Dump property chain for a relationship" )
    public static class DumpRelationshipPropertyChain extends DumpPropertyChain
    {
        @Override
        protected long firstPropId( StoreAccess access )
        {
            return access.getRelationshipStore().getRecord( id ).getNextProp();
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
            NodeRecord node = store.getNodeStore().getRecord( id );
            if ( node.isDense() )
            {
                RelationshipGroupRecord group = store.getRelationshipGroupStore().getRecord( node.getNextRel() );
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
                            store.getRelationshipGroupStore().getRecord( group.getNext() ) : null;
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
                RelationshipRecord record = access.getRelationshipStore().getRecord( rel );
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
