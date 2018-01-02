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
package org.neo4j.tools.txlog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

/**
 * Type of command ({@link NodeCommand}, {@link PropertyCommand}, ...) to check during transaction log verification.
 * This class exists to mitigate the absence of interfaces for commands with before and after state.
 * It also provides an alternative equality check instead of {@link Abstract64BitRecord#equals(Object)} that only
 * checks {@linkplain Abstract64BitRecord#getLongId() entity id}.
 *
 * @param <C> the type of command to check
 * @param <R> the type of records that this command contains
 */
abstract class CheckType<C extends Command, R extends Abstract64BitRecord>
{
    public static final NodeCheckType NODE = new NodeCheckType();
    public static final PropertyCheckType PROPERTY = new PropertyCheckType();

    private final Class<C> recordClass;

    CheckType( Class<C> recordClass )
    {
        this.recordClass = recordClass;
    }

    Class<C> commandClass()
    {
        return recordClass;
    }

    abstract R before( C command );

    abstract R after( C command );

    abstract boolean equal( R record1, R record2 );

    abstract String name();

    private static class NodeCheckType extends CheckType<NodeCommand,NodeRecord>
    {
        NodeCheckType()
        {
            super( NodeCommand.class );
        }

        @Override
        NodeRecord before( NodeCommand command )
        {
            return command.getBefore();
        }

        @Override
        NodeRecord after( NodeCommand command )
        {
            return command.getAfter();
        }

        @Override
        boolean equal( NodeRecord record1, NodeRecord record2 )
        {
            Objects.requireNonNull( record1 );
            Objects.requireNonNull( record2 );

            return record1.getId() == record2.getId() &&
                   record1.inUse() == record2.inUse() &&
                   record1.getNextProp() == record2.getNextProp() &&
                   record1.getNextRel() == record2.getNextRel() &&
                   record1.isDense() == record2.isDense() &&
                   record1.getLabelField() == record2.getLabelField();
        }

        @Override
        String name()
        {
            return "node";
        }
    }

    private static class PropertyCheckType extends CheckType<PropertyCommand,PropertyRecord>
    {
        PropertyCheckType()
        {
            super( PropertyCommand.class );
        }

        @Override
        PropertyRecord before( PropertyCommand command )
        {
            return command.getBefore();
        }

        @Override
        PropertyRecord after( PropertyCommand command )
        {
            return command.getAfter();
        }

        @Override
        boolean equal( PropertyRecord record1, PropertyRecord record2 )
        {
            Objects.requireNonNull( record1 );
            Objects.requireNonNull( record2 );

            if ( record1.getId() != record2.getId() )
            {
                return false;
            }
            if ( record1.inUse() != record2.inUse() )
            {
                return false;
            }
            if ( !record1.inUse() )
            {
                return true;
            }
            return record1.isNodeSet() == record2.isNodeSet() &&
                   record1.isRelSet() == record2.isRelSet() &&
                   record1.getNodeId() == record2.getNodeId() &&
                   record1.getRelId() == record2.getRelId() &&
                   record1.getNextProp() == record2.getNextProp() &&
                   record1.getPrevProp() == record2.getPrevProp() &&
                   blocksEqual( record1, record2 );
        }

        @Override
        String name()
        {
            return "property";
        }

        static boolean blocksEqual( PropertyRecord r1, PropertyRecord r2 )
        {
            if ( r1.size() != r2.size() )
            {
                return false;
            }
            List<PropertyBlock> r1Blocks = blocks( r1 );
            List<PropertyBlock> r2Blocks = blocks( r2 );
            if ( r1Blocks.size() != r2Blocks.size() )
            {
                return false;
            }
            for ( int i = 0; i < r1Blocks.size(); i++ )
            {
                PropertyBlock r1Block = r1Blocks.get( i );
                PropertyBlock r2Block = r2Blocks.get( i );
                if ( !Arrays.equals( r1Block.getValueBlocks(), r2Block.getValueBlocks() ) )
                {
                    return false;
                }
            }
            return true;
        }

        static List<PropertyBlock> blocks( PropertyRecord record )
        {
            List<PropertyBlock> result = new ArrayList<>();
            while ( record.hasNext() )
            {
                result.add( record.next() );
            }
            return result;
        }
    }
}
