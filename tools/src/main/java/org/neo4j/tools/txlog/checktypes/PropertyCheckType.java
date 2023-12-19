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
package org.neo4j.tools.txlog.checktypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

class PropertyCheckType extends CheckType<Command.PropertyCommand,PropertyRecord>
{
    PropertyCheckType()
    {
        super( Command.PropertyCommand.class );
    }

    @Override
    public PropertyRecord before( Command.PropertyCommand command )
    {
        return command.getBefore();
    }

    @Override
    public PropertyRecord after( Command.PropertyCommand command )
    {
        return command.getAfter();
    }

    @Override
    protected boolean inUseRecordsEqual( PropertyRecord record1, PropertyRecord record2 )
    {
        return record1.isNodeSet() == record2.isNodeSet() &&
               record1.isRelSet() == record2.isRelSet() &&
               record1.getNodeId() == record2.getNodeId() &&
               record1.getRelId() == record2.getRelId() &&
               record1.getNextProp() == record2.getNextProp() &&
               record1.getPrevProp() == record2.getPrevProp() &&
               blocksEqual( record1, record2 );
    }

    @Override
    public String name()
    {
        return "property";
    }

    private static boolean blocksEqual( PropertyRecord r1, PropertyRecord r2 )
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

    private static List<PropertyBlock> blocks( PropertyRecord record )
    {
        List<PropertyBlock> result = new ArrayList<>();
        for ( PropertyBlock block : record )
        {
            result.add( block );
        }
        return result;
    }
}
