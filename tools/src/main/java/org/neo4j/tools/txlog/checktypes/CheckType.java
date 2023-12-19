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

import java.util.Objects;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

/**
 * Type of command ({@link NodeCommand}, {@link PropertyCommand}, ...) to check during transaction log verification.
 * This class exists to mitigate the absence of interfaces for commands with before and after state.
 * It also provides an alternative equality check instead of {@link AbstractBaseRecord#equals(Object)} that only
 * checks {@linkplain AbstractBaseRecord#getId() entity id}.
 *
 * @param <C> the type of command to check
 * @param <R> the type of records that this command contains
 */
public abstract class CheckType<C extends Command, R extends AbstractBaseRecord>
{
    private final Class<C> recordClass;

    CheckType( Class<C> recordClass )
    {
        this.recordClass = recordClass;
    }

    public Class<C> commandClass()
    {
        return recordClass;
    }

    public abstract R before( C command );

    public abstract R after( C command );

    public final boolean equal( R record1, R record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        if ( record1.getId() != record2.getId() )
        {
            return false;
        }
        else if ( record1.inUse() != record2.inUse() )
        {
            return false;
        }
        else if ( !record1.inUse() )
        {
            return true;
        }
        else
        {
            return inUseRecordsEqual( record1, record2 );
        }
    }

    protected abstract boolean inUseRecordsEqual( R record1, R record2 );

    public abstract String name();
}
