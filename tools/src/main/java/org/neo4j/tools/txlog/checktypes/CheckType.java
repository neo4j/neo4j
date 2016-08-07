/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
