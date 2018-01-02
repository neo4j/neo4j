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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.command.Command;

/**
 * Keeper of state that is about to be committed. That state can be {@link #extractCommands(Collection) extracted}
 * into a list of {@link Command commands}.
 */
public interface RecordState
{
    /**
     * @return whether or not there are any changes in here. If {@code true} then {@link Command commands}
     * can be {@link #extractCommands(Collection) extracted}.
     */
    boolean hasChanges();

    /**
     * Extracts this record state in the form of {@link Command commands} into the supplied {@code target} list.
     * @param target list that commands will be added into.
     * @throws TransactionFailureException if the state is invalid or not applicable.
     */
    void extractCommands( Collection<Command> target ) throws TransactionFailureException;
}
