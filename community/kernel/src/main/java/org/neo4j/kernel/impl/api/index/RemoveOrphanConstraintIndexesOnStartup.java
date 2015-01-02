/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Used to assert that Indexes required by Uniqueness Constraints don't remain if the constraint never got created.
 * Solves the case where the database crashes after the index for the constraint has been created but before the
 * constraint itself has been committed.
 */
public class RemoveOrphanConstraintIndexesOnStartup
{
    private final StringLogger log;
    private final Transactor transactor;

    public RemoveOrphanConstraintIndexesOnStartup( Transactor transactor, Logging logging )
    {
        this.transactor = transactor;
        this.log = logging.getMessagesLog( getClass() );
    }

    public void perform()
    {
        try
        {
            transactor.execute( new Transactor.Work<Void, KernelException>()
            {
                @Override
                public Void perform( Statement state )
                        throws KernelException
                {
                    for ( Iterator<IndexDescriptor> indexes = state.readOperations().uniqueIndexesGetAll();
                          indexes.hasNext(); )
                    {
                        IndexDescriptor index = indexes.next();
                        if ( state.readOperations().indexGetOwningUniquenessConstraintId( index ) == null )
                        {
                            state.schemaWriteOperations().uniqueIndexDrop( index );
                        }
                    }
                    return null;
                }
            } );
        }
        catch ( KernelException | TransactionalException e )
        {
            log.error( "Failed to execute orphan index checking transaction.", e );
        }
    }
}
