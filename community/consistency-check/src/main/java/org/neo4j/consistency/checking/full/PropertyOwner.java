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
package org.neo4j.consistency.checking.full;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

abstract class PropertyOwner<RECORD extends PrimitiveRecord> implements Owner
{
    abstract RecordReference<RECORD> record( RecordAccess records );

    public void checkOrphanage()
    {
        // default: do nothing
    }

    static class OwningNode extends PropertyOwner<NodeRecord>
    {
        private final long id;

        OwningNode( NodeRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<NodeRecord> record( RecordAccess records )
        {
            return records.node( id );
        }
    }

    static class OwningRelationship extends PropertyOwner<RelationshipRecord>
    {
        private final long id;

        OwningRelationship( RelationshipRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<RelationshipRecord> record( RecordAccess records )
        {
            return records.relationship( id );
        }
    }

    static final PropertyOwner<NeoStoreRecord> OWNING_GRAPH = new PropertyOwner<NeoStoreRecord>()
    {
        @Override
        RecordReference<NeoStoreRecord> record( RecordAccess records )
        {
            return records.graph();
        }
    };

    static class UnknownOwner extends PropertyOwner<PrimitiveRecord> implements RecordReference<PrimitiveRecord>
    {
        private PendingReferenceCheck<PrimitiveRecord> reporter;

        @Override
        RecordReference<PrimitiveRecord> record( RecordAccess records )
        {
            // Getting the record for this owner means that some other owner replaced it
            // that means that it isn't an orphan, so we skip this orphan check
            // and return a record for conflict check that always is ok (by skipping the check)
            this.markInCustody();
            return skipReference();
        }

        @Override
        public void checkOrphanage()
        {
            PendingReferenceCheck<PrimitiveRecord> reporter;
            synchronized ( this )
            {
                reporter = this.reporter;
                this.reporter = null;
            }
            if ( reporter != null )
            {
                reporter.checkReference( null, null );
            }
        }

        synchronized void markInCustody()
        {
            if ( reporter != null )
            {
                reporter.skip();
                reporter = null;
            }
        }

        @Override
        public synchronized void dispatch( PendingReferenceCheck<PrimitiveRecord> reporter )
        {
            this.reporter = reporter;
        }
    }

    private PropertyOwner()
    {
        // only internal subclasses
    }
}
