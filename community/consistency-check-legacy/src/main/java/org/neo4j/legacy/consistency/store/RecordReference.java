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
package org.neo4j.legacy.consistency.store;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.legacy.consistency.report.PendingReferenceCheck;

public interface RecordReference<RECORD extends AbstractBaseRecord>
{
    void dispatch( PendingReferenceCheck<RECORD> reporter );

    class SkippingReference<RECORD extends AbstractBaseRecord> implements RecordReference<RECORD>
    {
        @SuppressWarnings("unchecked")
        public static <RECORD extends AbstractBaseRecord> SkippingReference<RECORD> skipReference()
        {
            return INSTANCE;
        }

        @Override
        public void dispatch( PendingReferenceCheck<RECORD> reporter )
        {
            reporter.skip();
        }

        @Override
        public String toString()
        {
            return "SkipReference";
        }

        private static final SkippingReference INSTANCE = new SkippingReference();

        private SkippingReference()
        {
            // singleton
        }
    }
}
