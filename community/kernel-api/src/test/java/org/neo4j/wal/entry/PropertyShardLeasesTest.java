/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.wal.entry;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.leases.PropertyShardLeases;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.storageengine.api.Leases;

public class PropertyShardLeasesTest {
    @Test
    public void leasesOrderIsDeterministic() {
        ArrayList<UUID> uuidList = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            uuidList.add(UUID.randomUUID());
        }
        PropertyShardLeases leases = PropertyShardLeases.EMPTY;
        for (UUID uuid : uuidList) {
            PropertyShardLeases nextLeases = leases.updateLease(DatabaseIdFactory.from(uuid), new ServerId(uuid));
            leases = leases.merge(nextLeases);
        }
        Collections.shuffle(uuidList);
        PropertyShardLeases leases2 = PropertyShardLeases.EMPTY;
        for (UUID uuid : uuidList) {
            PropertyShardLeases nextLeases = leases2.updateLease(DatabaseIdFactory.from(uuid), new ServerId(uuid));
            leases2 = leases2.merge(nextLeases);
        }

        Iterator<Leases.Lease> leaseIterator1 = leases.iterator();
        Iterator<Leases.Lease> leaseIterator2 = leases2.iterator();
        while (leaseIterator1.hasNext() && leaseIterator2.hasNext()) {
            assertThat(leaseIterator1.next()).isEqualTo(leaseIterator2.next());
        }
        assertThat(leaseIterator1.hasNext()).isFalse();
        assertThat(leaseIterator2.hasNext()).isFalse();
    }
}
