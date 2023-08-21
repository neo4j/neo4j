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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;

import java.util.function.Function;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.test.RandomSupport;

class GBPTreeParallelWritesFixedSizeIT extends GBPTreeParallelWritesIT<MutableLong, MutableLong> {
    @Override
    TestLayout<MutableLong, MutableLong> getLayout(RandomSupport random, int payloadSize) {
        return longLayout().withKeyPadding(random.intBetween(0, 10)).build();
    }

    @Override
    protected ValueAggregator<MutableLong> getAddingAggregator() {
        return (value, aggregation) -> aggregation.add(value);
    }

    @Override
    protected MutableLong sumValues(MutableLong value1, MutableLong value2) {
        return new MutableLong(value1.longValue() + value2.longValue());
    }

    @Override
    Function<MutableLong, MutableLong> getValueIncrementer() {
        return v -> new MutableLong(v.longValue() + 1);
    }
}
