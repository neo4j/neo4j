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

import java.util.function.Function;
import org.neo4j.test.RandomSupport;

class GBPTreeParallelWritesDynamicSizeIT extends GBPTreeParallelWritesIT<RawBytes, RawBytes> {
    @Override
    TestLayout<RawBytes, RawBytes> getLayout(RandomSupport random, int payloadSize) {
        return new SimpleByteArrayLayout(
                DynamicSizeUtil.keyValueSizeCapFromPageSize(payloadSize) / 2, random.intBetween(0, 10));
    }

    @Override
    protected ValueAggregator<RawBytes> getAddingAggregator() {
        return this::add;
    }

    @Override
    protected RawBytes sumValues(RawBytes value1, RawBytes value2) {
        long seed1 = layout.keySeed(value1);
        long seed2 = layout.keySeed(value2);
        return layout.value(seed1 + seed2);
    }

    @Override
    Function<RawBytes, RawBytes> getValueIncrementer() {
        return v -> layout.value(layout.valueSeed(v) + 1);
    }

    private void add(RawBytes add, RawBytes base) {
        base.copyFrom(sumValues(add, base));
    }
}
