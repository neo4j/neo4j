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
package org.neo4j.kernel.impl.api;

import java.util.Objects;
import org.neo4j.kernel.api.KernelTransaction;

public class KernelTransactionStamp {
    private final KernelTransaction ktx;
    private final long transactionSequenceNumber;

    public KernelTransactionStamp(KernelTransaction ktx) {
        this.transactionSequenceNumber = ktx.getTransactionSequenceNumber();
        this.ktx = ktx;
    }

    public boolean isOpen() {
        return ktx.isOpen() && transactionSequenceNumber == ktx.getTransactionSequenceNumber();
    }

    public boolean isCommitting() {
        return ktx.isCommitting() && transactionSequenceNumber == ktx.getTransactionSequenceNumber();
    }

    public boolean isRollingback() {
        return ktx.isRollingback() && transactionSequenceNumber == ktx.getTransactionSequenceNumber();
    }

    public boolean isClosing() {
        return ktx.isClosing() && transactionSequenceNumber == ktx.getTransactionSequenceNumber();
    }

    long getTransactionSequenceNumber() {
        return transactionSequenceNumber;
    }

    public boolean isNotExpired() {
        return transactionSequenceNumber == ktx.getTransactionSequenceNumber();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KernelTransactionStamp that = (KernelTransactionStamp) o;
        return transactionSequenceNumber == that.transactionSequenceNumber && Objects.equals(ktx, that.ktx);
    }

    @Override
    public int hashCode() {
        int result = ktx != null ? ktx.hashCode() : 0;
        result = 31 * result + (int) (transactionSequenceNumber ^ (transactionSequenceNumber >>> 32));
        return result;
    }
}
