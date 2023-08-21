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
package org.neo4j.graphdb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Consumer;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public abstract class AbstractMandatoryTransactionsTest<T> {
    @Inject
    public GraphDatabaseService db;

    public T obtainEntity() {
        try (Transaction tx = db.beginTx()) {
            T result = obtainEntityInTransaction(tx);
            tx.commit();

            return result;
        }
    }

    public void obtainEntityInTerminatedTransaction(Consumer<T> f) {
        try (Transaction tx = db.beginTx()) {
            T result = obtainEntityInTransaction(tx);
            tx.terminate();

            f.accept(result);
        }
    }

    protected abstract T obtainEntityInTransaction(Transaction transaction);

    public static <T> void assertFacadeMethodsThrowNotInTransaction(T entity, Consumer<T>[] methods) {
        for (Consumer<T> method : methods) {
            assertThrows(NotInTransactionException.class, () -> method.accept(entity), method::toString);
        }
    }

    public void assertFacadeMethodsThrowAfterTerminate(Consumer<T>[] methods) {
        for (final Consumer<T> method : methods) {
            obtainEntityInTerminatedTransaction(entity ->
                    assertThrows(TransactionTerminatedException.class, () -> method.accept(entity), method::toString));
        }
    }
}
