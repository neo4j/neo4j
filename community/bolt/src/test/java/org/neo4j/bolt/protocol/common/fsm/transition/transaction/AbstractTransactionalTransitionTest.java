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
package org.neo4j.bolt.protocol.common.fsm.transition.transaction;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.statement.Statement;

public abstract class AbstractTransactionalTransitionTest<
                R extends RequestMessage, D extends TransactionalStateTransition<R>>
        extends AbstractStateTransitionTest<R, D> {

    public static final String MOCK_BOOKMARK = "some-bookmark-123";

    protected Transaction transaction;
    protected Statement statement;

    @Override
    @BeforeEach
    protected void prepareContext() throws Exception {
        this.transaction = Mockito.mock(Transaction.class);
        this.statement = Mockito.mock(Statement.class);

        super.prepareContext();

        Mockito.doReturn(Optional.of(this.transaction)).when(this.connection).transaction();

        Mockito.doReturn(MOCK_BOOKMARK).when(this.transaction).commit();
        Mockito.doReturn(42L).when(this.transaction).latestStatementId();
        Mockito.doReturn(Optional.of(this.statement)).when(this.transaction).getStatement(Mockito.anyLong());
    }
}
