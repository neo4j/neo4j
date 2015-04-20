/*
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
package org.neo4j.kernel.impl.transaction;

public interface RecoveryVerifier
{
    RecoveryVerifier ALWAYS_VALID = new RecoveryVerifier()
    {
        @Override
        public boolean isValid( TransactionInfo txInfo )
        {
            return true;
        }
    };
    
    /**
     * Performs a check for the transaction which was just recovered to see if it's OK or not.
     * The relevant information about the transaction is in {@code txInfo}. Typically,
     * in a stand-alone environment there's no need to perform any additional checks since
     * the 2PC protocol will guard for those cases. For a distributed environment it might
     * be required.
     * 
     * @param txInfo the {@link TransactionInfo} containing relevant information about the
     * transaction which was recovered.
     * @return {@code true} if the transaction verifies OK, otherwise {@code false}.
     * Returning false will result in a {@link RecoveryVerificationException} being thrown
     * from the recovery process.
     */
    boolean isValid( TransactionInfo txInfo );
}
