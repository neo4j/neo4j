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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.impl.api.TransactionHeaderInformation;

public interface TransactionHeaderInformationFactory
{
    TransactionHeaderInformation create();

    static final TransactionHeaderInformationFactory DEFAULT = new TransactionHeaderInformationFactory.WithRandomBytes()
    {
        private static final int NO_ID = -1;

        @Override
        protected TransactionHeaderInformation createUsing( byte[] additionalHeader )
        {
            return new TransactionHeaderInformation( NO_ID, NO_ID, additionalHeader );
        }
    };

    static abstract class WithRandomBytes implements TransactionHeaderInformationFactory
    {
        private static final int ADDITIONAL_HEADER_SIZE = 8;

        @Override
        public TransactionHeaderInformation create()
        {
            byte[] additionalHeader = generateAdditionalHeader();
            return createUsing( additionalHeader );
        }

        protected abstract TransactionHeaderInformation createUsing( byte[] additionalHeader );

        private byte[] generateAdditionalHeader()
        {
            byte[] header = new byte[ADDITIONAL_HEADER_SIZE];
            ThreadLocalRandom.current().nextBytes( header );
            return header;
        }
    }
}
