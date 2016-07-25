/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.kernel.api.exceptions.Status;

public interface MessageHandler<E extends Exception>
{
    void handleRunMessage( String statement, Map<String,Object> params ) throws E;

    void handlePullAllMessage() throws E;

    void handleDiscardAllMessage() throws E;

    void handleRecordMessage( Record item ) throws E;

    void handleSuccessMessage( Map<String,Object> metadata ) throws E;

    void handleFailureMessage( Status status, String message ) throws E;

    void handleIgnoredMessage() throws E;

    void handleInitMessage( String clientName, Map<String,Object> credentials ) throws E;

    void handleResetMessage() throws E;

    void handleAckFailureMessage() throws E;

    class Adapter<E extends Exception> implements MessageHandler<E>
    {
        @Override
        public void handleRunMessage( String statement, Map<String,Object> params ) throws E
        {

        }

        @Override
        public void handlePullAllMessage() throws E
        {

        }

        @Override
        public void handleDiscardAllMessage() throws E
        {

        }

        @Override
        public void handleRecordMessage( Record item ) throws E
        {

        }

        @Override
        public void handleSuccessMessage( Map<String,Object> metadata ) throws E
        {

        }

        @Override
        public void handleFailureMessage( Status status, String message ) throws E
        {

        }

        @Override
        public void handleIgnoredMessage() throws E
        {

        }

        @Override
        public void handleInitMessage( String clientName, Map<String,Object> credentials ) throws E
        {

        }

        @Override
        public void handleResetMessage() throws E
        {

        }

        @Override
        public void handleAckFailureMessage() throws E
        {

        }
    }
}
