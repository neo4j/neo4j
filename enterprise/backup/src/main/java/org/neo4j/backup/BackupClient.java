/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.MasterCaller;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.ToNetworkStoreWriter;
import org.neo4j.kernel.impl.util.StringLogger;

class BackupClient extends Client<TheBackupInterface> implements TheBackupInterface
{
    public BackupClient( String hostNameOrIp, int port, StringLogger logger, StoreIdGetter storeIdGetter )
    {
        super( hostNameOrIp, port, logger, storeIdGetter, BackupServer.FRAME_LENGTH,
                BackupServer.PROTOCOL_VERSION, 40,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT );
    }

    public Response<Void> fullBackup( StoreWriter storeWriter )
    {
        return sendRequest( BackupRequestType.FULL_BACKUP, SlaveContext.EMPTY,
                Protocol.EMPTY_SERIALIZER, new Protocol.FileStreamsDeserializer( storeWriter ) );
    }

    public Response<Void> incrementalBackup( SlaveContext context )
    {
        return sendRequest( BackupRequestType.INCREMENTAL_BACKUP, context,
                Protocol.EMPTY_SERIALIZER, Protocol.VOID_DESERIALIZER );
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<TheBackupInterface> type )
    {
        return type != BackupRequestType.FULL_BACKUP;
    }

    public static enum BackupRequestType implements RequestType<TheBackupInterface>
    {
        FULL_BACKUP( new MasterCaller<TheBackupInterface, Void>()
        {
            public Response<Void> callMaster( TheBackupInterface master, SlaveContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.fullBackup( new ToNetworkStoreWriter( target ) );
            }
        }, Protocol.VOID_SERIALIZER ),
        INCREMENTAL_BACKUP( new MasterCaller<TheBackupInterface, Void>()
        {
            public Response<Void> callMaster( TheBackupInterface master, SlaveContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.incrementalBackup( context );
            }
        }, Protocol.VOID_SERIALIZER )

        ;
        @SuppressWarnings( "rawtypes" )
        private final MasterCaller masterCaller;
        @SuppressWarnings( "rawtypes" )
        private final ObjectSerializer serializer;

        @SuppressWarnings( "rawtypes" )
        private BackupRequestType( MasterCaller masterCaller, ObjectSerializer serializer )
        {
            this.masterCaller = masterCaller;
            this.serializer = serializer;
        }

        @SuppressWarnings( "rawtypes" )
        public MasterCaller getMasterCaller()
        {
            return masterCaller;
        }

        @SuppressWarnings( "rawtypes" )
        public ObjectSerializer getObjectSerializer()
        {
            return serializer;
        }

        public byte id()
        {
            return (byte) ordinal();
        }
    }
}
