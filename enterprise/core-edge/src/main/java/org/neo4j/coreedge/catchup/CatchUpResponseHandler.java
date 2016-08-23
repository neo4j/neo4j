/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import java.io.IOException;

import org.neo4j.coreedge.catchup.storecopy.FileContent;
import org.neo4j.coreedge.catchup.storecopy.FileHeader;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.coreedge.catchup.tx.TxPullResponse;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;

public interface CatchUpResponseHandler
{
    void onFileHeader( FileHeader fileHeader );

    /**
     * @param fileContent Part of a file.
     * @return <code>true</code> if this is the last part of the file that is currently being transferred.
     */
    boolean onFileContent( FileContent fileContent ) throws IOException;

    void onFileStreamingComplete( StoreCopyFinishedResponse response );

    void onTxPullResponse( TxPullResponse tx );

    void onTxStreamFinishedResponse( TxStreamFinishedResponse response );

    void onGetStoreIdResponse( GetStoreIdResponse response );

    void onCoreSnapshot( CoreSnapshot coreSnapshot );
}
