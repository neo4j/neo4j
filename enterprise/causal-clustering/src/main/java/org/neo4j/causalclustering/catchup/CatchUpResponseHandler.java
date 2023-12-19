/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

public interface CatchUpResponseHandler
{
    void onFileHeader( FileHeader fileHeader );

    /**
     * @param fileChunk Part of a file.
     * @return <code>true</code> if this is the last part of the file that is currently being transferred.
     */
    boolean onFileContent( FileChunk fileChunk ) throws IOException;

    void onFileStreamingComplete( StoreCopyFinishedResponse response );

    void onTxPullResponse( TxPullResponse tx );

    void onTxStreamFinishedResponse( TxStreamFinishedResponse response );

    void onGetStoreIdResponse( GetStoreIdResponse response );

    void onCoreSnapshot( CoreSnapshot coreSnapshot );

    void onStoreListingResponse( PrepareStoreCopyResponse storeListingRequest );

    void onClose();
}
