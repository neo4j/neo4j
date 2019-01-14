/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.adversaries.fs;

import java.nio.channels.FileChannel;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;

/**
 * File system abstraction that behaves exactly like {@link DefaultFileSystemAbstraction} <b>except</b> instead of
 * default {@link FileChannel} implementation {@link AdversarialFileChannel} will be used.
 *
 * This abstraction should be used in cases when it's desirable to have default file system implementation
 * and only verify handling of inconsistent channel operations.
 * Otherwise consider {@link AdversarialFileSystemAbstraction} since it should produce more failure cases.
 */
public class AdversarialChannelDefaultFileSystemAbstraction extends DefaultFileSystemAbstraction
{
    private final RandomAdversary adversary;

    public AdversarialChannelDefaultFileSystemAbstraction()
    {
        this( new RandomAdversary( 0.5, 0.0, 0.0 ) );
    }

    public AdversarialChannelDefaultFileSystemAbstraction( RandomAdversary adversary )
    {
        this.adversary = adversary;
    }

    @Override
    protected StoreFileChannel getStoreFileChannel( FileChannel channel )
    {
        return AdversarialFileChannel.wrap( super.getStoreFileChannel( channel ), adversary );
    }
}
