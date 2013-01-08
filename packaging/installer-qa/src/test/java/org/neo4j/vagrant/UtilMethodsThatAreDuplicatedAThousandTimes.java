/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.vagrant;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

public class UtilMethodsThatAreDuplicatedAThousandTimes {

    public static void copyFolder(File src, File dest)
    {
        try {
            if (src.isDirectory())
            {
    
                // if directory not exists, create it
                if (!dest.exists())
                {
                    dest.mkdir();
                }
    
                for (String file : src.list())
                {
                    // construct the src and dest file structure
                    File srcFile = new File(src, file);
                    File destFile = new File(dest, file);
                    // recursive copy
                    copyFolder(srcFile, destFile);
                }
    
            } else
            {
                InputStream in = null;
                OutputStream out = null;
                try {
                    in = new FileInputStream(src);
                    out = new FileOutputStream(dest);
                    IOUtils.copy(in, out);
                } finally {
                    if(in != null) in.close();
                    if(out != null) out.close();
                }
            }
        } catch (IOException e)
        {   
            throw new RuntimeException(e);
        }
    }
    
}
