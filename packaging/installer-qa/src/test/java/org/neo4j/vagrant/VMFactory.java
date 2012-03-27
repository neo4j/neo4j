/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.neo4j.vagrant.UtilMethodsThatAreDuplicatedAThousandTimes.copyFolder;

import java.io.File;

public class VMFactory {

    private static String workingDir = System.getProperty("jagrant.workingdir", System.getProperty("user.home") + "/jagrant/");
    private static String templateDir = System.getProperty("jagrant.templatedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/");
    private static String shareDir = System.getProperty("jagrant.templatesharedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/all/");

    public static VirtualMachine vm(VMDefinition config)
    {
        VirtualMachine v;
        File projectFolder = new File(workingDir + config.vmName());
        File templateFolder = new File(templateDir + config.vmName());
        File sharedFolder = new File(shareDir);
        
        if (!projectFolder.exists())
        {
            projectFolder.mkdirs();
            v = vm(projectFolder, templateFolder, sharedFolder, config);
            if(!templateFolder.exists()) {
                v.init();
            }
        } else
        {
            v = vm(projectFolder, templateFolder, sharedFolder, config);
        }
        
        return v;
    }

    private static VirtualMachine vm(File projectFolder, File templateFolder, File sharedFolder, VMDefinition config)
    {
        VirtualMachine v = new VirtualMachine(projectFolder, config);
        
        if(templateFolder.exists()) {
            copyFolder(templateFolder, projectFolder);
        }
        copyFolder(sharedFolder, projectFolder);
        
        v.ensureBoxExists(config.box());
        v.setTransactional(true);
        return v;
    }
    
}
