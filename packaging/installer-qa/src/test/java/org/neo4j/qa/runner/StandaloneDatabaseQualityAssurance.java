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
package org.neo4j.qa.runner;

import static org.neo4j.vagrant.VMFactory.vm;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.neo4j.qa.Neo4jVM;
import org.neo4j.qa.Platforms;
import org.neo4j.qa.SharedConstants;
import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.driver.UbuntuDebAdvancedDriver;
import org.neo4j.qa.driver.UbuntuDebCommunityDriver;
import org.neo4j.qa.driver.UbuntuDebEnterpriseDriver;
import org.neo4j.qa.driver.UbuntuTarGzAdvancedDriver;
import org.neo4j.qa.driver.UbuntuTarGzCommunityDriver;
import org.neo4j.qa.driver.UbuntuTarGzEnterpriseDriver;
import org.neo4j.qa.driver.WindowsAdvancedDriver;
import org.neo4j.qa.driver.WindowsCommunityDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.qa.machinestate.DefaultMachineModelImpl;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.machinestate.modifier.DownloadLogs;
import org.neo4j.qa.machinestate.modifier.RecreateMachine;
import org.neo4j.qa.machinestate.modifier.RollbackMachine;
import org.neo4j.vagrant.VirtualMachine;

public class StandaloneDatabaseQualityAssurance extends Suite {

    VagrantIssueIdentifier vagrantIssue = new VagrantIssueIdentifier();
    
    private class QualityAssuranceTestClassRunner extends
            BlockJUnit4ClassRunner {

        private TestPermutation testPermutation;

        QualityAssuranceTestClassRunner(Class<?> type,
                TestPermutation testPermutation) throws InitializationError
        {
            super(type);
            this.testPermutation = testPermutation;
        }

        @Override
        public Object createTest() throws Exception
        {
            return getTestClass().getOnlyConstructor().newInstance(
                    testPermutation.getConstructorArgs());
        }
        
        @Override
        protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
            Description description= describeChild(method);
            if (method.getAnnotation(Ignore.class) != null) {
                notifier.fireTestIgnored(description);
            } else {
                overriddenRunLeaf(methodBlock(method), description, notifier, testName(method));
            }
        }
        
        protected void overriddenRunLeaf(Statement statement, Description description,
                RunNotifier notifier, String testName) {
            EachTestNotifier eachNotifier= new EachTestNotifier(notifier, description);
            eachNotifier.fireTestStarted();
            try {
                try {
                    testPermutation.getMachineModel().forceApply(new RollbackMachine());
                    statement.evaluate();
                } catch (AssumptionViolatedException e) {
                    throw e;
                } catch(Throwable e) {
                    if(vagrantIssue.potentiallyAVagrantIssue(e)) {
                        // Destroy vm and retry
                        testPermutation.getMachineModel().forceApply(new RecreateMachine());
                        statement.evaluate();
                    } else {
                        throw e;
                    }
                }
                
            } catch (AssumptionViolatedException e) {
                eachNotifier.addFailedAssumption(e);
            } catch (Throwable e) {
                eachNotifier.addFailure(e);
            } finally {
                
                try {
                    testPermutation.getMachineModel().forceApply(new DownloadLogs(SharedConstants.TEST_LOGS_DIR + testName));
                } catch(Throwable e) {
                    
                }
                
                eachNotifier.fireTestFinished();
            }
        }

        @Override
        protected String getName()
        {
            return String.format("[%s]", testPermutation.getName());
        }

        @Override
        protected String testName(final FrameworkMethod method)
        {
            return String.format("%s[%s]", method.getName(),
                    testPermutation.getName());
        }

        @Override
        protected void validateConstructor(List<Throwable> errors)
        {
            validateOnlyOneConstructor(errors);
        }

        @Override
        protected Statement classBlock(RunNotifier notifier)
        {
            return childrenInvoker(notifier);
        }

        @Override
        protected Annotation[] getRunnerAnnotations()
        {
            return new Annotation[0];
        }
    }

    private class TestPermutation {

        private String name;
        private MachineModel model;

        public TestPermutation(String name, MachineModel model)
        {
            this.name = name;
            this.model = model;
        }

        public Object[] getConstructorArgs()
        {
            return new Object[]{model};
        }
        
        public MachineModel getMachineModel() {
            return model;
        }

        public String getName()
        {
            return name;
        }

    }

    private final ArrayList<Runner> runners = new ArrayList<Runner>();

    public StandaloneDatabaseQualityAssurance(Class<?> klass) throws Throwable
    {
        super(klass, Collections.<Runner> emptyList());

        for (TestPermutation pm : getTestPermutations(getTestClass()))
        {
            runners.add(new QualityAssuranceTestClassRunner(getTestClass()
                    .getJavaClass(), pm));
        }
    }

    @Override
    protected List<Runner> getChildren()
    {
        return runners;
    }

    // XXX: Here be dragons.
    private List<TestPermutation> getTestPermutations(TestClass klass)
            throws Throwable
    {
        Map<String, Neo4jDriver[]> platforms = new HashMap<String, Neo4jDriver[]>();
        List<TestPermutation> testParameters = new ArrayList<TestPermutation>();
        
        VirtualMachine windows = vm(Neo4jVM.WIN_1);
        VirtualMachine ubuntu = vm(Neo4jVM.UBUNTU_1);
        
        // Windows
        platforms.put(Platforms.WINDOWS, new Neo4jDriver[] {
            new WindowsCommunityDriver(  windows ),
            new WindowsAdvancedDriver(   windows ),
            new WindowsEnterpriseDriver( windows ) });
        
        // Ubuntu, with debian installer
        platforms.put(Platforms.UBUNTU_DEB, new Neo4jDriver[] {
                new UbuntuDebCommunityDriver(  ubuntu ),
                new UbuntuDebAdvancedDriver(   ubuntu ),
                new UbuntuDebEnterpriseDriver( ubuntu )});
        
        // Ubuntu, with tarball packages
        platforms.put(Platforms.UBUNTU_TAR_GZ, new Neo4jDriver[] {
                new UbuntuTarGzCommunityDriver(  ubuntu ),
                new UbuntuTarGzAdvancedDriver(   ubuntu ),
                new UbuntuTarGzEnterpriseDriver( ubuntu )});
        
        for(String platformKey : Platforms.selectedPlatforms()) {
            if(platforms.containsKey(platformKey)) {
                for(Neo4jDriver d : platforms.get(platformKey)) 
                {
                    String name = d.getClass().getSimpleName();
                    testParameters.add(new TestPermutation(name, new DefaultMachineModelImpl(d)));
                }
            }
        }
        
        return testParameters;
    }

}
