/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run.osgi;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.JcrUtils;
import org.junit.Before;
import org.junit.Test;

import static org.apache.commons.io.FilenameUtils.concat;
import static org.junit.Assert.assertNotNull;

public class OakOSGiRepositoryFactoryTest {

    private String repositoryHome;
    private RepositoryFactory repositoryFactory = new OakOSGiRepositoryFactory();
    private Map config = new HashMap();

    @Before
    public void setUp() throws IOException {
        repositoryHome = concat(getBaseDir(), "target/repository");
        config.put("org.apache.jackrabbit.repository.home", repositoryHome);

        File repoHome = new File(repositoryHome);
        if(repoHome.exists()){
            FileUtils.cleanDirectory(new File(repositoryHome));
        }
        copyConfig("common");
    }

    @Test
    public void testRepositoryTar() throws Exception {
        copyConfig("tar");

        Repository repository = repositoryFactory.getRepository(config);
        assertNotNull(repository);
        basicCrudTest(repository);
        System.out.println("Repository started ");

        shutdown(repository);
    }

    private void basicCrudTest(Repository repository) throws RepositoryException {
        Session session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Node rootNode = session.getRootNode();

        Node child  = JcrUtils.getOrAddNode(rootNode,"child", "oak:Unstructured");
        child.setProperty("foo3",  "bar3");
        session.logout();

        System.out.println("Basic test passed");
    }

    private void shutdown(Repository repository) {
        if(repository instanceof JackrabbitRepository){
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    private void copyConfig(String type) throws IOException {
        FileUtils.copyDirectory(new File(concat(getBaseDir(),"src/test/resources/config-"+type)),
                new File(concat(repositoryHome,"config")));
    }

    private static String getBaseDir(){
        return new File(".").getAbsolutePath();
    }
}
