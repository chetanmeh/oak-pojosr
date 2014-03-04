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

import de.kalpatec.pojosr.framework.launch.PojoServiceRegistry;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleException;

public class OSGiBasedRepository extends RepositoryImpl {

    private final PojoServiceRegistry registry;

    public OSGiBasedRepository(ContentRepository contentRepository,
                               Whiteboard whiteboard,
                               SecurityProvider securityProvider,
                               PojoServiceRegistry registry) {
        super(contentRepository, whiteboard, securityProvider);
        this.registry = registry;
    }

    @Override
    public void shutdown() {
        try {
            OakOSGiRepositoryFactory.shutdown(registry);
        } catch (BundleException e) {
            //Shutdown method does not have RepositoryException as part of signature
            //wrap and throw as RuntimeException
            throw new RuntimeException("Error shutting down ServiceRegistry ", e);
        }
        super.shutdown();
    }
}
