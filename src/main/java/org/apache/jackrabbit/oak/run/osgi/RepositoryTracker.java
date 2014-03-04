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

import javax.jcr.Repository;

import com.google.common.util.concurrent.SettableFuture;
import de.kalpatec.pojosr.framework.launch.PojoServiceRegistry;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.version.VersionEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexProvider;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;


/**
 * Tracker to track repository related dependencies. It assumes being used in static
 * environment like pojoSR and does not take care of situation like NodeStore getting
 * unbounded
 */
public class RepositoryTracker extends ServiceTracker {
    private final PojoServiceRegistry registry;
    private final SettableFuture<Repository> repoFuture;
    private final Whiteboard whiteboard;

    private final WhiteboardIndexProvider indexProvider = new WhiteboardIndexProvider();
    private final WhiteboardIndexEditorProvider indexEditorProvider = new WhiteboardIndexEditorProvider();

    private NodeStore store = null;
    private SecurityProvider securityProvider = null;

    public RepositoryTracker(PojoServiceRegistry registry, SettableFuture<Repository> repoFuture) {
        super(registry.getBundleContext(), createFilter(), null);
        this.registry = registry;
        this.repoFuture = repoFuture;

        this.whiteboard = new OsgiWhiteboard(registry.getBundleContext());
        indexProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);

        this.open();
    }

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = super.addingService(reference);

        //TODO This logic could be modelled as a DS Component like OSgiRepositoryManager
        //which has dependency on all such services and tracker just tracks that
        if(service instanceof NodeStore){
            this.store = (NodeStore) service;
        } else if( service instanceof SecurityProvider){
            this.securityProvider = (SecurityProvider) service;
        }

        createRepositoryIfPossible();

        return service;
    }

    private void createRepositoryIfPossible() {
        if(!dependenciesSatisfied()){
            return;
        }

        Oak oak = new Oak(store)
                .with(whiteboard)
                .with(new InitialContent())

                .with(JcrConflictHandler.JCR_CONFLICT_HANDLER)
                .with(new EditorHook(new VersionEditorProvider()))

                .with(securityProvider)

                .with(new NameValidatorProvider())
                .with(new NamespaceEditorProvider())
                .with(new TypeEditorProvider())
                .with(new ConflictValidatorProvider())

                        // index stuff
                .with(indexProvider)
                .with(indexEditorProvider)
                .withAsyncIndexing();

        ContentRepository contentRepository = oak.createContentRepository();
        Repository repository =
                new OSGiBasedRepository(contentRepository, whiteboard, securityProvider, registry);
        repoFuture.set(repository);
    }

    private boolean dependenciesSatisfied() {
        return store != null
                && securityProvider != null;
    }

    private static Filter createFilter() {
        try {
            return FrameworkUtil.createFilter(String.format("(|(objectClass=%s)(objectClass=%s))",
                    NodeStore.class.getName(), SecurityProvider.class.getName()));
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
