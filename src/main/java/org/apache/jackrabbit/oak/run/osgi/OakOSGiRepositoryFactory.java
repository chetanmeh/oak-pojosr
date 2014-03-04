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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;

import com.google.common.util.concurrent.SettableFuture;
import de.kalpatec.pojosr.framework.launch.ClasspathScanner;
import de.kalpatec.pojosr.framework.launch.PojoServiceRegistry;
import de.kalpatec.pojosr.framework.launch.PojoServiceRegistryFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OakOSGiRepositoryFactory implements RepositoryFactory{

    private static Logger log = LoggerFactory.getLogger(OakOSGiRepositoryFactory.class);
    /**
     * Name of the repository home parameter.
     */
    public static final String REPOSITORY_HOME
            = "org.apache.jackrabbit.repository.home";

    public static final String REPOSITORY_STARTUP_TIMEOUT
            = "org.apache.jackrabbit.repository.startupTimeOut";

    /**
     * Default timeout for repository creation
     */
    private static final int DEFAULT_TIMEOUT = (int) TimeUnit.MINUTES.toSeconds(10);

    public Repository getRepository(Map parameters) throws RepositoryException {
        Map config = new HashMap();
        config.putAll(parameters);

        processConfig(config);

        PojoServiceRegistry registry = createServiceRegistry(config);
        postProcessRegistry(registry);

        //Future which would be used to notify when repository is ready
        // to be used
        SettableFuture<Repository> repoFuture = SettableFuture.create();

        //Start the tracker for repository creation
        new RepositoryTracker(registry, repoFuture);

        //Now wait for repository to be created with given timeout
        //if repository creation takes more time. This is required to handle case
        // where OSGi runtime fails to start due to bugs (like cycles)
        int timeout = getTimeoutInSeconds(config);
        try {
            return repoFuture.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException("Repository initialization was interrupted");
        } catch (ExecutionException e) {
            throw new RepositoryException(e);
        } catch (TimeoutException e) {
            try {
                shutdown(registry);
            } catch (BundleException be) {
                log.warn("Error occurred while shutting down the service registry backing the Repository ", be);
            }
            throw new RepositoryException("Repository could not be started in "+
                    timeout+" seconds",e);
        }
    }

    /**
     * Enables post processing of service registry e.g. registering new services etc
     * by sub classes
     *
     * @param registry service registry
     */
    protected void postProcessRegistry(PojoServiceRegistry registry){

    }

    static void shutdown(PojoServiceRegistry registry) throws BundleException{
        if(registry != null){
            registry.getBundleContext().getBundle().stop();
        }
    }

    private static int getTimeoutInSeconds(Map config) {
        Integer timeout = (Integer) config.get(REPOSITORY_STARTUP_TIMEOUT);
        if(timeout == null){
            timeout = DEFAULT_TIMEOUT;
        }
        return timeout;
    }

    private static void processConfig(Map config) {
        String home = (String) config.get(REPOSITORY_HOME);
        checkNotNull(home, "Repository home not defined via [%s]", REPOSITORY_HOME);

        home = FilenameUtils.normalizeNoEndSeparator(home);

        String bundleDir = FilenameUtils.concat(home, "bundles");
        config.put(Constants.FRAMEWORK_STORAGE, bundleDir);

        //FIXME Pojo SR currently reads this from system property instead of Framework Property
        System.setProperty(Constants.FRAMEWORK_STORAGE, bundleDir);

        //Directory used by Felix File Install to watch for configs
        config.put("felix.fileinstall.dir", FilenameUtils.concat(home, "config"));
        config.put("felix.fileinstall.log.level", 4);

        //Directory used by Felix File Install to watch for configs
        config.put("repository.home", FilenameUtils.concat(home, "repository"));
    }

    private static PojoServiceRegistry createServiceRegistry(Map<String,Object> config) {
        try {
            config.put(PojoServiceRegistryFactory.BUNDLE_DESCRIPTORS, new ClasspathScanner().scanForBundles());
            ServiceLoader<PojoServiceRegistryFactory> loader = ServiceLoader.load(PojoServiceRegistryFactory.class);
            return loader.iterator().next().newPojoServiceRegistry(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class RepositoryTracker extends ServiceTracker {
        private final SettableFuture<Repository> repoFuture;
        private RepositoryProxy proxy;

        public RepositoryTracker(PojoServiceRegistry registry, SettableFuture<Repository> repoFuture) {
            super(registry.getBundleContext(), Repository.class.getName(), null);
            this.repoFuture = repoFuture;
            this.open();
        }

        @Override
        public Object addingService(ServiceReference reference) {
            Object service = super.addingService(reference);
            if(proxy == null){
                //As its possible that future is accessed before the service
                //get registered with tracker. We also capture the initial reference
                //and use that for the first access case
                repoFuture.set(createProxy((Repository) service));
            }
            return service;
        }

        @Override
        public void removedService(ServiceReference reference, Object service) {
            if(proxy != null){
                proxy.clearInitialReference();
            }
        }

        private Repository createProxy(Repository service) {
            proxy = new RepositoryProxy(this,service);
            return (Repository) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[] {Repository.class, JackrabbitRepository.class}, proxy);
        }
    }

    /**
     * Due to the way SecurityConfiguration is managed in OSGi env its possible
     * that repository gets created/shutdown few times. So need to have a proxy
     * to access the latest service
     */
    private static class RepositoryProxy implements InvocationHandler {
        private final RepositoryTracker tracker;
        private Repository initialService;

        private RepositoryProxy(RepositoryTracker tracker, Repository initialService) {
            this.tracker = tracker;
            this.initialService = initialService;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object obj = tracker.getService();
            if(obj == null){
                obj = initialService;
            }
            return method.invoke(obj, args);
        }

        public void clearInitialReference(){
            this.initialService = null;
        }
    }

}
