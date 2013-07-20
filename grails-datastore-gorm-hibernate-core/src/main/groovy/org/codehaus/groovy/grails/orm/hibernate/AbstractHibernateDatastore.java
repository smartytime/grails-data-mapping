/* Copyright (C) 2011 SpringSource
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codehaus.groovy.grails.orm.hibernate;

import groovy.util.ConfigObject;

import org.grails.datastore.mapping.core.AbstractDatastore;
import org.grails.datastore.mapping.model.MappingContext;
import org.hibernate.SessionFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Datastore implementation that uses a Hibernate SessionFactory underneath.
 *
 * @author Graeme Rocher
 * @since 2.0
 */
public abstract class AbstractHibernateDatastore extends AbstractDatastore implements ApplicationContextAware {

    protected SessionFactory sessionFactory;
    protected ConfigObject config;
    protected AbstractEventTriggeringInterceptor eventTriggeringInterceptor;

    protected AbstractHibernateDatastore(MappingContext mappingContext, SessionFactory sessionFactory, ConfigObject config) {
        super(mappingContext);
        this.sessionFactory = sessionFactory;
        this.config = config;
        initializeConverters(mappingContext);
    }

    public AbstractHibernateDatastore(MappingContext mappingContext, SessionFactory sessionFactory, ConfigObject config, ApplicationContext applicationContext) {
        this(mappingContext, sessionFactory, config);
        setApplicationContext(applicationContext);
    }

    /**
     * @return The Hibernate {@link SessionFactory} being used by this datastore instance
     */
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    @Override
    protected boolean registerValidationListener() {
        return false;
    }

    // for testing
    public AbstractEventTriggeringInterceptor getEventTriggeringInterceptor() {
        return eventTriggeringInterceptor;
    }
}
