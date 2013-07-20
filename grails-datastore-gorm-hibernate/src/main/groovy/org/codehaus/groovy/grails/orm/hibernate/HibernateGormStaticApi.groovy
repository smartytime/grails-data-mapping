/*
 * Copyright 2013 the original author or authors.
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
package org.codehaus.groovy.grails.orm.hibernate

import grails.orm.HibernateCriteriaBuilder
import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import org.codehaus.groovy.grails.orm.hibernate.metaclass.ExecuteQueryPersistentMethod
import org.codehaus.groovy.grails.orm.hibernate.metaclass.ExecuteUpdatePersistentMethod
import org.codehaus.groovy.grails.orm.hibernate.metaclass.FindAllPersistentMethod
import org.codehaus.groovy.grails.orm.hibernate.metaclass.FindPersistentMethod
import org.grails.datastore.gorm.finders.FinderMethod
import org.grails.datastore.mapping.query.api.Criteria
import org.hibernate.Session
import org.springframework.orm.hibernate4.SessionFactoryUtils
import org.springframework.orm.hibernate4.SessionHolder
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager

/**
 * The implementation of the GORM static method contract for Hibernate
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@CompileStatic
class HibernateGormStaticApi<D> extends AbstractHibernateGormStaticApi<D> {

    HibernateGormStaticApi(Class<D> persistentClass, HibernateDatastore datastore, List<FinderMethod> finders,
                ClassLoader classLoader, PlatformTransactionManager transactionManager) {
        super(persistentClass, datastore, finders, classLoader, transactionManager)

        executeQueryMethod = new ExecuteQueryPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)
        executeUpdateMethod = new ExecuteUpdatePersistentMethod(sessionFactory, classLoader, grailsApplication)
        findMethod = new FindPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)
        findAllMethod = new FindAllPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)
    }


    @Override
    Criteria createCriteria() {
        def builder = new HibernateCriteriaBuilder(persistentClass, sessionFactory)
        builder.grailsApplication = grailsApplication
        builder.conversionService = conversionService
        builder
    }



}
