package org.codehaus.groovy.grails.orm.hibernate

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import org.codehaus.groovy.grails.commons.DomainClassArtefactHandler
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.commons.metaclass.StaticMethodInvocation
import org.grails.datastore.gorm.GormStaticApi
import org.grails.datastore.gorm.config.GrailsDomainClassMappingContext
import org.grails.datastore.gorm.finders.FinderMethod
import org.grails.datastore.mapping.query.api.Criteria
import org.hibernate.SessionFactory
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager

/**
 * Created with IntelliJ IDEA.
 * User: ericm
 * Date: 7/19/13
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class AbstractHibernateGormStaticApi<D> extends GormStaticApi<D> {
    protected StaticMethodInvocation findAllMethod
    protected StaticMethodInvocation findMethod
    protected StaticMethodInvocation executeQueryMethod
    protected StaticMethodInvocation executeUpdateMethod

    GrailsApplication grailsApplication
    SessionFactory sessionFactory

    ClassLoader classLoader

    AbstractHibernateGormStaticApi(Class<D> persistentClass, AbstractHibernateDatastore datastore, List<FinderMethod> finders, ClassLoader classLoader,
                                                                                                      PlatformTransactionManager transactionManager) {
        super(persistentClass, datastore, finders)

        super.transactionManager = transactionManager
        this.classLoader = classLoader
        sessionFactory = datastore.getSessionFactory()
        conversionService = datastore.mappingContext.conversionService

        grailsApplication = datastore.applicationContext.getBean(GrailsApplication.class)
    }



    /**
     * Finds a single result for the given query and arguments and a maximum results to return value
     *
     * @param query The query
     * @param args The arguments
     * @param max The maximum to return
     * @return A single result or null
     *
     * @deprecated Use Book.find('..', [foo:'bar], [max:10]) instead
     */
    @Deprecated
    D find(String query, Map args, Integer max) {
        findMethod.invoke(persistentClass, "find", [query, args, max] as Object[]) as D
    }

    /**
     * Finds a single result for the given query and arguments and a maximum results to return value
     *
     * @param query The query
     * @param args The arguments
     * @param max The maximum to return
     * @param offset The offset
     * @return A single result or null
     *
     * @deprecated Use Book.find('..', [foo:'bar], [max:10, offset:5]) instead
     */
    @Deprecated
    D find(String query, Map args, Integer max, Integer offset) {
        findMethod.invoke(persistentClass, "find", [query, args, max, offset] as Object[]) as D
    }

    /**
     * Finds a single result for the given query and a maximum results to return value
     *
     * @param query The query
     * @param max The maximum to return
     * @return A single result or null
     *
     * @deprecated Use Book.find('..', [max:10]) instead
     */
    @Deprecated
    D find(String query, Integer max) {
        findMethod.invoke(persistentClass, "find", [query, max] as Object[]) as D
    }

    /**
     * Finds a single result for the given query and a maximum results to return value
     *
     * @param query The query
     * @param max The maximum to return
     * @param offset The offset to use
     * @return A single result or null
     *
     * @deprecated Use Book.find('..', [max:10, offset:5]) instead
     */
    @Deprecated
    D find(String query, Integer max, Integer offset) {
        findMethod.invoke(persistentClass, "find", [query, max, offset] as Object[]) as D
    }

    /**
     * Finds a list of results for the given query and arguments and a maximum results to return value
     *
     * @param query The query
     * @param args The arguments
     * @param max The maximum to return
     * @return A list of results
     *
     * @deprecated Use findAll('..', [foo:'bar], [max:10]) instead
     */
    @Deprecated
    List<D> findAll(String query, Map args, Integer max) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, args, max] as Object[])
    }

    /**
     * Finds a list of results for the given query and arguments and a maximum results to return value
     *
     * @param query The query
     * @param args The arguments
     * @param max The maximum to return
     * @param offset The offset
     *
     * @return A list of results
     *
     * @deprecated Use findAll('..', [foo:'bar], [max:10, offset:5]) instead
     */
    @Deprecated
    List<D> findAll(String query, Map args, Integer max, Integer offset) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, args, max, offset] as Object[])
    }

    /**
     * Finds a list of results for the given query and a maximum results to return value
     *
     * @param query The query
     * @param max The maximum to return
     * @return A list of results
     *
     * @deprecated Use findAll('..', [max:10]) instead
     */
    @Deprecated
    List<D> findAll(String query, Integer max) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, max] as Object[])
    }

    /**
     * Finds a list of results for the given query and a maximum results to return value
     *
     * @param query The query
     * @param max The maximum to return
     * @return A list of results
     *
     * @deprecated Use findAll('..', [max:10, offset:5]) instead
     */
    @Deprecated
    List<D> findAll(String query, Integer max, Integer offset) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, max, offset] as Object[])
    }


    @Override
    List<D> executeQuery(String query) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query] as Object[])
    }

    List<D> executeQuery(String query, arg) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, arg] as Object[])
    }

    @Override
    List<D> executeQuery(String query, Map args) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, args] as Object[])
    }

    @Override
    List<D> executeQuery(String query, Map params, Map args) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, params, args] as Object[])
    }

    @Override
    List<D> executeQuery(String query, Collection params) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, params] as Object[])
    }

    @Override
    List<D> executeQuery(String query, Collection params, Map args) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, params, args] as Object[])
    }

    @Override
    Integer executeUpdate(String query) {
        (Integer)executeUpdateMethod.invoke(persistentClass, "executeUpdate", [query] as Object[])
    }

    @Override
    Integer executeUpdate(String query, Map args) {
        (Integer)executeUpdateMethod.invoke(persistentClass, "executeUpdate", [query, args] as Object[])
    }

    @Override
    Integer executeUpdate(String query, Map params, Map args) {
        (Integer)executeUpdateMethod.invoke(persistentClass, "executeUpdate", [query, params, args] as Object[])
    }

    @Override
    Integer executeUpdate(String query, Collection params) {
        (Integer)executeUpdateMethod.invoke(persistentClass, "executeUpdate", [query, params] as Object[])
    }

    @Override
    Integer executeUpdate(String query, Collection params, Map args) {
        (Integer)executeUpdateMethod.invoke(persistentClass, "executeUpdate", [query, params, args] as Object[])
    }

    @Override
    D find(String query) {
        findMethod.invoke(persistentClass, "find", [query] as Object[]) as D
    }

    D find(String query, Object[] params) {
        findMethod.invoke(persistentClass, "find", [query, params] as Object[]) as D
    }

    @Override
    D find(String query, Map args) {
        findMethod.invoke(persistentClass, "find", [query, args] as Object[]) as D
    }

    @Override
    D find(String query, Map params, Map args) {
        findMethod.invoke(persistentClass, "find", [query, params, args] as Object[]) as D
    }

    @Override
    Object find(String query, Collection params) {
        findMethod.invoke(persistentClass, "find", [query, params] as Object[])
    }

    @Override
    D find(String query, Collection params, Map args) {
        findMethod.invoke(persistentClass, "find", [query, params, args] as Object[]) as D
    }

    @Override
    List<D> findAll(String query) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query] as Object[])
    }

    @Override
    List<D> findAll(String query, Map args) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, args] as Object[])
    }

    @Override
    List<D> findAll(String query, Map params, Map args) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, params, args] as Object[])
    }

    @Override
    List<D> findAll(String query, Collection params) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, params] as Object[])
    }

    @Override
    List<D> findAll(String query, Collection params, Map args) {
        (List<D>)findAllMethod.invoke(persistentClass, "findAll", [query, params, args] as Object[])
    }

}
