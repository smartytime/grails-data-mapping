package org.codehaus.groovy.grails.orm.hibernate

import grails.orm.HibernateCriteriaBuilder
import groovy.transform.CompileStatic
import org.codehaus.groovy.grails.commons.DomainClassArtefactHandler
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.domain.GrailsDomainClassMappingContext
import org.codehaus.groovy.grails.orm.hibernate.metaclass.*
import org.grails.datastore.gorm.GormStaticApi
import org.grails.datastore.gorm.finders.FinderMethod
import org.grails.datastore.mapping.query.api.Criteria as GrailsCriteria
import org.hibernate.SessionFactory
import org.springframework.core.convert.ConversionService
import org.springframework.transaction.PlatformTransactionManager

/**
 * The implementation of the GORM static method contract for Hibernate
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@CompileStatic
class HibernateGormStaticApi<D> extends GormStaticApi<D> {

    private GrailsHibernateTemplate hibernateTemplate
    private SessionFactory sessionFactory
    private ConversionService conversionService
    private Class identityType
    private ListPersistentMethod listMethod
    private FindAllPersistentMethod findAllMethod
    private FindPersistentMethod findMethod
    private ExecuteQueryPersistentMethod executeQueryMethod
    private ExecuteUpdatePersistentMethod executeUpdateMethod
    private MergePersistentMethod mergeMethod
    private ClassLoader classLoader
    private GrailsApplication grailsApplication
    private boolean cacheQueriesByDefault = false

    HibernateGormStaticApi(Class<D> persistentClass, HibernateDatastore datastore, List<FinderMethod> finders,
                ClassLoader classLoader, PlatformTransactionManager transactionManager) {
        super(persistentClass, datastore, finders)

         super.transactionManager = transactionManager

        this.classLoader = classLoader
        sessionFactory = datastore.getSessionFactory()
        conversionService = datastore.mappingContext.conversionService


        identityType = persistentEntity.identity?.type

        def mappingContext = datastore.mappingContext
        if (mappingContext instanceof GrailsDomainClassMappingContext) {
            GrailsDomainClassMappingContext domainClassMappingContext = (GrailsDomainClassMappingContext)mappingContext
            grailsApplication = domainClassMappingContext.getGrailsApplication()

            GrailsDomainClass domainClass = (GrailsDomainClass)grailsApplication.getArtefact(DomainClassArtefactHandler.TYPE, persistentClass.name)
            identityType = domainClass.identifier?.type

            mergeMethod = new MergePersistentMethod(sessionFactory, classLoader, grailsApplication, domainClass, datastore)
            listMethod = new ListPersistentMethod(grailsApplication, sessionFactory, classLoader, mappingContext.conversionService)
            hibernateTemplate = new GrailsHibernateTemplate(sessionFactory, grailsApplication)
            hibernateTemplate.setCacheQueries(cacheQueriesByDefault)
        } else {
            hibernateTemplate = new GrailsHibernateTemplate(sessionFactory)
        }

        executeQueryMethod = new ExecuteQueryPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)
        executeUpdateMethod = new ExecuteUpdatePersistentMethod(sessionFactory, classLoader, grailsApplication)
        findMethod = new FindPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)
        findAllMethod = new FindAllPersistentMethod(sessionFactory, classLoader, grailsApplication, conversionService)


    }

    @Override
    GrailsCriteria createCriteria() {
        def builder = new HibernateCriteriaBuilder(persistentClass, sessionFactory)
        builder.grailsApplication = grailsApplication
        builder.conversionService = conversionService
        builder
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

    List<D> executeQuery(String query, arg) {
        (List<D>)executeQueryMethod.invoke(persistentClass, "executeQuery", [query, arg] as Object[])
    }

    D find(String query, Object[] params) {
        findMethod.invoke(persistentClass, "find", [query, params] as Object[]) as D
    }


}
