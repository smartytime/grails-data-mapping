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

package grails.gorm

import grails.async.Promise
import grails.async.Promises

import javax.persistence.FetchType

import org.codehaus.groovy.runtime.typehandling.GroovyCastException
import org.grails.datastore.gorm.async.AsyncQuery
import org.grails.datastore.gorm.finders.DynamicFinder
import org.grails.datastore.gorm.finders.FinderMethod
import org.grails.datastore.gorm.query.GormOperations
import org.grails.datastore.gorm.query.criteria.DetachedAssociationCriteria
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.model.PersistentEntity
import org.grails.datastore.mapping.model.types.Association
import org.grails.datastore.mapping.query.Projections
import org.grails.datastore.mapping.query.Query
import org.grails.datastore.mapping.query.Restrictions
import org.grails.datastore.mapping.query.Query.Criterion
import org.grails.datastore.mapping.query.Query.Junction
import org.grails.datastore.mapping.query.Query.Order
import org.grails.datastore.mapping.query.Query.Projection
import org.grails.datastore.mapping.query.Query.PropertyCriterion
import org.grails.datastore.mapping.query.Query.Order.Direction
import org.grails.datastore.mapping.query.api.Criteria
import org.grails.datastore.mapping.query.api.ProjectionList
import org.grails.datastore.mapping.query.api.QueryableCriteria

/**
 * Represents criteria that is not bound to the current connection and can be built up and re-used at a later date.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
class DetachedCriteria<T> implements QueryableCriteria<T>, Cloneable, Iterable<T>, GormOperations<T> {

    protected List<Criterion> criteria = []
    protected List<Order> orders = []
    protected List<Projection> projections = []
    protected Class targetClass
    protected List<DynamicFinder> dynamicFinders
    protected Integer defaultOffset
    protected Integer defaultMax

    protected List<Junction> junctions = []
    protected PersistentEntity persistentEntity
    protected Map<String, FetchType> fetchStrategies = [:]
    protected Closure lazyQuery

    ProjectionList projectionList = new DetachedProjections(projections)

    /**
     * Constructs a DetachedCriteria instance target the given class
     * @param targetClass
     */
    DetachedCriteria(Class<T> targetClass) {
        this.targetClass = targetClass
    }

    /**
     * Allow casting to a Promise
     * @param c The type to cast to
     * @return The cast type
     */
    public <N> N asType(Class<N> c) {
        if (c == Promise) {
            return (N)Promises.createPromise {
                list()
            }
        }
        else {
            throw new GroovyCastException(this, c)
        }
    }

    Map<String, FetchType> getFetchStrategies() {
        return fetchStrategies
    }

    /**
     * Specifies whether a join query should be used (if join queries are supported by the underlying datastore)
     *
     * @param property The property
     * @return The query
     */
    Criteria join(String property) {
        fetchStrategies[property] = FetchType.EAGER
        return this
    }

    /**
     * Specifies whether a select (lazy) query should be used (if join queries are supported by the underlying datastore)
     *
     * @param property The property
     * @return The query
     */
    Criteria select(String property) {
        fetchStrategies[property] = FetchType.LAZY
        return this
    }

    @Override
    T getPersistentClass() {
        getPersistentEntity().getJavaClass()
    }

    PersistentEntity getPersistentEntity() {
        if (persistentEntity == null) initialiseIfNecessary(targetClass)
        return persistentEntity
    }

    protected initialiseIfNecessary(Class<T> targetClass) {
        if (dynamicFinders != null) {
            return
        }

        try {
            dynamicFinders = targetClass.gormDynamicFinders
            persistentEntity = targetClass.gormPersistentEntity
        } catch (MissingPropertyException mpe) {
            throw new IllegalArgumentException("Class [$targetClass.name] is not a domain class")
        }
    }

    void add(Criterion criterion) {
        applyLazyCriteria()
        if (criterion instanceof PropertyCriterion) {
            if (criterion.value instanceof Closure) {
                criterion.value = buildQueryableCriteria(criterion.value)
            }
        }
        if (junctions)  {
            junctions[-1].add criterion
        }
        else {
            criteria << criterion
        }
    }

    List<Criterion> getCriteria() { criteria }

    List<Projection> getProjections() { projections }

    List<Order> getOrders() { orders }

    /**
     * Evaluate projections within the context of the given closure
     *
     * @param callable The callable
     * @return  The projection list
     */
    Criteria projections(Closure callable) {
        callable.delegate = projectionList
        callable.call()
        return this
    }

    /**
     * Handles a conjunction
     * @param callable Callable closure
     * @return This criterion
     */
    Criteria and(Closure callable) {
        junctions << new Query.Conjunction()
        handleJunction(callable)
        return this
    }

    /**
     * Handles a disjunction
     * @param callable Callable closure
     * @return This criterion
     */
    Criteria or(Closure callable) {
        junctions << new Query.Disjunction()
        handleJunction(callable)
        return this
    }

    /**
     * Handles a disjunction
     * @param callable Callable closure
     * @return This criterion
     */
    Criteria not(Closure callable) {
        junctions << new Query.Negation()
        handleJunction(callable)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria 'in'(String propertyName, Collection values) {
        inList(propertyName, values)
    }

    /**
     * @see Criteria
     */
    Criteria 'in'(String propertyName, Object[] values) {
        inList(propertyName, values)
    }

    /**
     * @see Criteria
     */
    Criteria order(String propertyName) {
        orders << new Order(propertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria order(String propertyName, String direction) {
        orders << new Order(propertyName, Direction.valueOf(direction.toUpperCase()))
        return this
    }

    /**
     * @see Criteria
     */
    Criteria inList(String propertyName, Collection values) {
        add Restrictions.in(propertyName, values)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria inList(String propertyName, Object[] values) {
        add Restrictions.in(propertyName, Arrays.asList(values))
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeEq(String propertyName, int size) {
        add Restrictions.sizeEq(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeGt(String propertyName, int size) {
        add Restrictions.sizeGt(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeGe(String propertyName, int size) {
        add Restrictions.sizeGe(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeLe(String propertyName, int size) {
        add Restrictions.sizeLe(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeLt(String propertyName, int size) {
        add Restrictions.sizeLt(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria sizeNe(String propertyName, int size) {
        add Restrictions.sizeNe(propertyName, size)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria eqProperty(String propertyName, String otherPropertyName) {
        add Restrictions.eqProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria neProperty(String propertyName, String otherPropertyName) {
        add Restrictions.neProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria gtProperty(String propertyName, String otherPropertyName) {
        add Restrictions.gtProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria geProperty(String propertyName, String otherPropertyName) {
        add Restrictions.geProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria ltProperty(String propertyName, String otherPropertyName) {
        add Restrictions.ltProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria leProperty(String propertyName, String otherPropertyName) {
        add Restrictions.leProperty(propertyName,otherPropertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria idEquals(Object value) {
        add Restrictions.idEq(value)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria isEmpty(String propertyName) {
        add Restrictions.isEmpty(propertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria isNotEmpty(String propertyName) {
        add Restrictions.isNotEmpty(propertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria isNull(String propertyName) {
        add Restrictions.isNull(propertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria isNotNull(String propertyName) {
        add Restrictions.isNotNull(propertyName)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria eq(String propertyName, Object propertyValue) {
        add Restrictions.eq(propertyName,propertyValue)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria idEq(Object propertyValue) {
        add Restrictions.idEq(propertyValue)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria ne(String propertyName, Object propertyValue) {
        add Restrictions.ne(propertyName,propertyValue)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria between(String propertyName, Object start, Object finish) {
        add Restrictions.between(propertyName, start, finish)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria gte(String property, Object value) {
        add Restrictions.gte(property,value)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria ge(String property, Object value) {
        gte(property, value)
    }

    /**
     * @see Criteria
     */
    Criteria gt(String property, Object value) {
        add Restrictions.gt(property,value)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria lte(String property, Object value) {
        add Restrictions.lte(property, value)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria le(String property, Object value) {
        lte(property,value)
    }

    /**
     * @see Criteria
     */
    Criteria lt(String property, Object value) {
        add Restrictions.lt(property,value)
        return this
    }

    /**
     * @see Criteria
     */
    Criteria like(String propertyName, Object propertyValue) {
        add Restrictions.like(propertyName,propertyValue.toString())
        return this
    }

    /**
     * @see Criteria
     */
    Criteria ilike(String propertyName, Object propertyValue) {
        add Restrictions.ilike(propertyName, propertyValue.toString())
        return this
    }

    /**
     * @see Criteria
     */
    Criteria rlike(String propertyName, Object propertyValue) {
        add Restrictions.rlike(propertyName, propertyValue.toString())
        return this
    }

    Criteria eqAll(String propertyName, Closure propertyValue) {
        eqAll(propertyName, buildQueryableCriteria(propertyValue))
    }

    private QueryableCriteria buildQueryableCriteria(Closure queryClosure) {
        return new DetachedCriteria(targetClass).build(queryClosure)
    }

    Criteria gtAll(String propertyName, Closure propertyValue) {
        gtAll(propertyName, buildQueryableCriteria(propertyValue))
    }

    Criteria ltAll(String propertyName, Closure propertyValue) {
        ltAll(propertyName, buildQueryableCriteria(propertyValue))
    }

    Criteria geAll(String propertyName, Closure propertyValue) {
        geAll(propertyName, buildQueryableCriteria(propertyValue))
    }

    Criteria leAll(String propertyName, Closure propertyValue) {
        leAll(propertyName, buildQueryableCriteria(propertyValue))
    }

    @Override
    Criteria eqAll(String propertyName, QueryableCriteria propertyValue) {
        add new Query.EqualsAll(propertyName, propertyValue)
        return this
    }

    @Override
    Criteria gtAll(String propertyName, QueryableCriteria propertyValue) {
        add new Query.GreaterThanAll(propertyName, propertyValue)
        return this
    }

    @Override
    Criteria ltAll(String propertyName, QueryableCriteria propertyValue) {
        add new Query.LessThanAll(propertyName, propertyValue)
        return this
    }

    @Override
    Criteria geAll(String propertyName, QueryableCriteria propertyValue) {
        add new Query.GreaterThanEqualsAll(propertyName, propertyValue)
        return this
    }

    @Override
    Criteria leAll(String propertyName, QueryableCriteria propertyValue) {
        add new Query.LessThanEqualsAll(propertyName, propertyValue)
        return this
    }

    class DetachedProjections implements ProjectionList {

        List<Projection> projections

        DetachedProjections(List<Projection> projections) {
            this.projections = projections
        }

        ProjectionList avg(String name) {
            projections << Projections.avg(name)
            return this
        }

        ProjectionList max(String name) {
            projections << Projections.max(name)
            return this
        }

        ProjectionList min(String name) {
            projections << Projections.min(name)
            return this
        }

        ProjectionList sum(String name) {
            projections << Projections.sum(name)
            return this
        }

        ProjectionList property(String name) {
            projections << Projections.property(name)
            return this
        }

        ProjectionList rowCount() {
            projections << Projections.count()
            return this
        }

        ProjectionList distinct(String property) {
            projections << Projections.distinct(property)
            return this
        }

        ProjectionList distinct() {
            projections << Projections.distinct()
            return this
        }

        ProjectionList countDistinct(String property) {
            projections << Projections.countDistinct(property)
            return this
        }

        ProjectionList count() {
            projections << Projections.count()
            return this
        }

        ProjectionList id() {
            projections << Projections.id()
            return this
        }
    }

    /**
     * Where method derives a new query from this query. This method will not mutate the original query, but instead return a new one.
     *
     * @param additionalQuery The additional query
     * @return A new query
     */
    DetachedCriteria<T> where(Closure additionalQuery) {
        DetachedCriteria<T> newQuery = clone()
        return newQuery.build(additionalQuery)
    }

    /**
     * Synonym for #get
     */
    T find(Map args = Collections.emptyMap(), Closure additionalCriteria = null) {
        get(args, additionalCriteria)
    }

    /**
     * Synonym for #get
     */
    T find(Closure additionalCriteria) {
        get(Collections.emptyMap(), additionalCriteria)
    }

    /**
     * Returns a single result matching the criterion contained within this DetachedCriteria instance
     *
     * @return A single entity
     */
    T get(Map args = Collections.emptyMap(), Closure additionalCriteria = null) {
        (T)withPopulatedQuery(args, additionalCriteria) { Query query ->
            query.singleResult()
        }
    }

    /**
     * Returns a single result matching the criterion contained within this DetachedCriteria instance
     *
     * @return A single entity
     */
    T get(Closure additionalCriteria) {
        get(Collections.emptyMap(), additionalCriteria)
    }

    /**
     * Returns a single result matching the criterion contained within this DetachedCriteria instance
     *
     * @return A list of matching instances
     */
    List<T> list(Map args = Collections.emptyMap(), Closure additionalCriteria = null) {
        (List)withPopulatedQuery(args, additionalCriteria) { Query query ->
            if (args?.max) {
                return new PagedResultList(query)
            }
            return query.list()
        }
    }

    /**
     * Lists all records matching the criterion contained within this DetachedCriteria instance
     *
     * @return A list of matching instances
     */
    List<T> list(Closure additionalCriteria) {
        list(Collections.emptyMap(), additionalCriteria)
    }

    /**
     * Counts the number of records returned by the query
     *
     * @param args The arguments
     * @return The count
     */
    Number count(Map args = Collections.emptyMap(), Closure additionalCriteria = null) {
        (Number)withPopulatedQuery(args, additionalCriteria) { Query query ->
            query.projections().count()
            query.singleResult()
        }
    }

    /**
     * Counts the number of records returned by the query
     *
     * @param args The arguments
     * @return The count
     */
    Number count(Closure additionalCriteria) {
        (Number)withPopulatedQuery(Collections.emptyMap(), additionalCriteria) { Query query ->
            query.projections().count()
            query.singleResult()
        }
    }

    /**
     * Counts the number of records returned by the query
     *
     * @param args The arguments
     * @return The count
     */
    boolean exists(Closure additionalCriteria = null) {
        (Boolean)withPopulatedQuery(Collections.emptyMap(), additionalCriteria) { Query query ->
            query.projections().count()
            query.singleResult() > 0
        }
    }

    /**
     * Deletes all entities matching this criteria
     *
     * @return The total number deleted
     */
    Number deleteAll() {
        targetClass.withDatastoreSession { Session session ->
            session.deleteAll(this)
        }
    }

    /**
     * Updates all entities matching this criteria
     *
     * @return The total number deleted
     */
    Number updateAll(Map properties) {
        targetClass.withDatastoreSession { Session session ->
            session.updateAll(this, properties)
        }
    }

    /**
     * Enable the builder syntax for contructing Criteria
     *
     * @param callable The callable closure
     * @return This criteria instance
     */

    DetachedCriteria<T> build(Closure callable) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.with callable
        return newCriteria
    }

    /**
     * Enable the builder syntax for contructing Criteria
     *
     * @param callable The callable closure
     * @return This criteria instance
     */

    DetachedCriteria<T> buildLazy(Closure callable) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.lazyQuery = callable
        return newCriteria
    }

    /**
     * Sets the default max to use and returns a new criteria instance. This method does not mutate the original criteria!
     *
     * @param max The max to use
     * @return A new DetachedCriteria instance derived from this
     */
    DetachedCriteria<T> max(int max) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.defaultMax = max
        return newCriteria
    }

    /**
     * Sets the default offset to use and returns a new criteria instance. This method does not mutate the original criteria!
     *
     * @param offset The offset to use
     * @return A new DetachedCriteria instance derived from this
     */
    DetachedCriteria<T> offset(int offset) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.defaultOffset = offset
        return newCriteria
    }

    /**
     * Adds a sort order to this criteria instance
     *
     * @param property The property to sort by
     * @return This criteria instance
     */
    DetachedCriteria<T> sort(String property) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.orders.add(new Order(property))
        return newCriteria
    }

    /**
     * Adds a sort order to this criteria instance
     *
     * @param property The property to sort by
     * @param direction The direction to sort by
     * @return This criteria instance
     */
    DetachedCriteria<T> sort(String property, String direction) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.orders.add(new Order(property, "desc".equalsIgnoreCase(direction) ? Direction.DESC : Direction.ASC))
        return newCriteria
    }

    /**
     * Adds a property projection
     *
     * @param property The property to sort by
     * @return This criteria instance
     */
    DetachedCriteria<T> property(String property) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.projectionList.property(property)
        return newCriteria
    }

    def propertyMissing(String name) {
        final entity = getPersistentEntity()
        final p = entity.getPropertyByName(name)
        if (p == null) {
            throw new MissingPropertyException(name, DetachedCriteria)
        }
        return property(name)
    }

    /**
     * Adds a distinct property projection
     *
     * @param property The property to obtain the distinct value for
     * @return This criteria instance
     */
    DetachedCriteria<T> distinct(String property) {
        DetachedCriteria newCriteria = this.clone()
        newCriteria.projectionList.distinct(property)
        return newCriteria
    }

    /**
     * @return The async version of the DetachedCriteria API
     */
    AsyncQuery<T> getAsync() {
        return new AsyncQuery<T>(this)
    }

    /**
     * Method missing handler that deals with the invocation of dynamic finders
     *
     * @param methodName The method name
     * @param args The arguments
     * @return The result of the method call
     */
    def methodMissing(String methodName, args) {
        initialiseIfNecessary(targetClass)
        def method = dynamicFinders.find { FinderMethod f -> f.isMethodMatch(methodName) }
        if (method != null) {
            return method.invoke(targetClass, methodName,this, args)
        }

        if (!args || args.size() != 1 || !(args[-1] instanceof Closure)) {
            throw new MissingMethodException(methodName, DetachedCriteria, args)
        }

        final prop = persistentEntity.getPropertyByName(methodName)
        if (!(prop instanceof Association)) {
            throw new MissingMethodException(methodName, DetachedCriteria, args)
        }

        def associationCriteria = new DetachedAssociationCriteria(prop.associatedEntity.javaClass, prop)
        add associationCriteria
        final callable = args[-1]
        callable.delegate = associationCriteria
        callable.call()
    }

    @Override
    Iterator<T> iterator() {
        return list().iterator()
    }

    @Override
    protected DetachedCriteria<T> clone() {
        def criteria = new DetachedCriteria(targetClass)
        criteria.criteria = new ArrayList(this.criteria)
        final projections = new ArrayList(this.projections)
        criteria.projections = projections
        criteria.projectionList = new DetachedProjections(projections)
        criteria.orders = new ArrayList(this.orders)
        criteria.defaultMax = defaultMax
        criteria.defaultOffset = defaultOffset
        return criteria
    }

    protected void handleJunction(Closure callable) {
        try {
            callable.delegate = this
            callable.call()
        }
        finally {
            def lastJunction = junctions.remove(junctions.size() - 1)
            add lastJunction
        }
    }

    private withPopulatedQuery(Map args, Closure additionalCriteria, Closure callable)  {
        targetClass.withDatastoreSession { Session session ->
            applyLazyCriteria()
            Query query = session.createQuery(targetClass)
            if (defaultMax != null) {
                query.max(defaultMax)
            }
            if (defaultOffset != null) {
                query.offset(defaultOffset)
            }
            DynamicFinder.applyDetachedCriteria(query, this)

            if (additionalCriteria != null) {
                def additionalDetached = new DetachedCriteria(targetClass).build(additionalCriteria)
                DynamicFinder.applyDetachedCriteria(query, additionalDetached)
            }

            DynamicFinder.populateArgumentsForCriteria(targetClass, query, args)

            callable.call(query)
        }
    }

    private void applyLazyCriteria() {
        if (lazyQuery == null) {
            return
        }

        def criteria = lazyQuery
        lazyQuery = null
        this.with criteria
    }
}
