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
package org.codehaus.groovy.grails.orm.hibernate.query;

import grails.orm.HibernateCriteriaBuilder;
import grails.orm.RlikeExpression;

import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.query.Query;
import org.grails.datastore.mapping.query.api.QueryableCriteria;
import org.hibernate.criterion.Criterion;

/**
 * @author Graeme Rocher
 * @since 2.0
 */
public class HibernateCriterionAdapter extends AbstractHibernateCriterionAdapter {

    public HibernateCriterionAdapter(PersistentEntity entity, Query.Criterion criterion, String alias) {
        super(entity, criterion, alias);
    }

    public HibernateCriterionAdapter(Query.Criterion criterion) {
        super(criterion);
    }

    protected Criterion createRlikeExpression(String propertyName, String pattern) {
        return new RlikeExpression(propertyName, pattern);
    }

    protected Object getHibernateDetachedCriteria(QueryableCriteria<?> value) {
        return HibernateCriteriaBuilder.getHibernateDetachedCriteria(value);
    }
}
