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

package org.grails.datastore.gorm.query.criteria

import grails.gorm.DetachedCriteria

import org.grails.datastore.mapping.model.types.Association
import org.grails.datastore.mapping.query.Query.Criterion
import org.grails.datastore.mapping.query.api.AssociationCriteria

/**
 * Criterion related to an association
 *
 * @author Graeme Rocher
 * @since 1.0
 */
class DetachedAssociationCriteria<T> extends DetachedCriteria<T> implements Criterion, AssociationCriteria {

    Association association

    DetachedAssociationCriteria(Class<T> targetClass, Association association) {
        super(targetClass)
        this.association = association
    }
}
