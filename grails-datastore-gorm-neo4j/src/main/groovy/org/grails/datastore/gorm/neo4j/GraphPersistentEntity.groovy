/* Copyright (C) 2010 SpringSource
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
package org.grails.datastore.gorm.neo4j

import org.grails.datastore.mapping.config.Entity
import org.grails.datastore.mapping.model.AbstractClassMapping
import org.grails.datastore.mapping.model.AbstractPersistentEntity
import org.grails.datastore.mapping.model.ClassMapping
import org.grails.datastore.mapping.model.MappingContext

/**
 * @author Stefan Armbruster <stefan@armbruster-it.de>
 */
@SuppressWarnings("unchecked")
class GraphPersistentEntity extends AbstractPersistentEntity {

    Entity mappedForm

    GraphPersistentEntity(Class javaClass, MappingContext context) {
        super(javaClass, context)
        this.mappedForm = context.getMappingFactory().createMappedForm(this);
    }

    @Override
    ClassMapping getMapping() {
        return new AbstractClassMapping<Entity>(this, context) {
            @Override
            public Entity getMappedForm() {
                return GraphPersistentEntity.this.mappedForm;
            }
        };
    }
}
