package org.grails.datastore.gorm.utils;

import org.codehaus.groovy.grails.commons.GrailsDomainClass;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import org.springframework.core.convert.ConversionService;

/**
 * This class was previously HibernateConversionUtils, but the logic for PK conversion was moved from Hibernate
 * into the base GORM api, so this logic needed to jump modules.
 *
 * todo: Should remove the Hibernate version and migrate all usages to this class.
 *
 * User: ericm
 * Date: 7/14/13
 * Time: 2:23 AM
 */
public class GormConversionUtils {

    /**
     * Converts an id value to the appropriate type for a domain class.
     *
     * @param grailsDomainClass a GrailsDomainClass
     * @param idValue an value to be converted
     * @return the idValue parameter converted to the type that grailsDomainClass expects
     * its identifiers to be
     */
    static Object convertValueToIdentifierType(GrailsDomainClass grailsDomainClass, Object idValue, ConversionService conversionService) {
        convertValueToType(idValue, grailsDomainClass.identifier.type, conversionService)
    }

    static Object convertValueToType(Object passedValue, Class targetType, ConversionService conversionService) {
        // workaround for GROOVY-6127, do not assign directly in parameters before it's fixed
        Object value = passedValue
        if(targetType != null && value != null && !(value in targetType)) {
            if (value instanceof CharSequence) {
                value = value.toString()
                if(value in targetType) {
                    return value
                }
            }
            try {
                if (value instanceof Number && (targetType==Long || targetType==Integer)) {
                    if(targetType == Long) {
                        value = ((Number)value).toLong()
                    } else {
                        value = ((Number)value).toInteger()
                    }
                } else if (value instanceof String && targetType in Number) {
                    String strValue = value.trim()
                    if(targetType == Long) {
                        value = Long.parseLong(strValue)
                    } else if (targetType == Integer) {
                        value = Integer.parseInt(strValue)
                    } else {
                        value = StringGroovyMethods.asType(strValue, targetType)
                    }
                } else {
                    value = conversionService.convert(value, targetType)
                }
            } catch (e) {
                // ignore
            }
        }
        return value
    }
}
