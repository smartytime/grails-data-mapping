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
package org.grails.datastore.gorm.finders;

import grails.gorm.DetachedCriteria;
import groovy.lang.Closure;
import groovy.lang.MissingMethodException;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.FetchType;

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.grails.datastore.gorm.finders.MethodExpression.Between;
import org.grails.datastore.gorm.finders.MethodExpression.Equal;
import org.grails.datastore.gorm.finders.MethodExpression.GreaterThan;
import org.grails.datastore.gorm.finders.MethodExpression.GreaterThanEquals;
import org.grails.datastore.gorm.finders.MethodExpression.Ilike;
import org.grails.datastore.gorm.finders.MethodExpression.InList;
import org.grails.datastore.gorm.finders.MethodExpression.InRange;
import org.grails.datastore.gorm.finders.MethodExpression.IsEmpty;
import org.grails.datastore.gorm.finders.MethodExpression.IsNotEmpty;
import org.grails.datastore.gorm.finders.MethodExpression.IsNotNull;
import org.grails.datastore.gorm.finders.MethodExpression.IsNull;
import org.grails.datastore.gorm.finders.MethodExpression.LessThan;
import org.grails.datastore.gorm.finders.MethodExpression.LessThanEquals;
import org.grails.datastore.gorm.finders.MethodExpression.Like;
import org.grails.datastore.gorm.finders.MethodExpression.NotEqual;
import org.grails.datastore.gorm.finders.MethodExpression.Rlike;
import org.grails.datastore.mapping.core.Datastore;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.model.types.Basic;
import org.grails.datastore.mapping.query.Query;
import org.grails.datastore.mapping.query.api.QueryArgumentsAware;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.ConversionService;
import org.springframework.util.StringUtils;
/**
 * Abstract base class for dynamic finders.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class DynamicFinder extends AbstractFinder implements QueryBuildingFinder {

    public static final String ARGUMENT_MAX = "max";
    public static final String ARGUMENT_OFFSET = "offset";
    public static final String ARGUMENT_ORDER = "order";
    public static final String ARGUMENT_SORT = "sort";
    public static final String ORDER_DESC = "desc";
    public static final String ORDER_ASC = "asc";
    public static final String ARGUMENT_FETCH = "fetch";
    public static final String ARGUMENT_IGNORE_CASE = "ignoreCase";
    public static final String ARGUMENT_CACHE = "cache";
    public static final String ARGUMENT_LOCK = "lock";

    protected Pattern pattern;
    private Pattern[] operatorPatterns;
    private String[] operators;

    private static Pattern methodExpressinPattern;
    private static final Object[] EMPTY_OBJECT_ARRAY = {};

    private static final String NOT = "Not";
    private static final Map<String, Constructor> methodExpressions = new LinkedHashMap<String, Constructor>();

    static {
        // populate the default method expressions
        try {
            Class[] classes = {
                      Equal.class, NotEqual.class, InList.class, InRange.class, Between.class, Like.class, Ilike.class, Rlike.class,
                      GreaterThanEquals.class, LessThanEquals.class, GreaterThan.class,
                      LessThan.class, IsNull.class, IsNotNull.class, IsEmpty.class,
                      IsEmpty.class, IsNotEmpty.class };
            Class[] constructorParamTypes = { Class.class, String.class };
            for (Class c : classes) {
                methodExpressions.put(c.getSimpleName(), c.getConstructor(constructorParamTypes));
            }
        } catch (SecurityException e) {
            // ignore
        } catch (NoSuchMethodException e) {
            // ignore
        }

        resetMethodExpressionPattern();
    }

    static void resetMethodExpressionPattern() {
        String expressionPattern = DefaultGroovyMethods.join(methodExpressions.keySet(), "|");
        methodExpressinPattern = Pattern.compile("\\p{Upper}[\\p{Lower}\\d]+(" + expressionPattern + ")");
    }

    protected DynamicFinder(final Pattern pattern, final String[] operators, final Datastore datastore) {
        super(datastore);
        this.pattern = pattern;
        this.operators = operators;
        this.operatorPatterns = new Pattern[operators.length];
        for (int i = 0; i < operators.length; i++) {
            operatorPatterns[i] = Pattern.compile("(\\w+)(" + operators[i] + ")(\\p{Upper})(\\w+)");
        }
    }

    /**
     * Registers a new method expression. The Class must extends from the class {@link MethodExpression} and provide
     * a constructor that accepts a Class parameter and a String parameter.
     *
     * @param methodExpression A class that extends from {@link MethodExpression}
     */
    public static void registerNewMethodExpression(Class methodExpression) {
        try {
            methodExpressions.put(methodExpression.getSimpleName(), methodExpression.getConstructor(
                    Class.class, String.class));
            resetMethodExpressionPattern();
        } catch (SecurityException e) {
            throw new IllegalArgumentException("Class [" + methodExpression +
                    "] does not provide a constructor that takes parameters of type Class and String: " +
                    e.getMessage(), e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class [" + methodExpression +
                    "] does not provide a constructor that takes parameters of type Class and String: " +
                    e.getMessage(), e);
        }
    }

    public void setPattern(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    public boolean isMethodMatch(String methodName) {
        return pattern.matcher(methodName.subSequence(0, methodName.length())).find();
    }

    public Object invoke(final Class clazz, String methodName, Closure additionalCriteria, Object[] arguments) {
        DynamicFinderInvocation invocation = createFinderInvocation(clazz, methodName, additionalCriteria, arguments);
        return doInvokeInternal(invocation);
    }

    public Object invoke(final Class clazz, String methodName, DetachedCriteria detachedCriteria, Object[] arguments) {
        DynamicFinderInvocation invocation = createFinderInvocation(clazz, methodName, null, arguments);
        if (detachedCriteria != null ) {
            invocation.setDetachedCriteria(detachedCriteria);
        }
        return doInvokeInternal(invocation);
    }

    public DynamicFinderInvocation createFinderInvocation(Class clazz, String methodName,
            Closure additionalCriteria, Object[] arguments) {

        List expressions = new ArrayList();
        if (arguments == null) arguments = EMPTY_OBJECT_ARRAY;
        else {
            Object[] tmp = new Object[arguments.length];
            System.arraycopy(arguments,0,tmp, 0, arguments.length);
            arguments = tmp;
        }
        Matcher match = pattern.matcher(methodName);
        // find match
        match.find();

        String[] queryParameters;
        int totalRequiredArguments = 0;
        // get the sequence clauses
        final String querySequence;
        int groupCount = match.groupCount();
        if (groupCount == 6) {
            String booleanProperty = match.group(3);
            if (booleanProperty == null) {
                booleanProperty = match.group(6);
                querySequence = null;
            }
            else {
                querySequence = match.group(5);
            }
            Boolean arg = Boolean.TRUE;
            if (booleanProperty.matches("Not[A-Z].*")) {
                booleanProperty = booleanProperty.substring(3);
                arg = Boolean.FALSE;
            }
            MethodExpression booleanExpression = findMethodExpression(clazz, booleanProperty);
            booleanExpression.setArguments(new Object[]{arg});
            expressions.add(booleanExpression);
        }
        else {
            querySequence = match.group(2);
        }
        // if it contains operator and split
        boolean containsOperator = false;
        String operatorInUse = null;
        if (querySequence != null) {
            for (int i = 0; i < operators.length; i++) {
                Matcher currentMatcher = operatorPatterns[i].matcher(querySequence);
                if (currentMatcher.find()) {
                    containsOperator = true;
                    operatorInUse = operators[i];

                    queryParameters = querySequence.split(operatorInUse);

                    // loop through query parameters and create expressions
                    // calculating the number of arguments required for the expression
                    int argumentCursor = 0;
                    for (String queryParameter : queryParameters) {
                        MethodExpression currentExpression = findMethodExpression(clazz, queryParameter);
                        final int requiredArgs = currentExpression.getArgumentsRequired();
                        // populate the arguments into the GrailsExpression from the argument list
                        Object[] currentArguments = new Object[requiredArgs];
                        if ((argumentCursor + requiredArgs) > arguments.length) {
                            throw new MissingMethodException(methodName, clazz, arguments);
                        }

                        for (int k = 0; k < requiredArgs; k++, argumentCursor++) {
                            currentArguments[k] = arguments[argumentCursor];
                        }
                        currentExpression = getInitializedExpression(currentExpression, currentArguments);
                        PersistentEntity persistentEntity = datastore.getMappingContext().getPersistentEntity(clazz.getName());

                        try {
                            currentExpression.convertArguments(persistentEntity);
                        } catch (ConversionException e) {
                            throw new MissingMethodException(methodName, clazz, arguments);
                        }

                        // add to list of expressions
                        totalRequiredArguments += currentExpression.argumentsRequired;
                        expressions.add(currentExpression);
                    }
                    break;
                }
            }
        }
        // otherwise there is only one expression
        if (!containsOperator && querySequence != null) {
            MethodExpression solo =findMethodExpression(clazz,querySequence);

            final int requiredArguments = solo.getArgumentsRequired();
            if (requiredArguments  > arguments.length) {
                throw new MissingMethodException(methodName,clazz,arguments);
            }

            totalRequiredArguments += requiredArguments;
            Object[] soloArgs = new Object[requiredArguments];
            System.arraycopy(arguments, 0, soloArgs, 0, requiredArguments);
            solo = getInitializedExpression(solo, arguments);
            PersistentEntity persistentEntity = datastore.getMappingContext().getPersistentEntity(clazz.getName());
            try {
                solo.convertArguments(persistentEntity);
            } catch (ConversionException e) {
                if (!(persistentEntity.getPropertyByName(solo.propertyName) instanceof Basic)) {
                    throw new MissingMethodException(methodName, clazz, arguments);
                }
            }
            expressions.add(solo);
        }

        // if the total of all the arguments necessary does not equal the number of arguments
        // throw exception
        if (totalRequiredArguments > arguments.length) {
            throw new MissingMethodException(methodName,clazz,arguments);
        }

        // calculate the remaining arguments
        Object[] remainingArguments = new Object[arguments.length - totalRequiredArguments];
        if (remainingArguments.length > 0) {
            for (int i = 0, j = totalRequiredArguments; i < remainingArguments.length; i++,j++) {
                remainingArguments[i] = arguments[j];
            }
        }

        return new DynamicFinderInvocation(clazz, methodName, remainingArguments,
                expressions, additionalCriteria, operatorInUse);
    }

    /**
     * Initializes the arguments of the specified expression with the specified arguments.  If the
     * expression is an Equal expression and the argument is null then a new expression is created
     * and returned of type IsNull.
     *
     * @param expression expression to initialize
     * @param arguments arguments to the expression
     * @return the initialized expression
     */
    private MethodExpression getInitializedExpression(MethodExpression expression, Object[] arguments) {
        if (expression instanceof Equal && arguments.length == 1 && arguments[0] == null) {
            expression = new IsNull(expression.targetClass, expression.propertyName);
        } else {
            expression.setArguments(arguments);
        }
        return expression;
    }

    protected MethodExpression findMethodExpression(Class clazz, String expression) {
        MethodExpression me = null;
        final Matcher matcher = methodExpressinPattern.matcher(expression);

        if (matcher.find()) {
            Constructor constructor = methodExpressions.get(matcher.group(1));
            try {
                me = (MethodExpression) constructor.newInstance(clazz,
                        calcPropertyName(expression, constructor.getDeclaringClass().getSimpleName()));
            } catch (Exception e) {
                // ignore
            }
        }
        if (me == null) {
            me = new Equal(clazz, calcPropertyName(expression, Equal.class.getSimpleName()));
        }

        return me;
    }

    private static String calcPropertyName(String queryParameter, String clause) {
        String propName;
        if (clause != null && !clause.equals(Equal.class.getSimpleName())) {
            int i = queryParameter.indexOf(clause);
            propName = queryParameter.substring(0,i);
        }
        else {
            propName = queryParameter;
        }

        if (propName.endsWith(NOT)) {
            int i = propName.lastIndexOf(NOT);
            propName = propName.substring(0, i);
        }

        if (!StringUtils.hasLength(propName)) {
            throw new IllegalArgumentException("No property name specified in clause: " + clause);
        }

        return propName.substring(0,1).toLowerCase(Locale.ENGLISH) + propName.substring(1);
    }

    protected abstract Object doInvokeInternal(DynamicFinderInvocation invocation);

    public Object invoke(final Class clazz, String methodName, Object[] arguments) {
        return invoke(clazz, methodName, (Closure)null, arguments);
    }

    public static void populateArgumentsForCriteria(Class<?> targetClass, Query q, Map argMap) {
        if (argMap == null) {
            return;
        }

        Integer maxParam = null;
        Integer offsetParam = null;
        final ConversionService conversionService = q.getSession().getMappingContext().getConversionService();
        if (argMap.containsKey(ARGUMENT_MAX)) {
            maxParam = conversionService.convert(argMap.get(ARGUMENT_MAX), Integer.class);
        }
        if (argMap.containsKey(ARGUMENT_OFFSET)) {
            offsetParam = conversionService.convert(argMap.get(ARGUMENT_OFFSET), Integer.class);
        }
        String orderParam = (String)argMap.get(ARGUMENT_ORDER);

        Object fetchObj = argMap.get(ARGUMENT_FETCH);
        if (fetchObj instanceof Map) {
            Map fetch = (Map)fetchObj;
            for (Object o : fetch.keySet()) {
                String associationName = (String) o;
                FetchType fetchType = getFetchMode(fetch.get(associationName));
                switch(fetchType) {
                    case LAZY:
                       q.select(associationName);
                    break;
                    case EAGER:
                        q.join(associationName);
                }
            }
        }

        final String sort = (String)argMap.get(ARGUMENT_SORT);
        final String order = ORDER_DESC.equalsIgnoreCase(orderParam) ? ORDER_DESC : ORDER_ASC;
        final int max = maxParam == null ? -1 : maxParam;
        final int offset = offsetParam == null ? -1 : offsetParam;
        if (max > -1) {
            q.max(max);
        }
        if (offset > -1) {
            q.offset(offset);
        }
        if (sort != null) {
            if (ORDER_DESC.equals(order)) {
                q.order(Query.Order.desc(sort));
            }
            else {
                q.order(Query.Order.asc(sort));
            }
        }
        if (q instanceof QueryArgumentsAware) {
            ((QueryArgumentsAware)q).setArguments(argMap);
        }
    }

    /**
     * Retrieves the fetch mode for the specified instance; otherwise returns the default FetchMode.
     *
     * @param object The object, converted to a string
     * @return The FetchMode
     */
    public static FetchType getFetchMode(Object object) {
        String name = object != null ? object.toString() : "default";
        if (name.equalsIgnoreCase(FetchType.EAGER.toString()) || name.equalsIgnoreCase("join")) {
            return FetchType.EAGER;
        }
        if (name.equalsIgnoreCase(FetchType.LAZY.toString()) || name.equalsIgnoreCase("select")) {
            return FetchType.LAZY;
        }
        return FetchType.LAZY;
    }

    protected void configureQueryWithArguments(Class clazz, Query query, Object[] arguments) {
        if (arguments.length == 0 || !(arguments[0] instanceof Map)) {
            return;
        }

        Map<?, ?> argMap = (Map<?, ?>)arguments[0];
        populateArgumentsForCriteria(clazz, query, argMap);
    }

    public static void applyDetachedCriteria(Query q, DetachedCriteria detachedCriteria) {
        if (detachedCriteria != null) {
            List<Query.Criterion> criteria = detachedCriteria.getCriteria();
            for (Query.Criterion criterion : criteria) {
                q.add(criterion);
            }
            List<Query.Projection> projections = detachedCriteria.getProjections();
            for (Query.Projection projection : projections) {
                q.projections().add(projection);
            }
            List<Query.Order> orders = detachedCriteria.getOrders();
            for (Query.Order order : orders) {
                q.order(order);
            }

            Map<String, FetchType> fetchStrategies = detachedCriteria.getFetchStrategies();
            for (Map.Entry<String, FetchType> entry : fetchStrategies.entrySet()) {
                switch(entry.getValue()) {
                    case EAGER:
                        q.join(entry.getKey()); break;
                    case LAZY:
                        q.select(entry.getKey());
                }
            }
        }
    }
}
