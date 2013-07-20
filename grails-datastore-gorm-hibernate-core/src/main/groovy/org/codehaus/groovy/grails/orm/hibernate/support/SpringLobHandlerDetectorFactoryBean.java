/*
 * Copyright 2004-2005 the original author or authors.
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
package org.codehaus.groovy.grails.orm.hibernate.support;

import javax.sql.DataSource;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.jdbc.support.lob.OracleLobHandler;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;
import org.springframework.util.Assert;

/**
 * Attempts to auto-detect the LobHandler to use from the db meta data.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public class SpringLobHandlerDetectorFactoryBean implements FactoryBean<LobHandler>, InitializingBean {

    private static final String ORACLE_DB_NAME = "Oracle";
    private DataSource dataSource;
    private LobHandler lobHandler;
    private boolean pooledConnection;
    private NativeJdbcExtractor nativeJdbcExtractor = new CommonsDbcpNativeJdbcExtractor();

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setNativeJdbcExtractor(NativeJdbcExtractor nativeJdbcExtractor) {
        this.nativeJdbcExtractor = nativeJdbcExtractor;
    }

    public void setPooledConnection(boolean pooledConnection) {
        this.pooledConnection = pooledConnection;
    }

    public LobHandler getObject() {
        return lobHandler;
    }

    public Class<LobHandler> getObjectType() {
        return LobHandler.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void afterPropertiesSet() throws MetaDataAccessException {
        Assert.notNull(dataSource, "Data source is not set!");

        String dbName = (String)JdbcUtils.extractDatabaseMetaData(dataSource, "getDatabaseProductName");
        if (SpringLobHandlerDetectorFactoryBean.ORACLE_DB_NAME.equals(dbName)) {
            final OracleLobHandler oracleLobHandler = new OracleLobHandler();
            lobHandler = oracleLobHandler;
            if (pooledConnection) {
                oracleLobHandler.setNativeJdbcExtractor(nativeJdbcExtractor);
            }
        }
        else {
            lobHandler = new DefaultLobHandler();
        }
    }
}
