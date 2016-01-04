package org.stackexchange.dumps.importer.contexts;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import org.stackexchange.dumps.importer.Main;

import javax.sql.DataSource;

public class MysqlImporterContext extends AbstractImporterContext {

    @Bean
    DataSource mysqlDataSource() {
	String[] userCon = Main.con.split("@");
        BasicDataSource result = new BasicDataSource();
        //result.setUsername("root");
        //result.setUrl("jdbc:mysql://localhost:3306/stackexchange");
        result.setUsername(userCon[0]);
        result.setUrl(userCon[1]);
        result.setPassword("");
        result.setMaxActive(100);
        result.setMaxIdle(30);
        result.setMinIdle(0);
        result.setMaxWait(16000);
        result.setDriverClassName("com.mysql.jdbc.Driver");
        return result;
    }

    @Override
    @Bean
    LocalContainerEntityManagerFactoryBean localContainerEntityManagerFactoryBean() {
        LocalContainerEntityManagerFactoryBean result = super.templateEntityManagerFactoryBean();
        result.setDataSource(this.mysqlDataSource());
        return result;
    }

    @Bean
    @Override
    JpaTransactionManager transactionManager() {
        JpaTransactionManager result = new JpaTransactionManager();
        result.setDataSource(this.mysqlDataSource());
        result.setEntityManagerFactory(this.localContainerEntityManagerFactoryBean().getObject());
        return result;
    }

}
