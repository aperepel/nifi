package org.apache.nifi.jms.testcflib;

import static org.mockito.Mockito.mock;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

public class TestConnectionFactory implements ConnectionFactory {

    private String user;
    private String password;
    private String foo;
    private int bar;
    private String host;

    private int port;

    @Override
    public Connection createConnection() throws JMSException {
        return mock(Connection.class);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        this.user = user;
        this.password = password;
        return mock(Connection.class);
    }

    @Override
    public JMSContext createContext() {
        return mock(JMSContext.class);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        this.user = user;
        this.password = password;
        return mock(JMSContext.class);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        this.user = user;
        this.password = password;
        return mock(JMSContext.class);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return mock(JMSContext.class);
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    public void setBar(int bar) {
        this.bar = bar;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getFoo() {
        return foo;
    }

    public int getBar() {
        return bar;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
