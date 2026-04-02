package com.jmqtt.protocol;

public interface ClientAuthenticator {
    boolean authenticate(String username, String password);

    ClientAuthenticator ALLOW_ALL = (username, password) -> true;
}
