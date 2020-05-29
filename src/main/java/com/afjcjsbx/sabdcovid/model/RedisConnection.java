package com.afjcjsbx.sabdcovid.model;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

// Rendo serializzabile la connessione a Redis altrimenti il Task non può essere serializzato
public class RedisConnection implements Serializable {
    private final Jedis conn;

    public RedisConnection(String hostname) {
        conn = new Jedis(hostname);
    }

    public Jedis conn() {
        return conn;
    }

}