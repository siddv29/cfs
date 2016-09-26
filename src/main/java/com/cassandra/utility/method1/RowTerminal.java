package com.cassandra.utility.method1;

import com.datastax.driver.core.*;
import com.google.common.reflect.TypeToken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by siddharth on 8/9/16.
 */
public class RowTerminal implements Row {
    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return null;
    }

    @Override
    public Token getToken(int i) {
        return null;
    }

    @Override
    public Token getToken(String s) {
        return null;
    }

    @Override
    public Token getPartitionKeyToken() {
        return null;
    }

    @Override
    public boolean isNull(int i) {
        return false;
    }

    @Override
    public boolean getBool(int i) {
        return false;
    }

    @Override
    public byte getByte(int i) {
        return 0;
    }

    @Override
    public short getShort(int i) {
        return 0;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public Date getTimestamp(int i) {
        return null;
    }

    @Override
    public LocalDate getDate(int i) {
        return null;
    }

    @Override
    public long getTime(int i) {
        return 0;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        return null;
    }

    @Override
    public ByteBuffer getBytes(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public BigInteger getVarint(int i) {
        return null;
    }

    @Override
    public BigDecimal getDecimal(int i) {
        return null;
    }

    @Override
    public UUID getUUID(int i) {
        return null;
    }

    @Override
    public InetAddress getInet(int i) {
        return null;
    }

    @Override
    public <T> List<T> getList(int i, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> List<T> getList(int i, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int i, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> aClass, Class<V> aClass1) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
        return null;
    }

    @Override
    public UDTValue getUDTValue(int i) {
        return null;
    }

    @Override
    public TupleValue getTupleValue(int i) {
        return null;
    }

    @Override
    public Object getObject(int i) {
        return null;
    }

    @Override
    public <T> T get(int i, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> T get(int i, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <T> T get(int i, TypeCodec<T> typeCodec) {
        return null;
    }

    @Override
    public boolean isNull(String s) {
        return false;
    }

    @Override
    public boolean getBool(String s) {
        return false;
    }

    @Override
    public byte getByte(String s) {
        return 0;
    }

    @Override
    public short getShort(String s) {
        return 0;
    }

    @Override
    public int getInt(String s) {
        return 0;
    }

    @Override
    public long getLong(String s) {
        return 0;
    }

    @Override
    public Date getTimestamp(String s) {
        return null;
    }

    @Override
    public LocalDate getDate(String s) {
        return null;
    }

    @Override
    public long getTime(String s) {
        return 0;
    }

    @Override
    public float getFloat(String s) {
        return 0;
    }

    @Override
    public double getDouble(String s) {
        return 0;
    }

    @Override
    public ByteBuffer getBytesUnsafe(String s) {
        return null;
    }

    @Override
    public ByteBuffer getBytes(String s) {
        return null;
    }

    @Override
    public String getString(String s) {
        return null;
    }

    @Override
    public BigInteger getVarint(String s) {
        return null;
    }

    @Override
    public BigDecimal getDecimal(String s) {
        return null;
    }

    @Override
    public UUID getUUID(String s) {
        return null;
    }

    @Override
    public InetAddress getInet(String s) {
        return null;
    }

    @Override
    public <T> List<T> getList(String s, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> List<T> getList(String s, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(String s, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(String s, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(String s, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
        return null;
    }

    @Override
    public UDTValue getUDTValue(String s) {
        return null;
    }

    @Override
    public TupleValue getTupleValue(String s) {
        return null;
    }

    @Override
    public Object getObject(String s) {
        return null;
    }

    @Override
    public <T> T get(String s, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> T get(String s, TypeToken<T> typeToken) {
        return null;
    }

    @Override
    public <T> T get(String s, TypeCodec<T> typeCodec) {
        return null;
    }
}
