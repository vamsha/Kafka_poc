/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mio;

import java.util.Objects;

/**
 *
 * @author Kogentix
 */
public class KeyValueBean {

    private Long key;
    private Long value;

    public KeyValueBean() {
    }

    public KeyValueBean(final Long key, final Long value) {
        this.key = key;
        this.value = value;
    }

    public Long getKey() {

        return key;
    }

    public void setKey(final Long key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(final Long value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyValueBean that = (KeyValueBean) o;
        return Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValueBean{"
                + "key='" + key + '\''
                + ", value=" + value
                + '}';
    }

}
