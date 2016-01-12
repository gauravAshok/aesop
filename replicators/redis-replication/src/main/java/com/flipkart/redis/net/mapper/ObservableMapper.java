package com.flipkart.redis.net.mapper;

import rx.functions.Func1;

/**
 *  Interface trivially extending Func1 for a better name.
 * @author gaurav.ashok
 *
 * @param <T>
 * @param <U>
 */
public interface ObservableMapper<T,U> extends Func1<T,U> {
}

