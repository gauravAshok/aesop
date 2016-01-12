package com.flipkart.redis.event.listener;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.KeyValuePair;

public interface KeyValueEventListener extends AbstractEventListener<Event<KeyValuePair>> {

}
