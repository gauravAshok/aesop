package com.flipkart.redis.event.listener;

import com.flipkart.redis.event.Event;
import com.flipkart.redis.event.data.CommandArgsPair;

public interface CommandEventListener extends AbstractEventListener<Event<CommandArgsPair>> {

}
