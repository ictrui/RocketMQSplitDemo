package org.example.consumer;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class CustomMessageIdGenerator {
    private static final AtomicLong ID_COUNTER = new AtomicLong(0);

    public static String generateMessageId() {
        long randomLong = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        return "MSG-" + randomLong;
    }
}
