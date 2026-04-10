package com.learnjava.config;

public final class AppConstants {

    private AppConstants() {
        // Utility class
    }

    public static final String API_BASE_PATH = "/v1/libraryevent";
    public static final String DEFAULT_LIBRARY_EVENTS_TOPIC = "library-events";
    public static final String DEFAULT_PRODUCER_ACKS = "all";
}

