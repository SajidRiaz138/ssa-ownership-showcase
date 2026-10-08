package io.ssaownership.schemasync;

/** Small shared validation helpers for operator inputs (avoids repeating null/blank checks). */
final class Names {
    private Names() {}

    /** Reject null or blank, returning the value otherwise. */
    static String require(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " must not be null or blank");
        }
        return value;
    }
}
