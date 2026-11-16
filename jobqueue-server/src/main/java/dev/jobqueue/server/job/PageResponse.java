package dev.jobqueue.server.job;

import java.util.List;

/**
 * One page of a list. {@code page} starts at 0; {@code total} is the number of items over all pages.
 */
public record PageResponse<T>(List<T> items, int page, int size, long total) {
}
