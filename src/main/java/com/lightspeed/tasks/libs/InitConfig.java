package com.lightspeed.tasks.libs;

/**
 *
 * @param steamShareBlockSize
 * @param fileReadChunkSize
 */
public record InitConfig(int steamShareBlockSize, long fileReadChunkSize) {
    public static final InitConfig DEFAULT = new InitConfig(10_000, 5*1024*1024);
}
