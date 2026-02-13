package io.jenkins.plugins.pipeline.cache.agent;

import static java.lang.String.format;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3Object;

import hudson.FilePath;
import hudson.remoting.VirtualChannel;
import io.jenkins.plugins.pipeline.cache.CacheConfiguration;

/**
 * Extracts an existing tar archive from S3 to a given {@link FilePath}.
 *
 * <p>This implementation is resilient against long-lived stream interruptions by downloading the S3 object to
 * a temporary file on the agent first and then extracting locally.</p>
 */
public class RestoreCallable extends AbstractMasterToAgentS3Callable {
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BACKOFF_MS = 10_000L; // 10s
    private static final int DOWNLOAD_BUFFER_SIZE = 1024 * 1024; // 1 MiB

    private final String key;
    private final String[] restoreKeys;

    public RestoreCallable(CacheConfiguration config, String key, String... restoreKeys) {
        super(config);
        this.key = key;
        this.restoreKeys = restoreKeys;
    }

    @Override
    public Result invoke(File path, VirtualChannel channel) throws IOException, InterruptedException {
        // make sure that the restore path not exists yet or is a directory
        if (path.exists() && !path.isDirectory()) {
            return new ResultBuilder()
                    .withInfo("Cache not restored (path is not a directory)")
                    .build();
        }

        String restoreKey = cacheItemRepository().findRestoreKey(this.key, restoreKeys);

        // make sure that the cache exists
        if (restoreKey == null) {
            return new ResultBuilder()
                    .withInfo("Cache not restored (no such key found)")
                    .build();
        }

        long startNanoTime = System.nanoTime();

        long contentLength = -1L;
        try {
            contentLength = cacheItemRepository().getContentLength(restoreKey);
        } catch (RuntimeException ignored) {
            // best-effort; keep going even if metadata lookup fails
        }

        FilePath restorePath = new FilePath(path);
        File tmp = File.createTempFile("jenkins-pipeline-cache-", ".tar");
        boolean success = false;

        try {
            for (int attempt = 1; attempt <= DEFAULT_MAX_ATTEMPTS; attempt++) {
                long attemptStartNanoTime = System.nanoTime();

                // always start from a clean slate for each attempt
                if (tmp.exists() && !tmp.delete()) {
                    // try to truncate by recreating; worst case we overwrite below
                    tmp = File.createTempFile("jenkins-pipeline-cache-", ".tar");
                }

                try {
                    // 1) Download to local temp file (agent filesystem)
                    try (S3Object s3Object = cacheItemRepository().getS3Object(restoreKey);
                         InputStream in = new BufferedInputStream(s3Object.getObjectContent(), DOWNLOAD_BUFFER_SIZE)) {
                        new FilePath(tmp).copyFrom(in);
                    }

                    // 2) Extract locally from file
                    try (InputStream tarIn = new BufferedInputStream(new FileInputStream(tmp), DOWNLOAD_BUFFER_SIZE)) {
                        restorePath.untarFrom(tarIn, FilePath.TarCompression.NONE);
                    }

                    success = true;

                    long attemptMs = (System.nanoTime() - attemptStartNanoTime) / 1_000_000L;
                    return new ResultBuilder()
                            .withInfo(format("Cache restored successfully (%s)", restoreKey))
                            .withInfo(contentLength > 0
                                    ? format("Cache size: %d bytes", contentLength)
                                    : "Cache size: unknown")
                            .withInfo(format("Restore attempt %d/%d succeeded in %d ms", attempt, DEFAULT_MAX_ATTEMPTS, attemptMs))
                            .withInfo(performanceString(restoreKey, startNanoTime))
                            .build();
                } catch (IOException | AmazonClientException e) {
                    boolean lastAttempt = attempt == DEFAULT_MAX_ATTEMPTS;

                    String msg = format("Cache restore attempt %d/%d failed for key %s: %s",
                            attempt, DEFAULT_MAX_ATTEMPTS, restoreKey, rootMessage(e));

                    if (lastAttempt || !isRetryable(e)) {
                        IOException ioe = (e instanceof IOException) ? (IOException) e : new IOException(e);
                        throw (IOException) new IOException(msg).initCause(ioe);
                    }

                    // backoff before retry
                    Thread.sleep(DEFAULT_BACKOFF_MS * attempt);
                }
            }

            // should be unreachable
            throw new IOException(format("Cache restore failed for key %s", restoreKey));
        } finally {
            // update last access timestamp only if restore succeeded
            if (success) {
                try {
                    cacheItemRepository().updateLastAccess(restoreKey);
                } catch (RuntimeException ignored) {
                    // do not fail restore because of metadata update
                }
            }

            if (tmp != null && tmp.exists()) {
                //noinspection ResultOfMethodCallIgnored
                tmp.delete();
            }
        }
    }

    private static boolean isRetryable(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof SocketException) {
                String m = cur.getMessage();
                if (m != null && m.toLowerCase().contains("connection reset")) {
                    return true;
                }
                return true;
            }
            if (cur instanceof InterruptedException) {
                return false;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static String rootMessage(Throwable t) {
        Throwable cur = t;
        Throwable last = t;
        while (cur != null) {
            last = cur;
            cur = cur.getCause();
        }
        String m = last.getMessage();
        return m != null ? m : last.getClass().getName();
    }
}
