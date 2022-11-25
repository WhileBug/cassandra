package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class LogReplicaSet implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LogReplicaSet.class);

    private final Map<File, LogReplica> replicasByFile = Collections.synchronizedMap(new LinkedHashMap<>());

    private Collection<LogReplica> replicas() {
        return replicasByFile.values();
    }

    void addReplicas(List<File> replicas) {
        replicas.forEach(this::addReplica);
    }

    void addReplica(File file) {
        File directory = file.getParentFile();
        assert !replicasByFile.containsKey(directory);
        try {
            replicasByFile.put(directory, LogReplica.open(file));
        } catch (FSError e) {
            logger.error("Failed to open log replica {}", file, e);
            FileUtils.handleFSErrorAndPropagate(e);
        }
        logger.trace("Added log file replica {} ", file);
    }

    void maybeCreateReplica(File directory, String fileName, Set<LogRecord> records) {
        if (replicasByFile.containsKey(directory))
            return;
        try {
            @SuppressWarnings("resource")
            final LogReplica replica = LogReplica.create(directory, fileName);
            records.forEach(replica::append);
            replicasByFile.put(directory, replica);
            logger.trace("Created new file replica {}", replica);
        } catch (FSError e) {
            logger.error("Failed to create log replica {}/{}", directory, fileName, e);
            FileUtils.handleFSErrorAndPropagate(e);
        }
    }

    Throwable syncDirectory(Throwable accumulate) {
        return Throwables.perform(accumulate, replicas().stream().map(s -> s::syncDirectory));
    }

    Throwable delete(Throwable accumulate) {
        return Throwables.perform(accumulate, replicas().stream().map(s -> s::delete));
    }

    private static boolean isPrefixMatch(String first, String second) {
        return first.length() >= second.length() ? first.startsWith(second) : second.startsWith(first);
    }

    boolean readRecords(Set<LogRecord> records) {
        Map<LogReplica, List<String>> linesByReplica = replicas().stream().collect(Collectors.toMap(Function.<LogReplica>identity(), LogReplica::readLines, (k, v) -> {
            throw new IllegalStateException("Duplicated key: " + k);
        }, LinkedHashMap::new));
        int maxNumLines = linesByReplica.values().stream().map(List::size).reduce(0, Integer::max);
        for (int i = 0; i < maxNumLines; i++) {
            String firstLine = null;
            boolean partial = false;
            for (Map.Entry<LogReplica, List<String>> entry : linesByReplica.entrySet()) {
                List<String> currentLines = entry.getValue();
                if (i >= currentLines.size())
                    continue;
                String currentLine = currentLines.get(i);
                if (firstLine == null) {
                    firstLine = currentLine;
                    continue;
                }
                if (!isPrefixMatch(firstLine, currentLine)) {
                    logger.error("Mismatched line in file {}: got '{}' expected '{}', giving up", entry.getKey().getFileName(), currentLine, firstLine);
                    entry.getKey().setError(currentLine, String.format("Does not match <%s> in first replica file", firstLine));
                    return false;
                }
                if (!firstLine.equals(currentLine)) {
                    if (i == currentLines.size() - 1) {
                        logger.warn("Mismatched last line in file {}: '{}' not the same as '{}'", entry.getKey().getFileName(), currentLine, firstLine);
                        if (currentLine.length() > firstLine.length())
                            firstLine = currentLine;
                        partial = true;
                    } else {
                        logger.error("Mismatched line in file {}: got '{}' expected '{}', giving up", entry.getKey().getFileName(), currentLine, firstLine);
                        entry.getKey().setError(currentLine, String.format("Does not match <%s> in first replica file", firstLine));
                        return false;
                    }
                }
            }
            LogRecord record = LogRecord.make(firstLine);
            if (records.contains(record)) {
                logger.error("Found duplicate record {} for {}, giving up", record, record.fileName());
                setError(record, "Duplicated record");
                return false;
            }
            if (partial)
                record.setPartial();
            records.add(record);
            if (record.isFinal() && i != (maxNumLines - 1)) {
                logger.error("Found too many lines for {}, giving up", record.fileName());
                setError(record, "This record should have been the last one in all replicas");
                return false;
            }
        }
        return true;
    }

    void setError(LogRecord record, String error) {
        ???;
        setErrorInReplicas(record);
    }

    void setErrorInReplicas(LogRecord record) {
        replicas().forEach(r -> r.setError(record.raw, record.error()));
    }

    void printContentsWithAnyErrors(StringBuilder str) {
        replicas().forEach(r -> r.printContentsWithAnyErrors(str));
    }

    void append(LogRecord record) {
        Throwable err = Throwables.perform(null, replicas().stream().map(r -> () -> r.append(record)));
        if (err != null) {
            if (!record.isFinal() || err.getSuppressed().length == replicas().size() - 1)
                Throwables.maybeFail(err);
            logger.error("Failed to add record '{}' to some replicas '{}'", record, this);
        }
    }

    boolean exists() {
        Optional<Boolean> ret = replicas().stream().map(LogReplica::exists).reduce(Boolean::logicalAnd);
        return ret.isPresent() ? ret.get() : false;
    }

    public void close() {
        Throwables.maybeFail(Throwables.perform(null, replicas().stream().map(r -> r::close)));
    }

    @Override
    public String toString() {
        Optional<String> ret = replicas().stream().map(LogReplica::toString).reduce(String::concat);
        return ret.isPresent() ? ret.get() : "[-]";
    }

    String getDirectories() {
        return String.join(", ", replicas().stream().map(LogReplica::getDirectory).collect(Collectors.toList()));
    }

    @VisibleForTesting
    List<File> getFiles() {
        return replicas().stream().map(LogReplica::file).collect(Collectors.toList());
    }

    @VisibleForTesting
    List<String> getFilePaths() {
        return replicas().stream().map(LogReplica::file).map(File::getPath).collect(Collectors.toList());
    }
}
