package utils;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class Loop {
    private final BiFunction<Long, Long, Boolean> checker;

    public Loop(long duration, TimeUnit unit) {
        checker = (startTime, count) -> System.nanoTime() < startTime + unit.toNanos(duration);
    }

    public Loop(long cycles) {
        checker = (startTime, count) -> count < cycles;
    }

    private long ops = 0L;
    private long count = 0L;

    private long timedExec(Runnable block) {
        long beforeBlock = System.nanoTime();
        block.run();
        return System.nanoTime() - beforeBlock;
    }

    public Loop run(Runnable block) {
        long startTs = System.nanoTime();
        long totalExecutionTime_ns = 0L;

        while(checker.apply(startTs, count)) {
            totalExecutionTime_ns += timedExec(block);
            count += 1;
        }

        ops = (count * 1000000000)/totalExecutionTime_ns;
        return this;
    }

    public long ops() { return this.ops; }
    public long count() { return this.count; }
}
