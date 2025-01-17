package com.swang;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class BalancedSlots {

    private int cacheMaxSize;
    private Duration cacheExpireAfterAccess;
    private int slotSize;
    private LoadingCache<String, Integer> cache;
    private Map<Integer, SlotEntry> slotMap;
    private PriorityQueue<SlotEntry> heap;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    public BalancedSlots(int maxSize, Duration expireAfterAccess) {
        init(maxSize, expireAfterAccess);
    }

    public synchronized int getSlot(String key, int numSlots) throws ExecutionException {
        if (numSlots != slotSize) {
            if (numSlots < slotSize) {
                init(this.cacheMaxSize, this.cacheExpireAfterAccess);
            }
            for (int i = slotSize; i < numSlots; i++) {
                SlotEntry slotEntry = new SlotEntry(0, i);
                heap.add(slotEntry);
                slotMap.put(i, slotEntry);
            }
            this.slotSize = numSlots;
        }
        return cache.get(key);
    }

    public void reset() {
        init(this.cacheMaxSize, this.cacheExpireAfterAccess);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BalancedSlots{");
        sb.append("cacheMaxSize=").append(cacheMaxSize);
        sb.append(", cacheExpireAfterAccess=").append(cacheExpireAfterAccess);
        sb.append(", slotSize=").append(slotSize);
        sb.append(", cache=").append(cache.asMap());
        sb.append(", slotMap=").append(slotMap);
        sb.append(", heap=").append(heap);
        sb.append('}');
        return sb.toString();
    }

    protected LoadingCache<String, Integer> getCache(){
        return this.cache;
    }

    protected Map<Integer, SlotEntry> getSlotMap(){
        return this.slotMap;
    }

    private void init(int maxSize, Duration expireAfterAccess) {
        this.slotSize = 0;
        this.cacheMaxSize = maxSize;
        this.cacheExpireAfterAccess = expireAfterAccess;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccess)
                .removalListener(this::cacheOnRemove)
                .recordStats()
                .build(new StringIntegerCacheLoader());
        this.slotMap = new ConcurrentHashMap<>();
        this.heap = new PriorityQueue<>(Comparator.comparingInt(e -> e.keyCount));
        this.scheduledExecutorService.scheduleAtFixedRate(cache::cleanUp, 1, 1, TimeUnit.SECONDS);
    }

    private void cacheOnRemove(RemovalNotification<String, Integer> notification) {
        Integer slotIndex = notification.getValue();
        SlotEntry slotEntry = slotMap.get(slotIndex);
        assert slotEntry != null;
        slotEntry.keyCount -= 1;
        assert slotEntry.keyCount >= 0;
    }

    protected static class SlotEntry {
        int keyCount;
        int slotIndex;

        SlotEntry(int keyCount, int slotIndex) {
            this.keyCount = keyCount;
            this.slotIndex = slotIndex;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("SlotEntry{");
            sb.append("keyCount=").append(keyCount);
            sb.append(", slotIndex=").append(slotIndex);
            sb.append('}');
            return sb.toString();
        }

    }

    protected class StringIntegerCacheLoader extends CacheLoader<String, Integer> {
        public Integer load(String key) throws Exception {
            SlotEntry slotEntry = heap.poll();
            assert slotEntry != null;
            slotEntry.keyCount += 1;
            heap.add(slotEntry);
            return slotEntry.slotIndex;
        }
    }
}
